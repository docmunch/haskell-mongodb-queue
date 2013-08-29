{-# LANGUAGE FlexibleContexts, RecordWildCards, Rank2Types, DeriveDataTypeable, ExtendedDefaultRules #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
module Database.MongoDB.Queue (
    emit
  , nextFromQueue
  , createEmitter, mkEmitter, EmitterOpts (..)
  , createWorker, mkWorker, WorkerOpts (..)

) where

import Prelude hiding (lookup)
import Control.Concurrent (threadDelay)
import Control.Exception.Lifted (catch, throwIO, Exception, SomeException)
import Data.Default (Default (..))
import Data.Typeable (Typeable)
import Database.MongoDB
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Control (MonadBaseControl(..))
import Data.Text (Text)
import Network.BSD (getHostName, HostName)
import Control.Monad (void)

default (Int)

queueCollection, handled, dataField, _id, hostField, versionField :: Text
queueCollection = "queue"
handled = "handled"
dataField = "data"
hostField = "host"
versionField = "version"
_id = "_id"


type DBRunner = (MonadIO m, MonadBaseControl IO m) => Action m a -> m a
data QueueEmitter = QueueEmitter {
                      qeVersion :: Int -- ^ version
                    , qeHost :: HostName
                    , qeRunDB :: DBRunner
                    , qeCollection :: Collection
                    }

data EmitterOpts = EmitterOpts
                   { emitterVersion :: Int
                   , emitterCollection :: Collection
                   , emitterMaxByteSize :: Int
                   }

instance Default EmitterOpts where
    def = EmitterOpts 1 queueCollection 100000


-- | create a QueueEmitter
createEmitter :: DBRunner -> IO QueueEmitter
createEmitter = mkEmitter def

-- | create an emitter with non-default configuration
mkEmitter :: EmitterOpts -> DBRunner -> IO QueueEmitter
mkEmitter EmitterOpts {..} emitterRunner = do
  name <- getHostName
  void $ emitterRunner $ createCollection [Capped, MaxByteSize emitterMaxByteSize] emitterCollection
  return $ QueueEmitter emitterVersion name emitterRunner emitterCollection

-- | emit a message for a worker
emit :: QueueEmitter -> Document -> IO ()
emit QueueEmitter {..} doc =
  qeRunDB $ insert_ qeCollection [
            versionField =: qeVersion
          , dataField =: doc
          , handled =: False
          , hostField =: qeHost
          ]
          -- TODO: add timestamp
          -- but actually the _id will already have a timestamp
          -- localTime: dt,
          -- globalTime: new Date(dt-self.serverTimeOffset),
          -- pickedTime: new Date(dt-self.serverTimeOffset),

data QueueWorker = QueueWorker {
                     qwRunDB :: DBRunner
                   , qwGetCursor :: IO Cursor
                   , qwCollection :: Collection
                   }
data WorkerOpts = WorkerOpts
                  { workerMaxByteSize :: Int
                  , workerCollection :: Collection
                  }
instance Default WorkerOpts where
    def = WorkerOpts 100000 queueCollection

-- | creates a QueueWorker
-- create a single QueueWorker per process (per queue collection)
-- call nextFromQueue with the QueueWorker to get the next message
--
-- QueueWorker is probably poorly named now with the direction the library has taken.
-- To handle multiple messages at once use the setup mentioned above with just 1 QueueWorker.
-- But immediately hand off messages from nextFromQueue to worker threads (this library does not help you create worker threads)
createWorker :: DBRunner -> IO QueueWorker
createWorker = mkWorker def

-- | create an worker with non-default configuration
mkWorker :: WorkerOpts -> DBRunner -> IO QueueWorker
mkWorker WorkerOpts {..} workerRunner = do
    _<- workerRunner $
      createCollection [Capped, MaxByteSize workerMaxByteSize] workerCollection
    return $ QueueWorker
               workerRunner
               (getCursor workerRunner workerCollection)
               workerCollection

getCursor :: DBRunner -> Collection -> IO Cursor
getCursor runDB collection =
    runDB $ do
      _<- insert collection [ "tailableCursorFix" =: ("helps when there are no docs" :: Text) ]
      find (select [ handled =: False ] collection) {
          options = [TailableCursor, AwaitData, NoCursorTimeout]
        }


nextDoc :: (MonadIO m, MonadBaseControl IO m, Functor m) => Cursor -> Action m Document
nextDoc cursor = do
  n <- next cursor
  case n of
    Nothing -> nextDoc cursor
    (Just doc) -> return doc

data MongoQueueException = FindAndModifyError String
                         deriving (Show, Typeable)
instance Exception MongoQueueException

-- | Get the next message from the queue.
-- First marks the message as handled.
--
-- Do not call this from multiple threads against the same QueueWorker
nextFromQueue :: QueueWorker -> IO Document
nextFromQueue QueueWorker {..} =
    qwGetCursor >>= qwRunDB . processNext
  where
    processNext cursor = do
        origDoc <- nextDoc cursor `catch` handleDroppedCursor
        let idQuery = [_id := valueAt _id origDoc]

        eDoc <- findAndModify (selectQuery $ idQuery ++ [handled =: False])
                             ["$set" =: [handled =: True]]
        case eDoc of
          Right doc -> return (at dataField doc)
          Left err  ->  do
              -- a different cursor can lock this first by setting handled to True
              -- verify that this is what happened
              mDoc <- findOne (selectQuery idQuery)
              case mDoc of
                Nothing  -> liftIO $ throwIO $ FindAndModifyError err
                Just _ -> processNext cursor

    selectQuery query = (select query qwCollection) {
        sort = ["$natural" =: -1]
      }

    handleDroppedCursor :: (MonadIO m, MonadBaseControl IO m, Functor m) => SomeException -> Action m Document
    handleDroppedCursor _ = nextDoc =<< liftIO (
        threadDelay (1000 * 1000) >> qwGetCursor
      )

{-
-- | Perform the action every time there is a new message.
-- And then marks the message as handled.
-- Does not call ForkIO, blocks the program
--
-- Do not call this multiple times against the same QueueWorker
work :: QueueWorker -> (Document -> Action IO ()) -> IO ()
work qw handler = loop
  where
    loop = do
      doc <- nextFromQueue qw
      handler doc
      loop
      -}
