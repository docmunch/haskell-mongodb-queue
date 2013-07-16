{-# LANGUAGE FlexibleContexts, RecordWildCards, Rank2Types, DeriveDataTypeable #-}
module Database.MongoDB.Queue (
    emit
  , nextFromQueue
  , createEmitter, mkEmitter, EmitterOpts (..)
  , createWorker, mkWorker, WorkerOpts (..)

) where

import Prelude hiding (lookup)
import Control.Exception.Base (throwIO, Exception)
import Data.Default (Default (..))
import Data.Typeable (Typeable)
import Database.MongoDB
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Control (MonadBaseControl(..))
import Data.Text (Text)
import Network.BSD (getHostName, HostName)
import Control.Monad (void)

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
createEmitter runEmitter = mkEmitter def runEmitter

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
                   , qwCursor :: Cursor
                   , qwCollection :: Collection
                   }
data WorkerOpts = WorkerOpts
                  { workerMaxByteSize :: Int
                  , workerCollection :: Collection
                  }
instance Default WorkerOpts where
    def = WorkerOpts 100000 queueCollection

-- | creates a QueueWorker
-- Do not 'work' multiple times against the same QueueWorker
createWorker :: DBRunner -> IO QueueWorker
createWorker runWorker = mkWorker def runWorker

-- | create an worker with non-default configuration
mkWorker :: WorkerOpts -> DBRunner -> IO QueueWorker
mkWorker WorkerOpts {..} workerRunner = do
    _<- workerRunner $
      createCollection [Capped, MaxByteSize workerMaxByteSize] workerCollection
    cursor <- getCursor workerRunner workerCollection
    return $ QueueWorker workerRunner cursor workerCollection

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
nextFromQueue QueueWorker {..} = qwRunDB $ do
    origDoc <- nextDoc qwCursor {-`catch` (\e ->
      case e of
         dead cursor
          cursor <- getCursor
        _ -> liftIO $ thowIO e
      )
    -}
    eDoc <- findAndModify (select [_id := (valueAt _id origDoc)] qwCollection) {
        sort = ["$natural" =: (-1 :: Int)]
      } [ "$set" =: [handled =: True] ]
    case eDoc of
      Left err  -> liftIO $ throwIO $ FindAndModifyError err
      Right doc -> return (at dataField doc)

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
