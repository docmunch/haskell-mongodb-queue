{-# LANGUAGE FlexibleContexts, RecordWildCards, Rank2Types, DeriveDataTypeable #-}
module Database.MongoDB.Queue (
    emit
  , nextFromQueue
  , createEmitter
  , createWorker
) where

import Prelude hiding (lookup)
import Control.Exception.Base (throwIO, Exception)
import Data.Typeable (Typeable)
import Database.MongoDB
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Applicative (Applicative)
import Control.Monad.Trans.Control (MonadBaseControl(..))
import Data.Text (Text)
import Network.BSD (getHostName, HostName)

queueCollection, handled, dataField, _id, hostField, versionField :: Text
queueCollection = "queue"
handled = "handled"
dataField = "data"
hostField = "host"
versionField = "version"
_id = "_id"

-- from Database.Persist.MongoDB, trying to move into driver
findAndModify :: (Applicative m, MonadIO m)
              => Query
              -> Document -- ^ updates
              -> Action m (Either String Document)
findAndModify (Query {
    selection = Select sel collection
  , project = project
  , sort = sort
  }) updates = do
  result <- runCommand [
     "findAndModify" := String collection
   , "new"    := Bool True -- return updated document, not original document
   , "query"  := Doc sel
   , "update" := Doc updates
   , "fields" := Doc project
   , "sort"   := Doc sort
   ]
  return $ case findErr result of
    Nothing -> case lookup "value" result of
      Nothing -> Left "findAndModify: no document found (value field was empty)"
      Just doc -> Right doc
    Just e -> Left e
    where
      findErr result = lookup "err" (at "lastErrorObject" result)


type DBRunner = (MonadIO m, MonadBaseControl IO m) => Action m a -> m a
data QueueEmitter = QueueEmitter {
                      qeVersion :: Int -- ^ version
                    , qeHost :: HostName
                    , qeRunDB :: DBRunner
                    , qeCollection :: Collection
                    }

data EmitterOpts = EmitterOpts {
                     emitterVersion :: Int
                   , emitterRunner :: DBRunner
                   , emitterCollection :: Collection
                   }

-- | create a QueueEmitter
createEmitter :: DBRunner -> IO QueueEmitter
createEmitter runEmitter = mkEmitter $ EmitterOpts {
                             emitterVersion = 1
                           , emitterRunner = runEmitter
                           , emitterCollection = queueCollection
                           }

mkEmitter :: EmitterOpts -> IO QueueEmitter
mkEmitter EmitterOpts {..} = do
  name <- getHostName
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
data WorkerOpts = WorkerOpts { 
                    workerRunner :: DBRunner
                  , workerMaxByteSize :: Int
                  , workerCollection :: Collection
                  }

-- | creates a QueueWorker
-- Do not 'work' multiple times against the same QueueWorker
createWorker :: DBRunner -> IO QueueWorker
createWorker runWorker = mkWorker $ WorkerOpts { 
                         workerRunner = runWorker
                       , workerMaxByteSize = 100000
                       , workerCollection = queueCollection
                       }

mkWorker :: WorkerOpts -> IO QueueWorker
mkWorker WorkerOpts {..} = do
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
    Nothing -> liftIO $ throwIO $ TailableCursorError "tailable cursor ended"
    (Just doc) -> return doc

data MongoQueueException = FindAndModifyError String
                         | TailableCursorError String
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
        TailableCursorError "tailable cursor ended" -> do
          cursor <- getCursor
        _ -> liftIO $ thowIO e
      )
    -}
    liftIO $ print origDoc
    eDoc <- findAndModify (select [_id := (valueAt _id origDoc)] queueCollection) {
        sort = ["$natural" =: (-1 :: Int)]
      } [ "$set" =: [handled =: True] ]
    case eDoc of
      Left err  -> liftIO $ throwIO $ FindAndModifyError err
      Right doc -> do
        liftIO $ print doc
        return (at dataField doc)

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
