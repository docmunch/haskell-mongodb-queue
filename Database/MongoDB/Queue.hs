{-# LANGUAGE FlexibleContexts, RecordWildCards, Rank2Types #-}
module Database.MongoDB.Queue (
    work
  , emit
  , peek
  , createEmitter
  , createWorker
) where

import Prelude hiding (lookup)
import Database.MongoDB
import Control.Monad.IO.Class (MonadIO)
import Control.Applicative (Applicative)
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

-- from Database.Persist.MongoDB, trying to move into driver
findAndModifyOne :: (Applicative m, MonadIO m)
                 => Collection
                 -> ObjectId -- ^ _id for query
                 -> [Field] -- ^ updates
                 -> Action m (Either String Document)
findAndModifyOne collection objectId updates = do
  result <- runCommand [
     "findAndModify" := String collection,
     "new" := Bool True, -- return updated document, not original document
     "query" := Doc [_id := ObjId objectId],
     "update" := Doc updates
   ]
  return $ case findErr result of
    Nothing -> case lookup "value" result of
      Nothing -> Left "no value field"
      Just doc -> Right doc
    Just e -> Left e
    where
      findErr result = lookup "err" (at "lastErrorObject" result)


type DBRunner = MonadIO m => Action m a -> m a
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
    runDB $ find (select [ handled =: False ] collection) {
        options = [TailableCursor, AwaitData, NoCursorTimeout]
      }


-- | used for testing.
-- Get the next document, will not mark it as handled
-- Do not peek/work multiple times against the same QueueWorker
peek :: QueueWorker -> IO Document
peek QueueWorker {..} = qwRunDB $ 
  fmap (at dataField) (nextDoc qwCursor)

nextDoc :: (MonadIO m, MonadBaseControl IO m, Functor m) => Cursor -> Action m Document
nextDoc cursor = do
  n <- next cursor
  return $ case n of
    Nothing -> error "tailable cursor ended"
    (Just doc) -> doc

-- | Perform the action every time there is a new message.
-- And then marks the message as handled.
-- Does not call ForkIO, blocks the program
--
-- Do not call this multiple times against the same QueueWorker
work :: QueueWorker -> (Document -> Action IO ()) -> IO ()
work QueueWorker {..} handler = qwRunDB $ do
    void $ tailableCursorApply qwCursor $ \doc -> do
      handler (at dataField doc)
      findAndModifyOne queueCollection (at _id doc) [ handled =: True ]
  where
    tailableCursorApply :: (MonadIO m, MonadBaseControl IO m, Functor m) => Cursor -> (Document ->  Action m a) ->  Action m a
    tailableCursorApply cursor f = do
      x <- nextDoc cursor
      f x >> tailableCursorApply cursor f
