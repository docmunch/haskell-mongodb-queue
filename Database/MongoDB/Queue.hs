{-# LANGUAGE OverloadedStrings, FlexibleContexts, RecordWildCards, Rank2Types #-}
module Database.MongoDB.Queue (
    work
  , emit
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
                    , qeHost :: HostName -- ^ HostName
                    , qeRunDB :: DBRunner
                    }

data EmitterOpts = EmitterOpts { version :: Int, emitRunner :: DBRunner }

createEmitter :: DBRunner -> IO QueueEmitter
createEmitter runEmitter = mkEmitter $ EmitterOpts {
                             version = 1,
                             emitRunner = runEmitter
                           }

mkEmitter :: EmitterOpts -> IO QueueEmitter
mkEmitter EmitterOpts {..} = do
  name <- getHostName
  return $ QueueEmitter version name emitRunner

emit :: QueueEmitter -> Document -> IO ()
emit QueueEmitter {..} doc =
  qeRunDB $ insert_ queueCollection [
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

data QueueWorker = QueueWorker { qwRunDB :: DBRunner }
data WorkerOpts = WorkerOpts { workerRunner :: DBRunner }

createWorker :: DBRunner -> IO QueueWorker
createWorker runWorker = mkWorker $ WorkerOpts { 
                         workerRunner = runWorker
                       }

mkWorker :: WorkerOpts -> IO QueueWorker
mkWorker WorkerOpts {..} = return $ QueueWorker workerRunner

work :: QueueWorker -> (Document -> Action IO ()) -> IO ()
work QueueWorker {..} handler = qwRunDB $ do
    cursor <- find (select [ handled =: False ] queueCollection) { options = [TailableCursor, AwaitData] }
    void $ tailableCursorApply cursor $ \doc -> do
      handler (at "data" doc)
      findAndModifyOne queueCollection (at _id doc) [ handled =: True ]
  where
    tailableCursorApply :: (MonadIO m, MonadBaseControl IO m, Functor m) => Cursor -> (Document ->  Action m a) ->  Action m a
    tailableCursorApply cursor f = do
      n <- next cursor
      case n of
        Nothing -> error "tailable cursor ended"
        (Just x) -> f x >> tailableCursorApply cursor f


{- simple runner example
runDB :: Show a => Action IO a -> IO ()
runDB act = do
  pipe <- runIOE $ connect (host "127.0.0.1")
  m1 <- access pipe master "mongomq" act
  print m1
 - -}
