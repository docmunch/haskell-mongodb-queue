{-# LANGUAGE OverloadedStrings, FlexibleContexts, RecordWildCards #-}
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

queueCollection, handled, dataField, _id :: Text
queueCollection = "queue"
handled = "handled"
dataField = "data"
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

run :: Show a => Action IO a -> IO ()
run act = do
  pipe <- runIOE $ connect (host "127.0.0.1")
  m1 <- access pipe master "mongomq" act
  print m1


data QueueEmitter = QueueEmitter {
                      qeVersion :: Int -- ^ version
                    , qeHost :: HostName -- ^ HostName
                    }

data EmitterOpts = EmitterOpts { version :: Int }

createEmitter :: IO QueueEmitter
createEmitter = mkEmitter $ EmitterOpts { version = 1 }

mkEmitter :: EmitterOpts -> IO QueueEmitter
mkEmitter opts = do
  name <- getHostName
  return $ QueueEmitter (version opts) name 

emit :: QueueEmitter -> Document -> IO ()
emit QueueEmitter {..} doc =
  run $ insert_ queueCollection [
            "version" =: qeVersion
          , dataField =: doc
          , handled =: False
          , "host" =: qeHost
          ]
          -- localTime: dt, 
          -- globalTime: new Date(dt-self.serverTimeOffset),
          -- pickedTime: new Date(dt-self.serverTimeOffset),

data QueueWorker = QueueWorker
data WorkerOpts = WorkerOpts

createWorker :: IO QueueWorker
createWorker = mkWorker $ WorkerOpts

mkWorker :: WorkerOpts -> IO QueueWorker
mkWorker _ = do
  return $ QueueWorker

work :: QueueWorker -> (Document -> Action IO ()) -> IO ()
work QueueWorker {..} handler = run $ do
  cursor <- find (select [ handled =: False ] queueCollection) { options = [TailableCursor, AwaitData] }
  tailableCursorApply cursor $ \doc -> do
    handler (at "data" doc)
    findAndModifyOne queueCollection (at _id doc) [ handled =: True ]
  where
    tailableCursorApply :: (MonadIO m, MonadBaseControl IO m, Functor m) => Cursor -> (Document ->  Action m a) ->  Action m a
    tailableCursorApply cursor f = do
      n <- next cursor
      case n of
        Nothing -> error "tailable cursor ended"
        (Just x) -> f x >> tailableCursorApply cursor f
