{-# LANGUAGE FlexibleContexts, RecordWildCards, Rank2Types, DeriveDataTypeable, ExtendedDefaultRules #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
module Database.MongoDB.Queue (
    emit
  , nextFromQueuePoll, nextFromQueueTail
  -- * queue emitters
  , createEmitter, mkEmitter, EmitterOpts (..)
  -- * queue consumers
  , createPollBroker, createTailBroker, mkTailBroker, mkPollBroker, WorkerOpts (..)

) where

import Prelude hiding (lookup)
import Control.Concurrent (threadDelay)
import Data.IORef (atomicWriteIORef, IORef, newIORef, readIORef)
import Control.Exception.Lifted (catch, throwIO, Exception, SomeException)
import Data.Default (Default (..))
import Data.Typeable (Typeable)
import Database.MongoDB
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Control (MonadBaseControl(..))
import Data.Text (Text)
import Network.BSD (getHostName, HostName)
import Control.Monad (void)
import Control.Applicative

default (Int)

queueCollection, handled, dataField, _id, hostField, versionField :: Text
queueCollection = "queue"
handled = "handled"
dataField = "data"
hostField = "host"
versionField = "version"
_id = "_id"


data QueueEmitter = QueueEmitter {
                      qeVersion :: Int -- ^ version
                    , qeHost :: HostName
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
createEmitter :: (Applicative m, MonadIO m) => Action m QueueEmitter
createEmitter = mkEmitter def

-- | create an emitter with non-default configuration
mkEmitter :: (Applicative m, MonadIO m) => EmitterOpts -> Action m QueueEmitter
mkEmitter EmitterOpts {..} = do
  name <- liftIO getHostName
  void $ createCollection [Capped, MaxByteSize emitterMaxByteSize] emitterCollection
  return $ QueueEmitter emitterVersion name emitterCollection

-- | emit a message for a worker
emit :: (MonadIO m, Applicative m) => QueueEmitter -> Document -> Action m ()
emit QueueEmitter {..} doc =
  insert_ qeCollection [
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

-- | A worker that uses a tailable cursor
data TailBroker = TailBroker { tbCollection :: Collection  }

-- | A worker that uses polling
data PollBroker = PollBroker
                    { pbCollection :: Collection 
                    , pbPollInterval :: Int
                    , pbLastId :: IORef ObjectId
                    }


data WorkerOpts = WorkerOpts
                  { workerMaxByteSize :: Int
                  , workerCollection :: Collection
                  }

instance Default WorkerOpts where
    def = WorkerOpts 100000 queueCollection

-- | creates a TailBroker
-- create a single TailBroker per process (per queue collection)
-- call nextFromQueueTail with the TailBroker to get the next message
--
-- TailBroker is designed to have 1 instance per process (and 1 process per machine)
-- To handle multiple messages at once immediately hand off messages from nextFromQueueTail to worker threads (this library does not help you create worker threads)
createTailBroker :: (MonadIO m, Applicative m) => Action m TailBroker
createTailBroker = mkTailBroker def

-- | same as createTailBroker, but uses a polling technique instead of tailable cursors
createPollBroker :: (MonadIO m, Applicative m) => Action m PollBroker
createPollBroker = mkPollBroker def 100

-- | create a tailable cursor worker with non-default configuration
mkTailBroker :: (MonadIO m, Applicative m) => WorkerOpts -> Action m TailBroker
mkTailBroker WorkerOpts {..} = do
    _<- createCollection [Capped, MaxByteSize workerMaxByteSize] workerCollection
    _ <- insert workerCollection [ "tailableCursorFix" =: ("helps when there are no docs" :: Text) ]
    return TailBroker { tbCollection = workerCollection }

-- | create an worker with non-default configuration
mkPollBroker :: (MonadIO m, Applicative m)
             => WorkerOpts
             -> Int -- ^ polling interval in us (uses threadDelay)
             -> Action m PollBroker
mkPollBroker WorkerOpts {..} interval = do
    _<- createCollection [Capped, MaxByteSize workerMaxByteSize] workerCollection
    (ObjId insertId) <- insert workerCollection [
        "tailableCursorFix" =: ("helps when there are no docs" :: Text)
      ]
    lastId <- liftIO $ newIORef insertId
    return PollBroker
               { pbCollection = workerCollection
               , pbPollInterval = interval
               , pbLastId = lastId
               }



data MongoQueueException = FindAndModifyError String
                         deriving (Show, Typeable)
instance Exception MongoQueueException

-- | Get the next message from the queue.
-- First marks the message as handled.
--
-- Uses polling rather than a tailable cursor
--
-- Do not call this from multiple threads against the same PollBroker
nextFromQueuePoll :: (MonadIO m, MonadBaseControl IO m) => PollBroker -> Action m Document
nextFromQueuePoll pb = nextFromQueue (pbCollection pb) getCursor (nextDocPoll getCursor) $
    \doc -> atomicWriteIORef (pbLastId pb) (at _id doc)
  where
    getCursor = getCursorPoll pb

    getCursorPoll :: (MonadIO m, MonadBaseControl IO m) => PollBroker -> Action m Cursor
    getCursorPoll PollBroker{..} = do
        lastId <- liftIO $ readIORef pbLastId
        find (select [ handled =: False, _id =: ["$gt" =: lastId ] ] pbCollection)



-- | Get the next message from the queue.
-- First marks the message as handled.
--
-- Uses a tailable cursor rather than polling
--
-- Do not call this from multiple threads against the same TailBroker
nextFromQueueTail :: (MonadIO m, MonadBaseControl IO m) => TailBroker -> Action m Document
nextFromQueueTail tb = nextFromQueue (tbCollection tb) (getCursorTail tb) nextDocTail (const (return ()))
  where
    getCursorTail :: (MonadIO m, MonadBaseControl IO m) => TailBroker -> Action m Cursor
    getCursorTail TailBroker{..} =
        find (select [ handled =: False ] tbCollection) {
            options = [TailableCursor, AwaitData, NoCursorTimeout]
          }

nextFromQueue :: (MonadIO m, MonadBaseControl IO m)
              => Collection
              -> Action m Cursor
              -> (Cursor -> Action m Document)
              -> (Document -> IO ())
              -> Action m Document
nextFromQueue collection getCursor nextDoc successCb =
    getCursor >>= processNext
  where
    processNext cursor = do
        origDoc <- nextDoc cursor `catch` handleDroppedCursor getCursor nextDoc
        let idQuery = [_id := valueAt _id origDoc]

        eDoc <- findAndModify (selectQuery $ idQuery ++ [handled =: False])
                             ["$set" =: [handled =: True]]
        case eDoc of
          Right doc -> do
              liftIO $ successCb doc
              return (at dataField doc)
          Left err  ->  do
              -- a different cursor can lock this first by setting handled to True
              -- verify that this is what happened
              mDoc <- findOne (selectQuery idQuery)
              case mDoc of
                Nothing  -> liftIO $ throwIO $ FindAndModifyError err
                Just _ -> processNext cursor

    selectQuery query = (select query collection) {
        sort = ["$natural" =: -1]
      }

handleDroppedCursor :: (MonadIO m, MonadBaseControl IO m, Functor m) => Action m Cursor -> (Cursor -> Action m Document) -> SomeException -> Action m Document
handleDroppedCursor getCursor nextDoc _ = do
    liftIO ( threadDelay (1000 * 1000) ) >> (getCursor >>= nextDoc)

nextDocTail :: (MonadIO m, MonadBaseControl IO m, Functor m) => Cursor -> Action m Document
nextDocTail cursor = do
  n <- next cursor
  case n of
    Nothing -> nextDocTail cursor
    (Just doc) -> return doc

nextDocPoll :: (MonadIO m, MonadBaseControl IO m, Functor m) => Action m Cursor -> Cursor -> Action m Document
nextDocPoll getCursor cursor = do
    n <- next cursor
    case n of
        Nothing -> do
            liftIO $ threadDelay (1000 * 100) -- 100 ms
            getCursor >>= nextDocPoll getCursor
        (Just doc) -> return doc


{-
-- | Perform the action every time there is a new message.
-- And then marks the message as handled.
-- Does not call ForkIO, blocks the program
--
-- Do not call this multiple times against the same TailBroker
work :: TailBroker -> (Document -> Action IO ()) -> IO ()
work qw handler = loop
  where
    loop = do
      doc <- nextFromQueue qw
      handler doc
      loop
      -}
