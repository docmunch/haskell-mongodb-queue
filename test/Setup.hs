module Setup where

import Database.MongoDB
import Control.Monad.IO.Class (MonadIO, liftIO)
import System.Environment ( lookupEnv )
import Data.Maybe (fromMaybe)

runDB :: MonadIO m => Action m a -> m a
runDB act = do
  let mHost = Just $ Host "127.0.0.1" (PortNumber 49154)
  -- mHost <- liftIO $ lookupEnv "MONGODB_PORT_27017_TCP_ADDR"
  pipe <- liftIO $ runIOE $ connect (fromMaybe (host "127.0.0.1") mHost)
  res <- access pipe master "test" act
  case res of
    Left res -> error $ show res
    Right ok -> return ok
  -- liftIO $ close pipe
