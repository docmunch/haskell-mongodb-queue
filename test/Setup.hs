module Setup where

import Database.MongoDB
import Control.Monad.IO.Class (MonadIO, liftIO)

{-
withRunDB wantsRunDB = do
  pipe <- liftIO $ runIOE $ connect (host "127.0.0.1")
  wantsRunDB $ \act -> do 
    Right res <- access pipe master "test" act
    return res
  close pipe
-}

runDB :: MonadIO m => Action m a -> m a
runDB act = do
  pipe <- liftIO $ runIOE $ connect (host "127.0.0.1")
  Right res <- access pipe master "test" act
  -- liftIO $ close pipe
  return res
