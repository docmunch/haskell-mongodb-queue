import Test.Hspec
import Setup

import Data.Text (Text)
import Prelude hiding (lookup)
import Database.MongoDB
import Database.MongoDB.Queue
import Control.Concurrent


main :: IO ()
main = hspec $
  describe "MongoDB queue" $ do
    it "should handle what was emitted" $ do
      emitter <- runDB createEmitter
      worker <- runDB createWorker
      runDB $ emit emitter [("hello" :: Text) =: ("world" :: Text)]

      doc <- runDB $ nextFromQueue worker
      Just world <- lookup ("hello" :: Text) doc 
      world `shouldBe` ("world" :: Text)

    it "should wait for a document to be inserted" $ do
      emitter <- runDB createEmitter
      worker <- runDB createWorker
      forkIO $ do
        threadDelay $ 1 * 1000 * 1000 -- microseconds, 1 second
        runDB $ emit emitter [("hello" :: Text) =: ("world" :: Text)]
      doc <- runDB $ nextFromQueue worker
      Just world <- lookup ("hello" :: Text) doc 
      world `shouldBe` ("world" :: Text)
      return ()
