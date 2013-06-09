import Test.Hspec
import Setup

import Data.Text (Text)
import Prelude hiding (lookup)
import Database.MongoDB
import Database.MongoDB.Queue
import Control.Concurrent


main :: IO ()
main = hspec $ do
  describe "MongoDB queue" $ do
    it "should handle what was emitted" $ do
      emitter <- createEmitter runDB
      worker <- createWorker runDB
      emit emitter [("hello" :: Text) =: ("world" :: Text)]

      doc <- nextFromQueue worker
      Just world <- lookup ("hello" :: Text) doc 
      world `shouldBe` ("world" :: Text)

    it "should wait for a document to be inserted" $ do
      emitter <- createEmitter runDB
      worker <- createWorker runDB
      forkIO $ do
        threadDelay $ 1 * 1000 * 1000 -- microseconds, 1 second
        emit emitter [("hello" :: Text) =: ("world" :: Text)]
      doc <- nextFromQueue worker
      Just world <- lookup ("hello" :: Text) doc 
      world `shouldBe` ("world" :: Text)
      return ()
