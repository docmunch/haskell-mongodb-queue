import Test.Hspec
import Setup

import Data.Text (Text)
import Prelude hiding (lookup)
import Database.MongoDB
import Database.MongoDB.Queue


main :: IO ()
main = hspec $ do
  describe "MongoDB queue" $ do
    it "should peek at what was emitted" $ do
      emitter <- createEmitter runDB
      worker <- createWorker runDB
      emit emitter [("hello" :: Text) =: ("world" :: Text)]

      doc <- peek worker
      Just world <- lookup ("hello" :: Text) doc 
      world `shouldBe` ("world" :: Text)

      doc <- peek worker
      Just world <- lookup "hello" doc 
      world `shouldBe` ("world" :: Text)
