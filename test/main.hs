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
      let helloWorld createWorker getNext = do
              doc <- runDB $ do
                  emitter <- createEmitter
                  worker <- createWorker
                  emit emitter [("hello" :: Text) =: ("world" :: Text)]
                  getNext worker

              Just world <- lookup ("hello" :: Text) doc 
              world `shouldBe` ("world" :: Text)

      helloWorld createTailBroker nextFromQueueTail
      helloWorld createPollBroker nextFromQueuePoll


    it "should wait for a document to be inserted" $ do
      let waitForInsertion createWorker getNext = do
              emitter <- runDB createEmitter
              worker <- runDB createWorker
              forkIO $ do
                threadDelay $ 1 * 1000 * 1000 -- microseconds, 1 second
                runDB $ emit emitter [("hello" :: Text) =: ("world" :: Text)]
              doc <- runDB $ getNext worker
              Just world <- lookup ("hello" :: Text) doc 
              world `shouldBe` ("world" :: Text)

      waitForInsertion createTailBroker nextFromQueueTail
      waitForInsertion createPollBroker nextFromQueuePoll
