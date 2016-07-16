module TestBallots (
  tests
) where

-- local imports

import Control.Consensus.Paxos
import Control.Consensus.Paxos.Protocol.Courier

import Control.Consensus.Paxos.Storage.Memory

import Network.Transport.Memory

import SimpleDecree

-- external imports

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception

import qualified Data.Map as M
import qualified Data.Set as S

import Debug.Trace

import Network.Endpoints

import System.Timeout

import Test.Framework
import Test.HUnit
import Test.Framework.Providers.HUnit

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

tests :: [Test.Framework.Test]
tests = [
  testCase "1-ballot" test1Ballot
  ]

test1Ballot :: Assertion
test1Ballot = do
  let instId = InstanceId 1
      mid1 = MemberId 1
      mid2 = MemberId 2
      mid3 = MemberId 3
      members = S.fromList [mid1, mid2, mid3]
      memberNames = M.fromSet (Name . show) members
      decree = Decree {
        decreeInstanceId = instId,
        decreeMemberId = mid1,
        decreeable = SetValue 1
      }
  inst1 <- newInstance instId members mid1
  inst2 <- newInstance instId members mid2
  inst3 <- newInstance instId members mid3

  endpoint1 <- newEndpoint
  endpoint2 <- newEndpoint
  endpoint3 <- newEndpoint

  timeBound maxTestRun $
    withTransport newMemoryTransport $ \transport ->
      withAsync (runFollower1Ballot transport endpoint1 inst1 memberNames) $ \async1 ->
        withAsync (runFollower1Ballot transport endpoint2 inst2 memberNames) $ \async2 ->
          withAsync (runFollower1Ballot transport endpoint3 inst3 memberNames) $ \async3 ->
            withConnection3 transport endpoint1 (Name $ show mid1) (Name $ show mid2) (Name $ show mid3) $ do
              threadDelay (500 * 1000 :: Int)
              leader1 <- runLeader1Ballot endpoint1 inst1 memberNames decree
              follower1 <- wait async1
              (follower2,follower3) <- waitBoth async2 async3
              assertBool "expected leader decree" $ leader1 == Just decree
              assertBool "expected follower2 decree" $ leader1 == follower1
              assertBool "expected follower2 decree" $ follower1 == follower2
              assertBool "expected follower3 decree" $ follower2 == follower3

runFollower1Ballot :: (Decreeable d) => Transport -> Endpoint -> Instance d -> MemberNames -> IO (Maybe (Decree d))
runFollower1Ballot transport endpoint inst memberNames = catch (do
    let name = memberName inst memberNames
    withEndpoint transport endpoint $
      withBinding transport endpoint name $ do
        let p = protocol defaultTimeouts endpoint memberNames name
        s <- storage
        paxos inst $ followBasicPaxosBallot p s)
  (\e -> do
    traceIO $ "follower error: " ++ show (e :: SomeException)
    return Nothing)

runLeader1Ballot :: (Decreeable d) =>  Endpoint -> Instance d -> MemberNames -> Decree d -> IO (Maybe (Decree d))
runLeader1Ballot endpoint inst memberNames decree = catch (do
    let name = memberName inst memberNames
    let p = protocol defaultTimeouts endpoint memberNames name
    s <- storage
    paxos inst $ leadBasicPaxosBallot p s decree)
  (\e -> do
    traceIO $ "leader error: "  ++ show (e :: SomeException)
    return Nothing)

timeBound :: Int -> IO () -> IO ()
timeBound delay action = do
  outcome <- timeout delay action
  assertBool "Test should not block" $ outcome == Just ()

maxTestRun :: Int
maxTestRun = 5000 * 1000 -- 5 sec
