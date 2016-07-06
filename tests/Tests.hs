module Main where

-- local imports

import Control.Consensus.Paxos
import Control.Consensus.Paxos.Network.Server

import Network.Transport.Memory

import qualified TestChooseDecree as TC
import qualified TestMajority as TM
import SimpleDecree

-- external imports

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
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

main :: IO ()
main = defaultMain allTests

allTests :: [Test.Framework.Test]
allTests = [
  testCase "member-id" testMemberIdFactory,
  testCase "ledger" testLedgerFactory,
  testCase "1-ballot" test1Ballot
  ]
  ++ TM.tests
  ++ TC.tests

testMemberIdFactory :: Assertion
testMemberIdFactory = do
  mid1 <- mkMemberId
  mid2 <- mkMemberId
  assertBool "All member Ids are unique" $ mid1 /= mid2

testLedgerFactory :: Assertion
testLedgerFactory = do
  let instanceId = InstanceId 1
      members = S.fromList [MemberId 1, MemberId 2, MemberId 3]
      me = MemberId 1
  vLedger <- mkTLedger instanceId members me :: IO (TLedger IntegerOperation)
  paxos vLedger $ do
    ledger <- safely get
    io $ assertEqual "instance ids" (paxosInstanceId ledger) instanceId
    io $ assertEqual "members" (paxosMembers ledger) members
    io $ assertEqual "me" (paxosMemberId ledger) me

test1Ballot :: Assertion
test1Ballot = do
  let instanceId = InstanceId 1
      mid1 = MemberId 1
      mid2 = MemberId 2
      mid3 = MemberId 3
      members = S.fromList [mid1, mid2, mid3]
      memberNames = M.fromSet (Name . show) members
      decree = Decree {
        decreeInstanceId = instanceId,
        decreeMemberId = mid1,
        decreeable = SetValue 1
      }
  vLedger1 <- mkTLedger instanceId members mid1
  vLedger2 <- mkTLedger instanceId members mid2
  vLedger3 <- mkTLedger instanceId members mid3

  endpoint1 <- newEndpoint
  endpoint2 <- newEndpoint
  endpoint3 <- newEndpoint

  timeBound maxTestRun $
    withTransport newMemoryTransport $ \transport ->
      withAsync (runFollower1Ballot transport endpoint1 vLedger1 memberNames) $ \async1 ->
        withAsync (runFollower1Ballot transport endpoint2 vLedger2 memberNames) $ \async2 ->
          withAsync (runFollower1Ballot transport endpoint3 vLedger3 memberNames) $ \async3 ->
            withConnection3 transport endpoint1 (Name $ show mid1) (Name $ show mid2) (Name $ show mid3) $ do
              traceIO "before leading"
              threadDelay (250 * 1000 :: Int)
              leader1 <- runLeader1Ballot endpoint1 vLedger1 memberNames decree
              traceIO "before waiting on followers"
              follower1 <- wait async1
              (follower2,follower3) <- waitBoth async2 async3
              traceIO $ "Results are : " ++ show leader1 ++ " " ++ show follower1 ++ " "++ show follower2 ++ " "++ show follower3 ++ " "
              assertBool "expected leader decree" $ leader1 == Just decree
              assertBool "expected follower2 decree" $ leader1 == follower1
              assertBool "expected follower2 decree" $ follower1 == follower2
              assertBool "expected follower3 decree" $ follower2 == follower3

runFollower1Ballot :: (Decreeable d) => Transport -> Endpoint -> TLedger d -> MemberNames -> IO (Maybe (Decree d))
runFollower1Ballot transport endpoint vLedger memberNames = catch (do
    name <- atomically $ do
      ledger <- readTVar vLedger
      return $ memberName ledger memberNames
    withEndpoint transport endpoint $
      withBinding transport endpoint name $ do
        let p = protocol endpoint memberNames name
        traceIO $ "starting to follow on " ++ show name
        paxos vLedger $ followBasicPaxosBallot p)
  (\e -> do
    traceIO $ "follower error: " ++ show (e :: SomeException)
    return Nothing)

runLeader1Ballot :: (Decreeable d) =>  Endpoint -> TLedger d -> MemberNames -> Decree d -> IO (Maybe (Decree d))
runLeader1Ballot endpoint vLedger memberNames decree = catch (do
    name <- atomically $ do
      ledger <- readTVar vLedger
      return $ memberName ledger memberNames
    let p = protocol endpoint memberNames name
    traceIO $ "starting to lead : " ++ show name
    paxos vLedger $ leadBasicPaxosBallot p decree)
  (\e -> do
    traceIO $ "leader error: "  ++ show (e :: SomeException)
    return Nothing)

timeBound :: Int -> IO () -> IO ()
timeBound delay action = do
  outcome <- timeout delay action
  assertBool "Test should not block" $ outcome == Just ()

maxTestRun :: Int
maxTestRun = 5000 * 1000 -- 5 sec
