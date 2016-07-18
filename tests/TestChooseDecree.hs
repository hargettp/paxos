module TestChooseDecree (
  tests
)
where

  -- local imports

import Control.Consensus.Paxos

import SimpleDecree

-- external imports

import qualified Data.Map as M
import qualified Data.Set as S

import Test.Framework
import Test.HUnit
import Test.Framework.Providers.HUnit

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

tests :: [Test.Framework.Test]
tests = [
  testCase "no votes" testNoVotes,
  testCase "unanimous no existing decree" testNoExistingDecree,
  testCase "majority no existing decree" testNoExistingDecreeMajority,
  testCase "1 dissent 1 assent no existing decree" testNoExistingDecreeWith1Dissent1Assent,
  testCase "2 dissents no existing decree" testNoExistingDecreeWith2Dissents
  ]

testNoVotes :: Assertion
testNoVotes =
  with3Votes $ \instId members mid1 mid2 mid3 ->
    let decree = Decree {
          decreeInstanceId = instId,
          decreeMemberId = mid1,
          decreeable = SetValue 1
        }
        votes = M.fromList [(mid1, Nothing),(mid2, Nothing),(mid3, Nothing)]
        chosenDecree  = chooseDecree members decree votes
      in assertEqual "No votes should result in no decree" Nothing chosenDecree

testNoExistingDecree :: Assertion
testNoExistingDecree =
  with3Votes $ \instId members mid1 mid2 mid3 ->
    let votes = M.fromList [(mid1, Just Assent),(mid2, Just Assent),(mid3, Just Assent)]
        decree = Decree {
          decreeInstanceId = instId,
          decreeMemberId = mid1,
          decreeable = SetValue 1
        }
        chosenDecree  = chooseDecree members decree votes
      in assertEqual "Unanimous assent with no existing decree should be decree" (Just decree) chosenDecree

testNoExistingDecreeMajority :: Assertion
testNoExistingDecreeMajority =
  with3Votes $ \instId members mid1 mid2 mid3 ->
    let votes = M.fromList [(mid1, Just Assent),(mid2, Just Assent),(mid3, Nothing)]
        decree = Decree {
          decreeInstanceId = instId,
          decreeMemberId = mid1,
          decreeable = SetValue 1
        }
        chosenDecree  = chooseDecree members decree votes
      in assertEqual "Majority assent with no existing decree should be decree" (Just decree) chosenDecree

testNoExistingDecreeWith1Dissent1Assent :: Assertion
testNoExistingDecreeWith1Dissent1Assent =
  with3Votes $ \instId members mid1 mid2 mid3 ->
    let votes = M.fromList [(mid1, Just Assent),(mid2, Just $ Dissent (BallotNumber 2)),(mid3, Nothing)]
        decree = Decree {
          decreeInstanceId = instId,
          decreeMemberId = mid1,
          decreeable = SetValue 1
        }
        chosenDecree  = chooseDecree members decree votes
      in assertEqual "1 dissent, 1 assent with no existing decree should be nothing" Nothing chosenDecree

testNoExistingDecreeWith2Dissents :: Assertion
testNoExistingDecreeWith2Dissents =
  with3Votes $ \instId members mid1 mid2 mid3 ->
    let votes = M.fromList [
          (mid1, Just Assent),
          (mid2, Just $ Dissent (BallotNumber 2)),
          (mid3, Just $ Dissent (BallotNumber 2))
          ]
        decree = Decree {
          decreeInstanceId = instId,
          decreeMemberId = mid1,
          decreeable = SetValue 1
        }
        chosenDecree  = chooseDecree members decree votes
      in assertEqual "2 dissents with no existing decree should be nothing" Nothing chosenDecree

with3Votes :: (InstanceId -> Members -> MemberId -> MemberId -> MemberId -> a) -> a
with3Votes fn =
  let instId = InstanceId 1
      mid1 = MemberId 1
      mid2 = MemberId 2
      mid3 = MemberId 3
      members = S.fromList [mid1, mid2, mid3]
    in fn instId members mid1 mid2 mid3
