module Main where

-- local imports

import Control.Consensus.Paxos

import qualified TestBallots as TB
import qualified TestChooseDecree as TC
import qualified TestMajority as TM
import SimpleDecree

-- external imports

import qualified Data.Set as S

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
  testCase "ledger" tesInstanceFactory
  ]
  ++ TB.tests
  ++ TM.tests
  ++ TC.tests

testMemberIdFactory :: Assertion
testMemberIdFactory = do
  mid1 <- mkMemberId
  mid2 <- mkMemberId
  assertBool "All member Ids are unique" $ mid1 /= mid2

tesInstanceFactory :: Assertion
tesInstanceFactory = do
  let instId = InstanceId 1
      members = S.fromList [MemberId 1, MemberId 2, MemberId 3]
      me = MemberId 1
  inst <- newInstance instId members me :: IO (Instance IntegerOperation)
  paxos inst $ do
    ledger <- safely get
    io $ assertEqual "instance ids" (instanceId inst) instId
    io $ assertEqual "members" (paxosMembers ledger) members
    io $ assertEqual "me" (instanceMe inst) me
