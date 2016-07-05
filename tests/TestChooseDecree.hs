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
  testCase "no existing decree" testNoExistingDecree
  ]

testNoExistingDecree :: Assertion
testNoExistingDecree = do
  let instanceId = InstanceId 1
      mid1 = MemberId 1
      mid2 = MemberId 2
      mid3 = MemberId 3
      members = S.fromList [mid1, mid2, mid3]
      votes = M.fromList [(mid1, Nothing),(mid2, Nothing),(mid3, Nothing)]
      decree = Decree {
        decreeInstanceId = instanceId,
        decreeMemberId = mid1,
        decreeable = SetValue 1
      }
      chosenDecree  = chooseDecree members decree votes
  assertEqual "No votes should result in no decree" Nothing chosenDecree
