{-# LANGUAGE ExistentialQuantification #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus
-- Copyright   :  (c) Phil Hargett 2015
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  experimental
-- Portability :  non-portable (uses STM)
--
--
-----------------------------------------------------------------------------

module Control.Consensus.Paxos (

  leadBasicPaxosInstance,
  leadBasicPaxosBallot,

  onPrepare,
  onPropose,
  onAccept,

  mkMemberId,

  module Control.Consensus.Paxos.Types

) where

-- local imports
import Control.Consensus.Paxos.Types

-- external imports

import Control.Concurrent.STM

import qualified Data.Map as M
import Data.Maybe
import qualified Data.Set as S

import qualified System.Random as R

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
Continue inititiating ballots with Paxos until the decree is finally accepted.

This will guarantee the decree is eventually accepted, provided the caller doesn't crash.
-}
leadBasicPaxosInstance :: (Decreeable d) => Paxos d -> Ledger d -> d -> IO (Ledger d,Maybe (Decree d))
leadBasicPaxosInstance p m d = do
  let memberId = paxosMemberId m
      decree = Decree {
        decreeInstanceId = paxosInstanceId m,
        decreeMemberId = memberId,
        decreeable = d
      }
  (m1,maybeDecree) <- leadBasicPaxosBallot p m decree
  case maybeDecree of
    -- if this decree is accepted, we are done
    Just c | decreeMemberId c == memberId -> return (m1,Just c)
    -- if the response is Nothing or another decree, keep trying
    _ -> leadBasicPaxosInstance p m d

{-|
Lead one ballot of voting, with one of 3 possible outcomes:

* The proposed decree is accepted
* Another decree proposed by another `Member` is accepted
* No decree is accepted
-}
leadBasicPaxosBallot :: (Decreeable d) => Paxos d -> Ledger d -> Decree d -> IO (Ledger d, Maybe (Decree d))
leadBasicPaxosBallot p m d = do
  (m1,earlierVotes) <- preparation p m
  let maybeChosenDecree = chooseDecree (paxosMembers m1) d earlierVotes
  case maybeChosenDecree of
    Nothing -> return (m1,Nothing)
    Just chosenDecree -> do
      (m2,promised) <- proposition p m1 d
      if promised
        then acceptance p m chosenDecree
        else return (m2,Nothing)

preparation :: (Decreeable d) => Paxos d -> Ledger d -> IO (Ledger d,Votes d)
preparation proposer p = do
  let p1 = incrementNextProposedBallotNumber p
  let prep = Prepare {
        prepareInstanceId = paxosInstanceId p1,
        tentativeBallotNumber = lastProposedBallotNumber p1
        }
  votes <- prepare proposer p1 prep
  let ballotNumber = maxBallotNumber p1 votes
      p2 = setNextExpectedBallotNumber p1 ballotNumber
  return (p2,votes)

chooseDecree :: (Decreeable d) => Members -> Decree d -> Votes d -> Maybe (Decree d)
chooseDecree members decree votes =
  if isMajority members votes $ \vote ->
    case vote of
      Dissent{} -> False
      _ -> True
    -- we didn't hear from a majority of members--we have no common decree
    then Nothing
    -- we did hear from the majority
    else case maximum votes of
      -- there was no other preferred decree, so use ours
      Nothing -> Just decree
      -- there is agreement on ours
      Just Assent -> Just decree
      -- pick the latest one from earlier
      Just vote -> Just $ voteDecree vote

proposition :: (Decreeable d) => Paxos d -> Ledger d -> Decree d -> IO (Ledger d,Bool)
proposition proposer p d = do
  let proposed = nextProposedBallotNumber p
      proposal = Proposal {
        proposalInstanceId = paxosInstanceId p,
        proposedBallotNumber = proposed,
        proposedDecree = d
      }
  votes <- propose proposer p proposal
  let ballotNumber = maxBallotNumber p votes
      p1 = setNextExpectedBallotNumber p ballotNumber
      success = isMajority (paxosMembers p1) votes $ \vote ->
        case vote of
          Vote {} ->
            (voteBallotNumber vote == proposed) &&
             (voteInstanceId vote == paxosInstanceId p1)
          Assent -> True
          Dissent {} -> False
  return (p1,success)

acceptance :: (Decreeable d) => Paxos d -> Ledger d -> Decree d -> IO (Ledger d,Maybe (Decree d))
acceptance p m d = do
  responses <- accept p m d
  if isMajority (paxosMembers m) responses $ const True
    then return (m,Just d)
    else return (m,Nothing)

--
-- Actual protocol
--

-- callee

onPrepare :: (Decreeable d) => Ledger d -> Prepare -> IO (Ledger d,Vote d)
onPrepare p prep = atomically $ do
  let preparedBallotNumber = tentativeBallotNumber prep
      ballotNumber = nextExpectedBallotNumber p
      p1 = p {nextExpectedBallotNumber = preparedBallotNumber}
  if preparedBallotNumber > ballotNumber
    then
      case lastVote p1 of
        Just vote -> return (p1,vote)
        Nothing -> return (p1,Assent)
    else
      return (p1,Dissent {
        dissentInstanceId = paxosInstanceId p1,
        dissentBallotNumber = ballotNumber
      })

onPropose :: (Decreeable d) => Ledger d -> Proposal d -> IO (Ledger d, Vote d)
onPropose p prop = atomically $ do
  let ballotNumber = nextProposedBallotNumber p
  if ballotNumber == proposedBallotNumber prop
    then do
      let instanceId = proposalInstanceId prop
          decree = proposedDecree prop
          vote = Vote {
            voteInstanceId = instanceId,
            voteBallotNumber = ballotNumber,
            voteDecree = decree
          }
          p1 = setLastVote p vote
      return (p1,vote)
    else
      return (p,Dissent {
          dissentInstanceId = proposalInstanceId prop,
          dissentBallotNumber = ballotNumber
        })

onAccept :: (Decreeable d) => Ledger d -> Decree d -> IO (Ledger d,())
onAccept p _ = return (p,())

---
--- Ledger functions
---

incrementNextProposedBallotNumber :: Ledger d -> Ledger d
incrementNextProposedBallotNumber p =
  let BallotNumber lastProposed = lastProposedBallotNumber p
      BallotNumber nextExpected = nextExpectedBallotNumber p
    in p {
      lastProposedBallotNumber = BallotNumber $ 1 + max lastProposed nextExpected
      }

nextProposedBallotNumber :: Ledger d -> BallotNumber
nextProposedBallotNumber = lastProposedBallotNumber

setLastVote :: Ledger d -> Vote d -> Ledger d
setLastVote p vote =
  p {
  lastVote = Just vote
  }

setNextExpectedBallotNumber :: Ledger d -> BallotNumber -> Ledger d
setNextExpectedBallotNumber p nextBallotNumber =
  let nextExpected = nextExpectedBallotNumber p
    in p {nextExpectedBallotNumber = max nextExpected nextBallotNumber}

--
-- Factories
--

mkMemberId :: IO MemberId
mkMemberId = fmap MemberId R.randomIO

--
-- Utility
--

{-|
Return true if there are enough responses that are not Nothing and which pass
the supplied test.
-}
isMajority :: Members -> M.Map MemberId (Maybe v) -> (v -> Bool)-> Bool
isMajority members votes test =
  let actualVotes = filter isJust $ M.elems votes
      countedVotes = filter (\(Just v) -> test v) actualVotes
  in (toInteger . length) countedVotes >= (toInteger . S.size $ members) `quot` 2

maxBallotNumber :: Ledger d -> Votes d -> BallotNumber
maxBallotNumber p votes = do
  let maxVote = maximum votes
      ballotNumber = nextExpectedBallotNumber p
  case maxVote of
    Just vote -> case vote of
      Dissent{} -> max (dissentBallotNumber vote) ballotNumber
      Assent -> ballotNumber
      Vote{} -> max (voteBallotNumber vote) ballotNumber
    Nothing -> ballotNumber
