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
leadBasicPaxosInstance :: (Decreeable d) => Proposer d -> Paxos d -> d -> IO (Paxos d,Maybe (Decree d))
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
leadBasicPaxosBallot :: (Decreeable d) => Proposer d -> Paxos d -> Decree d -> IO (Paxos d, Maybe (Decree d))
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

preparation :: (Decreeable d) => Proposer d -> Paxos d -> IO (Paxos d,Votes d)
preparation p m = do
  proposed <- atomically $ incrementNextProposedBallotNumber m
  let prep = Prepare {
        prepareInstanceId = paxosInstanceId m,
        tentativeBallotNumber = proposed
        }
  votes <- prepare p m prep
  atomically $ do
    ballotNumber <- maxBallotNumber m votes
    setNextExpectedBallotNumber m ballotNumber
  return (m,votes)

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

proposition :: (Decreeable d) => Proposer d -> Paxos d -> Decree d -> IO (Paxos d,Bool)
proposition p m d = do
  proposed <- atomically $ nextProposedBallotNumber m
  let proposal = Proposal {
    proposalInstanceId = paxosInstanceId m,
    proposedBallotNumber = proposed,
    proposedDecree = d
  }
  votes <- propose p m proposal
  atomically $ do
    ballotNumber <- maxBallotNumber m votes
    setNextExpectedBallotNumber m ballotNumber
  let success = isMajority (paxosMembers m) votes $ \vote ->
        case vote of
          Vote {} ->
            (voteBallotNumber vote == proposed) &&
             (voteInstanceId vote == paxosInstanceId m)
          Assent -> True
          Dissent {} -> False
  return (m,success)

acceptance :: (Decreeable d) => Proposer d -> Paxos d -> Decree d -> IO (Paxos d,Maybe (Decree d))
acceptance p m d = do
  responses <- accept p m d
  if isMajority (paxosMembers m) responses id
    then return (m,Just d)
    else return (m,Nothing)

--
-- Actual protocol
--

-- callee

onPrepare :: (Decreeable d) => Preparation d
onPrepare member prep = atomically $ do
  let preparedBallotNumber = tentativeBallotNumber prep
      vLedger = paxosLedger member
  ledger <- readTVar vLedger
  let ballotNumber = nextExpectedBallotNumber ledger
  if preparedBallotNumber > ballotNumber
    then do
      modifyTVar vLedger $ \oldLedger -> oldLedger {nextExpectedBallotNumber = preparedBallotNumber}
      case lastVote ledger of
        Just vote -> return (member,vote)
        Nothing -> return (member,Assent)
    else do
      modifyTVar vLedger $ \oldLedger -> oldLedger {nextExpectedBallotNumber = preparedBallotNumber}
      return (member,Dissent {
        dissentInstanceId = paxosInstanceId member,
        dissentBallotNumber = ballotNumber
      })

onPropose :: (Decreeable d) => Proposition d
onPropose member prop = atomically $ do
  ballotNumber <- nextProposedBallotNumber member
  if ballotNumber == proposedBallotNumber prop
    then do
      let instanceId = proposalInstanceId prop
          decree = proposedDecree prop
          vote = Vote {
            voteInstanceId = instanceId,
            voteBallotNumber = ballotNumber,
            voteDecree = decree
          }
      setLastVote member vote
      return (member,vote)
    else
      return (member,Dissent {
          dissentInstanceId = proposalInstanceId prop,
          dissentBallotNumber = ballotNumber
        })

onAccept :: (Decreeable d) => Acceptance d
onAccept member d = do
  success <- acceptDecree member $ decreeable d
  return (member,success)

---
--- Ledger functions
---

incrementNextProposedBallotNumber :: Paxos d -> STM BallotNumber
incrementNextProposedBallotNumber m = do
  let vLedger = paxosLedger m
  modifyTVar vLedger $ \ledger ->
    let BallotNumber lastProposed = lastProposedBallotNumber ledger
        BallotNumber nextExpected = nextExpectedBallotNumber ledger
    in ledger {
      lastProposedBallotNumber = BallotNumber $ 1 + max lastProposed nextExpected
      }
  nextProposedBallotNumber m

nextProposedBallotNumber :: Paxos d -> STM BallotNumber
nextProposedBallotNumber m = do
  ledger <- readTVar $ paxosLedger m
  return $ lastProposedBallotNumber ledger

setLastVote :: Paxos d -> Vote d-> STM ()
setLastVote p vote = do
  let vLedger = paxosLedger p
  modifyTVar vLedger $ \ledger -> ledger { lastVote = Just vote}

setNextExpectedBallotNumber :: Paxos d -> BallotNumber -> STM ()
setNextExpectedBallotNumber p nextBallotNumber = do
  let vLedger = paxosLedger p
  modifyTVar vLedger $ \ledger ->
    let nextExpected = nextExpectedBallotNumber ledger
    in ledger {nextExpectedBallotNumber = max nextExpected nextBallotNumber}

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

maxBallotNumber :: Paxos d -> Votes d -> STM BallotNumber
maxBallotNumber p votes = do
  ledger <- readTVar $ paxosLedger p
  let maxVote = maximum votes
      ballotNumber = nextExpectedBallotNumber ledger
  return $ case maxVote of
    Just vote -> case vote of
      Dissent{} -> max (dissentBallotNumber vote) ballotNumber
      Assent -> ballotNumber
      Vote{} -> max (voteBallotNumber vote) ballotNumber
    Nothing -> ballotNumber

  -- return $ foldr (\vote maxSoFar -> maximum maxSoFar (voteBallotNumber vote ))
  --   (nextExpectedBallotNumber ledger) M.elems votes
