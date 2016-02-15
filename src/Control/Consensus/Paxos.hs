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
  leadBasicPaxosRound,

  module Control.Consensus.Paxos.Types

) where

-- local imports
import Control.Consensus.Paxos.Types

-- external imports

import Control.Concurrent.STM

import qualified Data.Map as M
import Data.Maybe
import Data.Serialize

import Network.Endpoints
import Network.RPC.Typed

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
Continue inititiating rounds of Paxos until the decree is finally accepted.

This will guarantee the decree is eventually accepted, provided the caller doesn't crash.
-}
leadBasicPaxosInstance :: (Decreeable d) => Paxos d -> d -> IO (Maybe (Decree d))
leadBasicPaxosInstance p d = do
  let memberId = paxosMemberId p
      decree = Decree {
        decreeMemberId = memberId,
        decreeable = d
      }
  maybeDecree <- leadBasicPaxosRound p decree
  case maybeDecree of
    -- if this decree is accepted, we are done
    Just c | decreeMemberId c == memberId -> return $ Just c
    -- if the response is Nothing or another decree, keep trying
    _ -> leadBasicPaxosInstance p d

{-|
Lead one round of voting, with one of 3 possible outcomes:

* The proposed decree is accepted
* Another decree proposed by another `Member` is accepted
* No decree is accepted

-}

leadBasicPaxosRound :: (Decreeable d) => Paxos d -> Decree d -> IO (Maybe (Decree d))
leadBasicPaxosRound p d = do
  votes <- preparation p
  let maybeChosenDecree = chooseDecree p d votes
  case maybeChosenDecree of
    Nothing -> return Nothing
    Just chosenDecree -> do
      promised <- proposition p d
      if promised
        then acceptance p chosenDecree
        else return Nothing

preparation :: (Decreeable d) => Paxos d -> IO (Votes d)
preparation p = do
  proposedBallotNumber <- atomically $ incrementNextProposedBallotNumber p
  let prep = Prepare {
        prepareInstanceId = instanceId p,
        tentativeBallotNumber = proposedBallotNumber
        }
  votes <- prepare p prep
  atomically $ do
    ballotNumber <- maxBallotNumber p votes
    setNextBallotNumber p ballotNumber
  return votes

chooseDecree :: (Decreeable d) => Paxos d -> Decree d -> Votes d -> Maybe (Decree d)
chooseDecree p decree votes =
  if isMajority p votes $ \vote ->
    case vote of
      Dissent{} -> False
      _ -> True
    -- we didn't hear from a majority of members--we have no common decree
    then Nothing
    -- we did hear from the majority
    else case maximum votes of
      Nothing -> Just decree
      Just Assent -> Just decree
      Just vote -> Just $ voteDecree vote

proposition :: (Decreeable d) => Paxos d -> Decree d -> IO Bool
proposition p d = do
  proposedBallotNumber <- atomically $ nextProposedBallotNumber p
  let proposal = Proposal {
    proposalInstanceId = instanceId p,
    proposedBallotNumber = proposedBallotNumber,
    proposedDecree = d
  }
  votes <- propose p proposal
  atomically $ do
    ballotNumber <- maxBallotNumber p votes
    setNextBallotNumber p ballotNumber
  return $ isMajority p votes $ \vote ->
    case vote of
      Vote {} ->
        (voteBallotNumber vote == proposedBallotNumber) &&
         (voteInstanceId vote == paxosMemberId p)
      Dissent {} -> False

acceptance :: (Decreeable d) => Paxos d -> Decree d -> IO (Maybe (Decree d))
acceptance p d = do
  responses <- accept p d
  if isMajority p responses id
    then return $ Just d
    else return Nothing

--
-- Actual protocol
--

-- caller

prepare :: (Decreeable d) => Paxos d -> Prepare -> IO (Votes d)
prepare p = pcall p "prepare"

propose :: (Decreeable d) => Paxos d -> Proposal d-> IO (Votes d)
propose p = pcall p "propose"

accept :: (Decreeable d) => Paxos d -> Decree d -> IO (M.Map Name (Maybe Bool))
accept p = pcall p "accept"

-- callee

onPrepare :: (Decreeable d) => Paxos d -> Prepare -> IO (Vote d)
onPrepare p prep = atomically $ do
  let preparedBallotNumber = tentativeBallotNumber prep
      vLedger = paxosLedger p
  ledger <- readTVar vLedger
  let ballotNumber = nextBallotNumber ledger
  if preparedBallotNumber > ballotNumber
    then do
      modifyTVar vLedger $ \ledger -> ledger {nextBallotNumber = preparedBallotNumber}
      case lastVote ledger of
        Just vote -> return vote
        Nothing -> return Assent
    else do
      modifyTVar vLedger $ \ledger -> ledger {nextBallotNumber = preparedBallotNumber}
      return Dissent {
        dissentInstanceId = instanceId p,
        dissentBallotNumber = ballotNumber
      }

onPropose :: (Decreeable d) => Paxos d -> Proposal d -> IO (Vote d)
onPropose p prop = atomically $ do
  ballotNumber <- nextProposedBallotNumber p
  if ballotNumber == proposedBallotNumber prop
    then do
      let instanceId = proposalInstanceId prop
          decree = proposedDecree prop
          vote = Vote {
            voteInstanceId = instanceId,
            voteBallotNumber = ballotNumber,
            voteDecree = decree
          }
      setLastVote p vote
      return vote
    else
      return Dissent {
          dissentInstanceId = proposalInstanceId prop,
          dissentBallotNumber = ballotNumber
        }

onAccept :: (Decreeable d) => Paxos d -> Decree d -> IO d
onAccept p d = return $ decreeable d

---
--- Ledger functions
---

incrementNextProposedBallotNumber :: Paxos d -> STM Integer
incrementNextProposedBallotNumber p = do
  let vLedger = paxosLedger p
  modifyTVar vLedger $ \ledger ->
    let lastProposed = lastProposedBallotNumber ledger
        nextHeard = nextBallotNumber ledger
    in ledger {
      lastProposedBallotNumber = 1 + max lastProposed nextHeard
      }
  nextProposedBallotNumber p

nextProposedBallotNumber :: Paxos d -> STM Integer
nextProposedBallotNumber p = do
  ledger <- readTVar $ paxosLedger p
  return $ lastProposedBallotNumber ledger

setLastVote :: Paxos d -> Vote d-> STM (Vote d)
setLastVote p vote = do
  ballotNumber <- nextProposedBallotNumber p
  let vLedger = paxosLedger p
  modifyTVar vLedger $ \ledger -> ledger { lastVote = Just vote}
  return vote

setNextBallotNumber :: Paxos d -> Integer -> STM ()
setNextBallotNumber p newNextBallotNumber = do
  let vLedger = paxosLedger p
  modifyTVar vLedger $ \ledger ->
    let ballotNumber = nextBallotNumber ledger
    in ledger {nextBallotNumber = max ballotNumber newNextBallotNumber}

--
-- Utility
--

{-|
Return true if there are enough responses that are not Nothing and which pass
the supplied test.
-}
isMajority :: Paxos d -> M.Map Name (Maybe v) -> (v -> Bool)-> Bool
isMajority p votes test =
  let actualVotes = filter isJust $ M.elems votes
      countedVotes = filter (\(Just v) -> test v) actualVotes
  in (toInteger . length) countedVotes >= (toInteger . M.size $ paxosMembers p) `quot` 2

maxBallotNumber :: Paxos d -> Votes d -> STM Integer
maxBallotNumber p votes = do
  let vLedger = paxosLedger p
  ledger <- readTVar vLedger
  let maxVote = maximum votes
      ballotNumber = nextBallotNumber ledger
  return $ case maxVote of
    Just vote -> case vote of
      Dissent{} -> max (dissentBallotNumber vote) ballotNumber
      Assent -> ballotNumber
      Vote{} -> max (voteBallotNumber vote) ballotNumber
    Nothing -> ballotNumber

  -- return $ foldr (\vote maxSoFar -> maximum maxSoFar (voteBallotNumber vote ))
  --   (nextBallotNumber ledger) M.elems votes

{-|
Invoke a method on members of the Paxos instance. Because of the semantics of `gcallWithTimeout`, there
will be a response for every `Member`, even if it's just `Nothing`.
-}
pcall :: (Decreeable d,Serialize a,Serialize r) => Paxos d -> String -> a -> IO (M.Map Name (Maybe r))
pcall p method args = do
  let cs = newCallSite (paxosEndpoint p) (paxosName p)
      members = M.keys $ paxosMembers p
  gcallWithTimeout cs members method (fromInteger $ paxosTimeout p) args
