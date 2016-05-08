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
import Data.Serialize
import qualified Data.Set as S

import Network.Endpoints
import Network.RPC.Typed

import qualified System.Random as R

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
Continue inititiating rounds of Paxos until the decree is finally accepted.

This will guarantee the decree is eventually accepted, provided the caller doesn't crash.
-}
leadBasicPaxosInstance :: (Decreeable d) => Member d -> d -> IO (Maybe (Decree d))
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
leadBasicPaxosRound :: (Decreeable d) => Member d -> Decree d -> IO (Maybe (Decree d))
leadBasicPaxosRound p d = do
  earlierVotes <- preparation p
  let maybeChosenDecree = chooseDecree p d earlierVotes
  case maybeChosenDecree of
    Nothing -> return Nothing
    Just chosenDecree -> do
      promised <- proposition p d
      if promised
        then acceptance p chosenDecree
        else return Nothing

preparation :: (Decreeable d) => Member d -> IO (Votes d)
preparation p = do
  proposed <- atomically $ incrementNextProposedBallotNumber p
  let prep = Prepare {
        prepareInstanceId = paxosInstanceId p,
        tentativeBallotNumber = proposed
        }
  votes <- prepare p prep
  atomically $ do
    ballotNumber <- maxBallotNumber p votes
    setNextExpectedBallotNumber p ballotNumber
  return votes

chooseDecree :: (Decreeable d) => Member d -> Decree d -> Votes d -> Maybe (Decree d)
chooseDecree p decree votes =
  if isMajority p votes $ \vote ->
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

proposition :: (Decreeable d) => Member d -> Decree d -> IO Bool
proposition p d = do
  proposed <- atomically $ nextProposedBallotNumber p
  let proposal = Proposal {
    proposalInstanceId = paxosInstanceId p,
    proposedBallotNumber = proposed,
    proposedDecree = d
  }
  votes <- propose p proposal
  atomically $ do
    ballotNumber <- maxBallotNumber p votes
    setNextExpectedBallotNumber p ballotNumber
  return $ isMajority p votes $ \vote ->
    case vote of
      Vote {} ->
        (voteBallotNumber vote == proposed) &&
         (voteInstanceId vote == paxosInstanceId p)
      Assent -> True
      Dissent {} -> False

acceptance :: (Decreeable d) => Member d -> Decree d -> IO (Maybe (Decree d))
acceptance p d = do
  responses <- accept p d
  if isMajority p responses id
    then return $ Just d
    else return Nothing

--
-- Actual protocol
--

-- caller

prepare :: (Decreeable d) => Member d -> Prepare -> IO (Votes d)
prepare p = pcall p "prepare"

propose :: (Decreeable d) => Member d -> Proposal d-> IO (Votes d)
propose p = pcall p "propose"

accept :: (Decreeable d) => Member d -> Decree d -> IO (M.Map Name (Maybe Bool))
accept p = pcall p "accept"

-- callee

onPrepare :: (Decreeable d) => Member d -> Prepare -> IO (Vote d)
onPrepare p prep = atomically $ do
  let preparedBallotNumber = tentativeBallotNumber prep
      vLedger = paxosLedger p
  ledger <- readTVar vLedger
  let ballotNumber = nextExpectedBallotNumber ledger
  if preparedBallotNumber > ballotNumber
    then do
      modifyTVar vLedger $ \oldLedger -> oldLedger {nextExpectedBallotNumber = preparedBallotNumber}
      case lastVote ledger of
        Just vote -> return vote
        Nothing -> return Assent
    else do
      modifyTVar vLedger $ \oldLedger -> oldLedger {nextExpectedBallotNumber = preparedBallotNumber}
      return Dissent {
        dissentInstanceId = paxosInstanceId p,
        dissentBallotNumber = ballotNumber
      }

onPropose :: (Decreeable d) => Member d -> Proposal d -> IO (Vote d)
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

onAccept :: (Decreeable d) => Member d -> Decree d -> IO d
onAccept _ d = return $ decreeable d

---
--- Ledger functions
---

incrementNextProposedBallotNumber :: Member d -> STM BallotNumber
incrementNextProposedBallotNumber m = do
  let vLedger = paxosLedger m
  modifyTVar vLedger $ \ledger ->
    let BallotNumber lastProposed = lastProposedBallotNumber ledger
        BallotNumber nextExpected = nextExpectedBallotNumber ledger
    in ledger {
      lastProposedBallotNumber = BallotNumber $ 1 + max lastProposed nextExpected
      }
  nextProposedBallotNumber m

nextProposedBallotNumber :: Member d -> STM BallotNumber
nextProposedBallotNumber m = do
  ledger <- readTVar $ paxosLedger m
  return $ lastProposedBallotNumber ledger

setLastVote :: Member d -> Vote d-> STM ()
setLastVote p vote = do
  let vLedger = paxosLedger p
  modifyTVar vLedger $ \ledger -> ledger { lastVote = Just vote}

setNextExpectedBallotNumber :: Member d -> BallotNumber -> STM ()
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
isMajority :: Member d -> M.Map Name (Maybe v) -> (v -> Bool)-> Bool
isMajority p votes test =
  let actualVotes = filter isJust $ M.elems votes
      countedVotes = filter (\(Just v) -> test v) actualVotes
  in (toInteger . length) countedVotes >= (toInteger . S.size $ paxosMembers p) `quot` 2

maxBallotNumber :: Member d -> Votes d -> STM BallotNumber
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

{-|
Invoke a method on members of the Paxos instance. Because of the semantics of `gcallWithTimeout`, there
will be a response for every `Member`, even if it's just `Nothing`.
-}
pcall :: (Decreeable d,Serialize a,Serialize r) => Member d -> String -> a -> IO (M.Map Name (Maybe r))
pcall p method args = do
  let cs = newCallSite (paxosEndpoint p) (paxosName p)
      members = S.elems $ paxosMembers p
  gcallWithTimeout cs members method (fromInteger $ paxosTimeout p) args
