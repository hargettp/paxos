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
leadBasicPaxosInstance :: (Decree d) => Paxos d -> d -> IO (Maybe d)
leadBasicPaxosInstance p d = do
  maybeDecree <- leadBasicPaxosRound p d
  case maybeDecree of
    -- if this decree is accepted, we are done
    Just c | c == d -> return $ Just d
    -- if the response is Nothing or another decree, keep trying
    _ -> leadBasicPaxosInstance p d

{-|
Lead one round of voting, with one of 3 possible outcomes:

* The proposed decree is accepted
* Another decree proposed by another `Member` is accepted
* No decree is accepted

-}
leadBasicPaxosRound :: (Decree d) => Paxos d -> d -> IO (Maybe d)
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

preparation :: (Decree d) => Paxos d -> IO (Votes d)
preparation p = do
  proposedBallotNumber <- atomically $ incrementNextProposedBallotNumber p
  let prep = Prepare {
        prepareInstanceId = instanceId p,
        tentativeBallotNumber = proposedBallotNumber
        }
  prepare p prep

chooseDecree :: (Decree d) => Paxos d -> d -> Votes d -> Maybe d
chooseDecree p decree votes =
  if isMajority p votes (/= Dissent)
    -- we didn't hear from a majority of members--we have no common decree
    then Nothing
    -- we did hear from the majority
    else case maximum votes of
      Nothing -> Just decree
      Just Assent -> Just decree
      Just vote -> Just $ voteDecree vote

proposition :: (Decree d) => Paxos d -> d -> IO Bool
proposition p d = do
  proposedBallotNumber <- atomically $ nextProposedBallotNumber p
  let proposal = Proposal {
    proposalInstanceId = instanceId p,
    proposedBallotNumber = proposedBallotNumber,
    proposedDecree = d
  }
  responses <- propose p proposal
  return $ isMajority p responses $ \promise ->
    case promise of
      Promise _ _ ->
        (promiseBallotNumber promise == proposedBallotNumber) &&
         (promiseInstanceId promise == paxosMemberId p)
      Decline _ _ -> False

acceptance :: (Decree d) => Paxos d -> d -> IO (Maybe d)
acceptance p d = do
  responses <- accept p d
  if isMajority p responses id
    then return $ Just d
    else return Nothing

--
-- Actual protocol
--

-- caller

prepare :: (Decree d) => Paxos d -> Prepare -> IO (Votes d)
prepare p = pcall p "prepare"

propose :: (Decree d) => Paxos d -> Proposal d-> IO (M.Map Name (Maybe Promise))
propose p = pcall p "propose"

accept :: (Decree d) => Paxos d -> d -> IO (M.Map Name (Maybe Bool))
accept p = pcall p "accept"

-- callee

onPrepare :: (Decree d) => Paxos d -> Prepare -> IO (Vote d)
onPrepare p prep = atomically $ do
  let preparedBallotNumber = tentativeBallotNumber prep
      vLedger = paxosLedger p
  ledger <- readTVar vLedger
  if preparedBallotNumber > nextBallotNumber ledger
    then do
      modifyTVar vLedger $ \ledger -> ledger {nextBallotNumber = preparedBallotNumber}
      case lastVote ledger of
        Just vote -> return vote
        Nothing -> return Assent
    else
      return Dissent

onPropose :: (Decree d) => Paxos d -> Proposal d -> IO Promise
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
      return Promise {
          promiseInstanceId = proposalInstanceId prop,
          promiseBallotNumber = proposedBallotNumber prop
        }
    else
      return Decline {
          declineInstanceId = proposalInstanceId prop,
          declineBallotNumber = ballotNumber
        }

onAccept :: (Decree d) => Paxos d -> d -> IO d
onAccept p = return

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

{-|
Invoke a method on members of the Paxos instance. Because of the semantics of `gcallWithTimeout`, there
will be a response for every `Member`, even if it's just `Nothing`.
-}
pcall :: (Decree d,Serialize a,Serialize r) => Paxos d -> String -> a -> IO (M.Map Name (Maybe r))
pcall p method args = do
  let cs = newCallSite (paxosEndpoint p) (paxosName p)
      members = M.keys $ paxosMembers p
  gcallWithTimeout cs members method (fromInteger $ paxosTimeout p) args
