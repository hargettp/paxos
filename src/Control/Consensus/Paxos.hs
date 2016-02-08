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
  atomically $ incNextProposedBallotNumber p
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
      promised <- proposition p
      if promised
        then acceptance p chosenDecree
        else return Nothing

preparation :: (Decree d) => Paxos d -> IO (Votes d)
preparation p = do
  proposedBallotNumber <- atomically $ nextProposedBallotNumber p
  let prep = Prepare {
        prepareInstanceId = instanceId p,
        tentativeProposalId = proposedBallotNumber
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

proposition :: (Decree d) => Paxos d -> IO Bool
proposition p = do
  proposedBallotNumber <- atomically $ nextProposedBallotNumber p
  let proposal = Proposal {
    proposalInstanceId = instanceId p,
    proposedBallotNumber = proposedBallotNumber
  }
  responses <- propose p proposal
  return $ isMajority p responses id

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

{-# ANN propose "HLint: ignore Eta reduce" #-}
propose :: (Decree d) => Paxos d -> Proposal -> IO (M.Map Name (Maybe Bool))
propose p proposal = pcall p "propose" proposal

{-# ANN accept "HLint: ignore Eta reduce" #-}
accept :: (Decree d) => Paxos d -> d -> IO (M.Map Name (Maybe Bool))
accept p d = pcall p "accept" d

-- callee

onPrepare :: (Decree d) => Paxos d -> Prepare -> IO (Vote d)
onPrepare p prep = return Assent

onPropose :: (Decree d) => Paxos d -> Proposal -> IO Bool
onPropose _ _ = return True

onAccept :: (Decree d) => Paxos d -> d -> IO Bool
onAccept _ _ = return True

---
--- Ledger functions
---

incNextProposedBallotNumber :: Paxos d -> STM Integer
incNextProposedBallotNumber p = do
  let vLedger = paxosLedger p
  modifyTVar vLedger $ \ledger -> ledger {lastProposedBallotNumber = lastProposedBallotNumber ledger + 1}
  ledger <- readTVar vLedger
  return $ lastProposedBallotNumber ledger

nextProposedBallotNumber :: Paxos d -> STM Integer
nextProposedBallotNumber p = do
  ledger <- readTVar $ paxosLedger p
  return $ lastProposedBallotNumber ledger

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
