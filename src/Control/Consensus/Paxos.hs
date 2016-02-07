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
leadBasicPaxosInstance :: (Decree d) => Paxos d -> d -> IO (Paxos d, Maybe d)
leadBasicPaxosInstance p d = do
  let newP = p {
    lastProposalId = 1 + lastProposalId p
  }
  maybeDecree <- leadBasicPaxosRound newP d
  case maybeDecree of
    -- if this decree is accepted, we are done
    Just c | c == d -> return (newP,Just d)
    -- if the response is Nothing or another decree, keep trying
    _ -> leadBasicPaxosInstance newP d
    
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
  let prep = Prepare {
        prepareInstanceId = instanceId p,
        tentativeProposalId = lastProposalId p
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
  let proposal = Proposal {
    proposalInstanceId = instanceId p,
    proposalId = lastProposalId p
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
