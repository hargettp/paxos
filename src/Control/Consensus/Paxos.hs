{-# LANGUAGE ExistentialQuantification #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus
-- Copyright   :  (c) Phil Hargett 2015
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  experimental
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Control.Consensus.Paxos (

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
import Network.RPC

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
Lead one round of voting, with one of 3 possible outcomes:

* The proposed decree
* Another decree proposed by another Member
* No decree chosen

-}
leadBasicPaxosRound :: (Decree d) => Paxos d -> d -> IO (Paxos d, Maybe d)
leadBasicPaxosRound p d = do
  (prepared,votes) <- preparation p
  let maybeChosenDecree = chooseDecree prepared d votes
  case maybeChosenDecree of
    Nothing -> return (prepared,Nothing)
    Just chosenDecree -> do
      (proposed, promised) <- proposition prepared
      if promised
        then acceptance proposed chosenDecree
        else return (proposed,Nothing)

preparation :: (Decree d) => Paxos d -> IO (Paxos d,M.Map Name (Maybe (Vote d)))
preparation p = do
  let nextProposalId = 1 + lastProposalId p
      prep = Prepare {
        prepareInstanceId = instanceId p,
        tentativeProposalId = nextProposalId
        }
      prepared = p {
        lastProposalId = nextProposalId
        }
  votes <- prepare prepared prep
  return (p,votes)

chooseDecree :: (Decree d) => Paxos d -> d -> M.Map Name (Maybe (Vote d)) -> Maybe d
chooseDecree p decree votes =
  if isMajority p votes (/= Dissent)
    -- we didn't hear from a majority of members--we have no common decree
    then Nothing
    -- we did hear from the majority
    else case maximum votes of
      Nothing -> Just decree
      Just Assent -> Just decree
      Just vote -> Just $ voteDecree vote

proposition :: (Decree d) => Paxos d -> IO (Paxos d, Bool)
proposition p = do
  let proposal = Proposal {
    proposalInstanceId = instanceId p,
    proposalId = lastProposalId p
  }
  responses <- propose p proposal
  return (p,isMajority p responses id)

acceptance :: (Decree d) => Paxos d -> d -> IO (Paxos d, Maybe d)
acceptance p d = do
  responses <- accept p d
  if isMajority p responses id
    then return (p,Just d)
    else return (p,Nothing)

--
-- Actual protocol
--

prepare :: (Decree d) => Paxos d -> Prepare -> IO (M.Map Name (Maybe (Vote d)))
prepare p = pcall p "prepare"

{-# ANN propose "HLint: ignore Eta reduce" #-}
propose :: (Decree d) => Paxos d -> Proposal -> IO (M.Map Name (Maybe Bool))
propose p proposal = pcall p "propose" proposal

{-# ANN accept "HLint: ignore Eta reduce" #-}
accept :: (Decree d) => Paxos d -> d -> IO (M.Map Name (Maybe Bool))
accept p d = pcall p "accept" d

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
will be a response for every `Member`.
-}
pcall :: (Decree d,Serialize a,Serialize r) => Paxos d -> String -> a -> IO (M.Map Name (Maybe r))
pcall p method args = do
  let cs = newCallSite (paxosEndpoint p) (paxosName p)
      members = M.keys $ paxosMembers p
  responses <- gcallWithTimeout cs members method (fromInteger $ paxosTimeout p) (encode args)
  return $ decodeResponses responses

decodeResponses :: (Serialize r) => M.Map Name (Maybe Message)  -> M.Map Name (Maybe r)
decodeResponses = M.map decodeResponse
  where
    decodeResponse maybeMsg = case maybeMsg of
      Nothing -> Nothing
      Just msg -> case decode msg of
        Left _ -> Nothing
        Right response -> Just response
