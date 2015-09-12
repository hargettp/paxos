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

  module Control.Consensus.Paxos.Types

) where

-- local imports
import Control.Consensus.Paxos.Types

-- external imports

import qualified Data.Map as M
import Data.Maybe
import Data.Serialize

import GHC.Generics

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
leadBasicPaxos :: (Decree d) => Paxos d -> d -> IO (Paxos d, Maybe d)
leadBasicPaxos p d = do
  (prepared,votes) <- preparation p d
  let maybeChosenDecree = chooseDecree p d votes
  case maybeChosenDecree of
    Nothing -> return (prepared,Nothing)
    Just chosenDecree -> do
      (proposed, promised) <- proposition prepared chosenDecree
      if promised
        then do
          accepted <- acceptance proposed chosenDecree
          return (accepted, Just chosenDecree)
        else return (proposed,Nothing)

preparation :: (Decree d) => Paxos d -> d -> IO (Paxos d,M.Map Name (Maybe (Vote d)))
preparation p d = do
  let nextProposalId = 1 + lastProposalId p
      prep = Prepare {
        prepareInstanceId = instanceId p,
        tentativeProposalId = nextProposalId
        }
      paxos = p {
        lastProposalId = nextProposalId
        }
  votes <- prepare p prep
  return (p,votes)

chooseDecree :: (Decree d) => Paxos d -> d -> M.Map Name (Maybe (Vote d)) -> Maybe d
chooseDecree p decree votes =
  if isMajority p votes
    -- we didn't hear from a majority of members--we have no common decree
    then Nothing
    -- we did hear from the majority
    else case maximum votes of
      Nothing -> Just decree
      Just Assent -> Just decree
      Just vote -> Just $ voteDecree vote

proposition :: (Decree d) => Paxos d -> d -> IO (Paxos d, Bool)
proposition p _ = return (p,True)

acceptance :: (Decree d) => Paxos d -> d -> IO (Paxos d)
acceptance p _ = return p

--
-- Actual protocol
--

prepare :: (Decree d) => Paxos d -> Prepare -> IO (M.Map Name (Maybe (Vote d)))
prepare p = pcall p "prepare"

propose :: (Decree d) => Paxos d -> Proposal d -> IO (M.Map Name (Maybe Bool))
propose p = pcall p "propose"

--
-- Utility
--

{-|
Return true if there are enough votes required for a majority quorum, based on number of members
-}
isMajority :: Paxos d -> M.Map Name (Maybe v) -> Bool
isMajority p votes =
  let actualVotes = filter isJust $ M.elems votes
  in (toInteger . length) actualVotes >= (toInteger . M.size $ paxosMembers p) `quot` 2

{-|
Invoke a method on members of the Paxos instance, and fill in responses to ensure completeness
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
