{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE StandaloneDeriving #-}

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

module Control.Consensus (

) where

-- local imports

-- external imports

import qualified Data.Map as M
import Data.Maybe
import Data.Serialize

import GHC.Generics

import Network.Endpoints
import Network.RPC

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data Paxos d = Paxos {
  paxosEndpoint :: Endpoint,
  paxosName :: Name,
  paxosMembers :: M.Map Name Member,
  paxosTimeout :: Integer,
  instanceId :: Integer,
  lastProposalId :: Integer,
  nextProposalId :: Integer,
  lastVote :: Maybe (Vote d)
}

data BallotId = BallotId {
  ballotNumber :: Integer,
  proposerId :: Integer
} deriving (Eq,Ord,Generic)

instance Serialize BallotId

data Member = Member {
  memberPriority :: Integer
  }

data Vote d = Vote {
  ballotId :: BallotId,
  voteDecree :: d
  }
  | Assent
  deriving (Generic,Eq)

instance (Decree d) => Serialize (Vote d)

instance (Decree d) => Ord (Vote d) where
  Assent <= Assent = True
  Assent <= _ = True
  _ <= Assent = False
  a <= b = (ballotNumber $ ballotId a) <= (ballotNumber $ ballotId b)

{-|
Eq. to NextBallot in basic protocol
-}
data Prepare = Prepare {
  prepareInstanceId :: Integer,
  tentativeProposalId :: Integer
} deriving (Generic)

instance Serialize Prepare

class (Eq d,Serialize d) => Decree d

data Promise d = Promise {
  } |
  Decline {
    declineInstanceId :: Integer,
    declineProposalId :: Integer
  }

newPrepare :: Paxos d -> (Paxos d, Prepare)
newPrepare p =
  let nextProposalId = 1 + lastProposalId p
      prepare = Prepare {
        prepareInstanceId = instanceId p,
        tentativeProposalId = nextProposalId
        }
      paxos = p {
        lastProposalId = nextProposalId
        }
  in (paxos,prepare)

{-|
Eq. BeginBallot in basic protocolx
-}
data Proposal d = (Decree d) => Proposal {
  proposalInstanceId :: Integer,
  proposalId :: Integer,
  proposedDecree :: d
}

instance (Decree d) => Serialize (Proposal d) where
  put p = do
    put $ proposalInstanceId p
    put $ proposalId p
    put $ proposedDecree p

  get = do
    _proposalInstanceId <- get
    _proposalId <- get
    _proposedDecree <- get
    return Proposal {
      proposalInstanceId = _proposalInstanceId,
      proposalId = _proposalId,
      proposedDecree = _proposedDecree
    }

newProposal :: (Decree d) => Paxos d -> d -> (Paxos d, Proposal d)
newProposal p d =
  let nextProposalId = 1 + lastProposalId p
      proposal = Proposal {
        proposalInstanceId = instanceId p,
        proposalId = nextProposalId,
        proposedDecree = d
      }
      paxos = p {
        lastProposalId = nextProposalId
      }
  in (paxos,proposal)

runBasicPaxos :: (Decree d) => Paxos d -> d -> IO (Paxos d, Maybe d)
runBasicPaxos p d = do
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
-- proposition p d = do
--   let nextProposalId = 1 + lastProposalId p
--       prop = Proposal {
--         proposalInstanceId = instanceId p,
--         proposalId = nextProposalId,
--         proposedDecree = d
--       }
--       paxos = p {
--         lastProposalId = nextProposalId
--       }
--   votes <- propose p prop
--   let actualVotes = filter isJust votes


acceptance :: (Decree d) => Paxos d -> d -> IO (Paxos d)
acceptance p _ = return p

--
-- Actual protocol
--

prepare :: (Decree d) => Paxos d -> Prepare -> IO (M.Map Name (Maybe (Vote d)))
prepare p prep = pcall p "prepare" prep

propose :: (Decree d) => Paxos d -> Proposal d -> IO (M.Map Name (Maybe Bool))
propose p proposal = pcall p "propose" proposal

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
decodeResponses responses = M.map decodeResponse responses
  where
    decodeResponse maybeMsg = case maybeMsg of
      Nothing -> Nothing
      Just msg -> case decode msg of
        Left _ -> Nothing
        Right response -> Just response
