{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Paxos.Types
-- Copyright   :  (c) Phil Hargett 2015
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Control.Consensus.Paxos.Types (

  Paxos(..),
  BallotId(..),
  Member(..),
  Vote(..),
  Prepare(..),
  Proposal(..),
  Promise(..),
  Decree

) where

-- local imports

-- external imports

import qualified Data.Map as M
import Data.Maybe
import Data.Serialize

import GHC.Generics

import Network.Endpoints

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data Paxos d = Paxos {
  -- common fields
  paxosEndpoint :: Endpoint,
  paxosName :: Name,
  paxosMembers :: M.Map Name Member,
  paxosTimeout :: Integer,
  instanceId :: Integer,
  -- leader fields
  -- | The last proposal made by this member
  lastProposalId :: Integer --,
  -- lastVote :: Maybe (Vote d)
}

data BallotId = BallotId {
  ballotNumber :: Integer,
  proposerId :: Integer
} deriving (Eq,Ord,Generic)

instance Serialize BallotId

data Member = Member {
  memberPriority :: Integer
  }

data Vote d = Dissent |
  Assent |
  Vote {
    ballotId :: BallotId,
    voteDecree :: d
    }
  deriving (Generic)

instance Eq (Vote d) where
  a == b = ballotId a == ballotId b

instance Ord (Vote d) where
  Dissent <= _ = True
  _ <= Dissent = False
  Assent <= _ = True
  _ <= Assent = False
  a <= b = ballotId a <= ballotId b

instance (Decree d) => Serialize (Vote d)

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

{-|
Eq. BeginBallot in basic protocolx
-}
data Proposal = Proposal {
  proposalInstanceId :: Integer,
  proposalId :: Integer
} deriving (Generic)

instance Serialize Proposal
