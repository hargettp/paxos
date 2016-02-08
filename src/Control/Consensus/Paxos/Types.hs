{-# LANGUAGE DeriveGeneric #-}

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
  Ledger(..),
  BallotId(..),
  Member(..),
  Vote(..),
  Votes(..),
  Prepare(..),
  Proposal(..),
  Promise(..),
  Decree

) where

-- local imports

-- external imports

import Control.Concurrent.STM
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
  paxosMemberId :: Integer,
  paxosMembers :: M.Map Name Member,
  paxosTimeout :: Integer,
  instanceId :: Integer,
  paxosLedger :: TVar (Ledger d)
}

data Ledger d = Ledger {
  -- leader fields
  -- | The last proposal made by this member
  lastProposedBallotNumber :: Integer, -- this is lastTried[p]
  -- member fields
  nextBallotNumber:: Integer, -- this is nextBal[q]
  lastVote :: Maybe (Vote d) -- this is prevVote[q]
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

type Votes d = M.Map Name (Maybe (Vote d))

instance (Decree d) => Serialize (Vote d)

{-|
Eq. to NextBallot in basic protocol
-}
data Prepare = Prepare {
  prepareInstanceId :: Integer,
  tentativeBallotId :: BallotId
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
  proposedBallotId :: BallotId
} deriving (Generic)

instance Serialize Proposal
