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

{-|
Eq. to NextBallot in basic protocol
-}
data Prepare = Prepare {
  prepareInstanceId :: Integer,
  tentativeBallotNumber :: Integer
} deriving (Generic)

instance Serialize Prepare

data Vote d = Dissent |
  Assent |
  Vote {
    voteBallotNumber :: Integer,
    voteDecree :: d
    }
  deriving (Generic)

instance Eq (Vote d) where
  a == b = voteBallotNumber a == voteBallotNumber b

instance Ord (Vote d) where
  Dissent <= _ = True
  _ <= Dissent = False
  Assent <= _ = True
  _ <= Assent = False
  a <= b = voteBallotNumber a <= voteBallotNumber b

type Votes d = M.Map Name (Maybe (Vote d))

instance (Decree d) => Serialize (Vote d)

class (Eq d,Serialize d) => Decree d

{-|
Eq. BeginBallot in basic protocolx
-}
data Proposal d =  Proposal {
  proposalInstanceId :: Integer,
  proposedBallotNumber :: Integer,
  proposedDecree :: d
} deriving Generic

data Promise d = Promise {
  } |
  Decline {
    declineInstanceId :: Integer,
    declineProposalId :: Integer
  }

instance (Decree d) => Serialize (Proposal d)
