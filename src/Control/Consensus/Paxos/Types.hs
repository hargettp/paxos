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

  Member(..),
  Ledger(..),
  Vote(..),
  Votes,
  Proposer(..),
  Prepare(..),
  Proposal(..),
  Decree(..),
  Decreeable,
  BallotNumber(..),
  InstanceId(..),
  MemberId(..)

) where

-- local imports

-- external imports

import Control.Concurrent.STM
import qualified Data.Map as M
import Data.Serialize
import qualified Data.Set as S

import GHC.Generics

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data Member d = Member {
  paxosInstanceId :: InstanceId,
  paxosMembers :: S.Set MemberId,
  paxosMemberId :: MemberId,
  paxosLedger :: TVar (Ledger d),
  acceptDecree :: d -> IO Bool
}

{-|
Central state for an instance of the Paxos algorithm.
-}
data Ledger d = Ledger {
  -- leader fields
  -- | The last proposal made by this member
  lastProposedBallotNumber :: BallotNumber, -- ^ this is lastTried[p]
  -- member fields
  nextExpectedBallotNumber:: BallotNumber, -- ^ this is nextBal[q]
  lastVote :: Maybe (Vote d) -- ^ this is prevVote[q]
}

data Proposer d = (Decreeable d) => Proposer {
  prepare :: Member d -> Prepare -> IO (Votes d),
  propose :: Member d -> Proposal d-> IO (Votes d),
  accept :: Member d -> Decree d -> IO (M.Map MemberId (Maybe Bool))
}

{-|
Eq. to NextBallot in basic protocol
-}
data Prepare = Prepare {
  prepareInstanceId :: InstanceId,
  tentativeBallotNumber :: BallotNumber
} deriving (Generic)

instance Serialize Prepare

data Vote d = Dissent {
    dissentInstanceId :: InstanceId,
    dissentBallotNumber :: BallotNumber
  } |
  Assent |
  Vote {
    voteInstanceId :: InstanceId,
    voteBallotNumber :: BallotNumber,
    voteDecree :: Decree d
    }
  deriving (Generic)

instance Eq (Vote d) where
  a == b = voteBallotNumber a == voteBallotNumber b

instance Ord (Vote d) where
  Dissent _ _ <= _ = True
  _ <= Dissent _ _ = False
  Assent <= _ = True
  _ <= Assent = False
  a <= b = voteBallotNumber a <= voteBallotNumber b

type Votes d = M.Map MemberId (Maybe (Vote d))

instance (Decreeable d) => Serialize (Vote d)

class (Generic d, Serialize d) => Decreeable d

data Decree d = (Decreeable d) => Decree {
  decreeInstanceId :: InstanceId,
  -- the member from which this decree originated
  decreeMemberId :: MemberId,
  decreeable :: d
  }

instance (Decreeable d) => Serialize (Decree d) where
  put d = do
    put $ decreeInstanceId d
    put $ decreeMemberId d
    put $ decreeable d
  get = do
    instanceId <- get
    memberId <- get
    decree <- get
    return Decree {
      decreeInstanceId = instanceId,
      decreeMemberId = memberId,
      decreeable = decree
      }

{-|
Eq. BeginBallot in basic protocol
-}
data Proposal d =  Proposal {
  proposalInstanceId :: InstanceId,
  proposedBallotNumber :: BallotNumber,
  proposedDecree :: Decree d
} deriving Generic

instance (Decreeable d) => Serialize (Proposal d)

newtype InstanceId = InstanceId Integer deriving (Eq, Ord, Show, Generic)

instance Serialize InstanceId

newtype MemberId = MemberId Integer deriving (Eq, Ord, Show, Generic)

instance Serialize MemberId

newtype BallotNumber = BallotNumber Integer deriving (Eq, Ord, Show, Generic)

instance Serialize BallotNumber
