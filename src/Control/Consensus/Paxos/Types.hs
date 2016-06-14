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
  Protocol(..),
  Ledger(..),
  Members(),
  Vote(..),
  Votes,
  Petition(..),
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

import qualified Data.Map as M
import Data.Serialize
import qualified Data.Set as S

import GHC.Generics

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data Paxos d a = Paxos {
  runPaxos :: Ledger d -> IO (Ledger d, a)
}

instance Functor (Paxos d) where
  fmap fn p = Paxos $ \l -> do
    (l1,a) <- runPaxos p l
    let b = fn a
    return (l1,b)

instance Applicative (Paxos d) where
  pure a = Paxos $ \l ->
    return (l,a)
  pfn <*> pa = Paxos $ \l -> do
    (l1,a) <- runPaxos pa l
    (l2,fn) <- runPaxos pfn l1
    let b = fn a
    return (l2,b)

instance Monad (Paxos d) where
  p >>= pfn = Paxos $ \l -> do
    (l1,a) <- runPaxos p l
    let pb = pfn a
    runPaxos pb l1

data Ledger d = (Decreeable d) => Member {
  paxosInstanceId :: InstanceId,
  paxosMembers :: Members,
  paxosMemberId :: MemberId,
  -- leader fields
  -- | The last proposal made by this member
  lastProposedBallotNumber :: BallotNumber, -- ^ this is lastTried[p]
  -- member fields
  nextExpectedBallotNumber:: BallotNumber, -- ^ this is nextBal[q]
  lastVote :: Maybe (Vote d), -- ^ this is prevVote[q]
  acceptedDecree :: Maybe (Decree d)
}

type Members = S.Set MemberId

data Protocol d = (Decreeable d) => Protocol {
  prepare :: Ledger d -> Prepare -> IO (Votes d),
  propose :: Ledger d -> Proposal d-> IO (Votes d),
  accept :: Ledger d -> Decree d -> IO (M.Map MemberId (Maybe ()))
}

data Petition d = (Decreeable d) => Petition {
  petitionClientId :: ClientId,
  petitionSequenceNumber :: Integer,
  petitionDecree :: d
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

newtype ClientId = ClientId Integer deriving (Eq, Ord, Show, Generic)

newtype BallotNumber = BallotNumber Integer deriving (Eq, Ord, Show, Generic)

instance Serialize BallotNumber
