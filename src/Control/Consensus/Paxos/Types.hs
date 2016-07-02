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
  PaxosSTM(..),
  Protocol(..),
  Ledger(..),
  TLedger,
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

import Control.Concurrent.STM

import qualified Data.Map as M
import Data.Serialize
import qualified Data.Set as S

import GHC.Generics

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data Paxos d a = Paxos {
  runPaxos :: TLedger d -> IO a
}

instance Functor (Paxos d) where
  fmap fn p = Paxos $ \l -> do
    a <- runPaxos p l
    let b = fn a
    return b

instance Applicative (Paxos d) where
  pure a = Paxos $ \_ ->
    return a
  pfn <*> pa = Paxos $ \l -> do
    a <- runPaxos pa l
    fn <- runPaxos pfn l
    let b = fn a
    return b

instance Monad (Paxos d) where
  p >>= pfn = Paxos $ \l -> do
    a <- runPaxos p l
    let pb = pfn a
    runPaxos pb l

data PaxosSTM d a = PaxosSTM {
  runPaxosSTM :: TLedger d -> STM a
}

instance Functor (PaxosSTM d) where
  fmap fn p = PaxosSTM $ \vl -> do
    a <- runPaxosSTM p vl
    let b = fn a
    return b

instance Applicative (PaxosSTM d) where
  pure a = PaxosSTM $ \_ ->
    return a
  pfn <*> pa = PaxosSTM $ \vl -> do
    a <- runPaxosSTM pa vl
    fn <- runPaxosSTM pfn vl
    let b = fn a
    return b

instance Monad (PaxosSTM d) where
  p >>= pfn = PaxosSTM $ \vl -> do
    a <- runPaxosSTM p vl
    let pb = pfn a
    runPaxosSTM pb vl

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

type TLedger d = TVar (Ledger d)

type Members = S.Set MemberId

data Protocol d = (Decreeable d) => Protocol {
  -- leader methods
  prepare :: Members -> Prepare -> Paxos d (Votes d),
  propose :: Members -> Proposal d-> Paxos d (Votes d),
  accept :: Members -> Decree d -> Paxos d (M.Map MemberId (Maybe ())),

  -- follower methods
  expectPrepare :: (Prepare -> Paxos d (Vote d)) -> Paxos d Bool,
  expectPropose :: (Proposal d -> Paxos d (Vote d)) -> Paxos d Bool,
  expectAccept :: (Decree d -> Paxos d (Decree d)) -> Paxos d (Maybe (Decree d))
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
