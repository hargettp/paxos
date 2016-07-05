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

  leadBasicPaxosBallot,
  followBasicPaxosBallot,

  paxos,

  onPrepare,
  onPropose,
  onAccept,

  get,
  io,
  safely,

  mkMemberId,
  mkTLedger,

  isMajority,
  chooseDecree,

  module Control.Consensus.Paxos.Types

) where

-- local imports
import Control.Consensus.Paxos.Types

-- external imports

import Control.Concurrent.STM

import qualified Data.Map as M
import Data.Maybe
import qualified Data.Set as S

import Debug.Trace

import qualified System.Random as R

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
Lead one ballot of voting, with one of 3 possible outcomes:

* The proposed decree is accepted
* Another decree proposed by another `Member` is accepted
* No decree is accepted
-}
leadBasicPaxosBallot :: (Decreeable d) => Protocol d -> Decree d -> Paxos d (Maybe (Decree d))
leadBasicPaxosBallot p d = do
  earlierVotes <- preparation p
  maybeChosenDecree <- safely $ do
    ledger <- get
    return $ chooseDecree (paxosMembers ledger) d earlierVotes
  case maybeChosenDecree of
    Nothing -> return Nothing
    Just chosenDecree -> do
      promised <- proposition p d
      if promised
        then acceptance p chosenDecree
        else return Nothing

followBasicPaxosBallot :: (Decreeable d) => Protocol d -> Paxos d (Maybe (Decree d))
followBasicPaxosBallot p =
  expectPrepare p onPrepare >>= \prepared -> if prepared
    then expectPropose p onPropose >>= \proposed -> if proposed
      then expectAccept p onAccept
      else return Nothing
    else return Nothing


paxos :: TLedger d -> Paxos d a -> IO a
paxos = flip runPaxos

preparation :: (Decreeable d) => Protocol d -> Paxos d (Votes d)
preparation proposer = do
  (members,prep) <- safely $ do
    b <- incrementNextProposedBallotNumber
    ledger <- get
    let prep = Prepare {
            prepareInstanceId = paxosInstanceId ledger,
            tentativeBallotNumber = b
            }
        members = paxosMembers ledger
    return (members,prep)
  votes <- prepare proposer members prep
  io $ traceIO $ "Votes are " ++ show votes
  safely $
    maxBallotNumber votes >>= setNextExpectedBallotNumber
  return votes

chooseDecree :: (Decreeable d) => Members -> Decree d -> Votes d -> Maybe (Decree d)
chooseDecree members decree votes =
  if isMajority members votes $ \vote ->
    case vote of
      Dissent{} -> False
      _ -> True
    -- we didn't hear from a majority of members--we have no common decree
    then Nothing
    -- we did hear from the majority
    else case maximum votes of
      -- there was no other preferred decree, so use ours
      Nothing -> Just decree
      -- there is agreement on ours
      Just Assent -> Just decree
      -- pick the latest one from earlier
      Just vote -> Just $ voteDecree vote

proposition :: (Decreeable d) => Protocol d -> Decree d -> Paxos d Bool
proposition proposer d = do
  io $ traceIO $ "Proposing " ++ show d
  (members,proposal,proposed) <- safely $ do
    proposed <- getNextProposedBallotNumber
    ledger <- get
    let proposal = Proposal {
          proposalInstanceId = paxosInstanceId ledger,
          proposedBallotNumber = proposed,
          proposedDecree = d
          }
        members = paxosMembers ledger
    return (members,proposal,proposed)
  votes <- propose proposer members proposal
  safely $ do
    maxBallotNumber votes >>= setNextExpectedBallotNumber
    ledger <- get
    let success = isMajority (paxosMembers ledger) votes $ \vote ->
          case vote of
            Vote {} ->
              (voteBallotNumber vote == proposed) &&
               (voteInstanceId vote == paxosInstanceId ledger)
            Assent -> True
            Dissent {} -> False
    return success

acceptance :: (Decreeable d) => Protocol d -> Decree d -> Paxos d (Maybe (Decree d))
acceptance p d = do
  members <- safely $ do
    ledger <- get
    return $ paxosMembers ledger
  responses <- accept p members d
  safely $ do
    ledger <- get
    if isMajority (paxosMembers ledger) responses $ const True
      then return $ Just d
      else return Nothing

--
-- Actual protocol
--

-- callee

onPrepare :: (Decreeable d) => Prepare -> Paxos d (Vote d)
onPrepare prep = safely $
  get >>= \ledger -> do
    let expectedBallotNumber = nextExpectedBallotNumber ledger
        preparedBallotNumber = tentativeBallotNumber prep
    if preparedBallotNumber > expectedBallotNumber
      then
        case lastVote ledger of
          Just vote -> do
            setNextExpectedBallotNumber preparedBallotNumber
            return vote
          Nothing -> return Assent
      else
        return Dissent {
          dissentInstanceId = paxosInstanceId ledger,
          dissentBallotNumber = expectedBallotNumber
        }

onPropose :: (Decreeable d) => Proposal d -> Paxos d (Vote d)
onPropose prop = safely $ do
  ballotNumber <- getNextExpectedBallotNumber
  if ballotNumber == proposedBallotNumber prop
    then do
      let instanceId = proposalInstanceId prop
          decree = proposedDecree prop
          vote = Vote {
            voteInstanceId = instanceId,
            voteBallotNumber = ballotNumber,
            voteDecree = decree
          }
      setLastVote vote
      return vote
    else
      return Dissent {
          dissentInstanceId = proposalInstanceId prop,
          dissentBallotNumber = ballotNumber
        }

onAccept :: (Decreeable d) => Decree d -> Paxos d (Decree d)
onAccept d = do
  safely $ modify $ \ledger ->
    ledger { acceptedDecree = Just d}
  return d

---
--- Ledger functions
---

incrementNextProposedBallotNumber :: PaxosSTM d BallotNumber
incrementNextProposedBallotNumber = PaxosSTM $ \vLedger -> do
  ledger <- readTVar vLedger
  let BallotNumber lastProposed = lastProposedBallotNumber ledger
      BallotNumber nextExpected = nextExpectedBallotNumber ledger
      newBallotNumber = BallotNumber $ 1 + max lastProposed nextExpected
  writeTVar vLedger ledger {
      lastProposedBallotNumber = newBallotNumber
      }
  return newBallotNumber

getNextProposedBallotNumber :: PaxosSTM d BallotNumber
getNextProposedBallotNumber = PaxosSTM $ \vLedger -> do
  ledger <- readTVar vLedger
  return $ lastProposedBallotNumber ledger

getNextExpectedBallotNumber :: PaxosSTM d BallotNumber
getNextExpectedBallotNumber = PaxosSTM $ \vLedger -> do
  ledger <- readTVar vLedger
  return $ nextExpectedBallotNumber ledger

setLastVote :: Vote d -> PaxosSTM d ()
setLastVote vote =
  modify $ \ledger ->
    ledger {
      lastVote = Just vote
      }

setNextExpectedBallotNumber :: BallotNumber -> PaxosSTM d ()
setNextExpectedBallotNumber nextBallotNumber =
  modify $ \ledger ->
    let nextExpected = nextExpectedBallotNumber ledger
    in ledger {
      nextExpectedBallotNumber = max nextExpected nextBallotNumber
      }

get :: PaxosSTM d (Ledger d)
get = PaxosSTM readTVar

modify :: (Ledger d -> Ledger d) -> PaxosSTM d ()
modify fn = PaxosSTM $ \vLedger -> modifyTVar vLedger fn

io :: IO a -> Paxos d a
io fn = Paxos $ const fn

safely :: PaxosSTM d a -> Paxos d a
safely p = Paxos $ \vLedger ->
  atomically $ runPaxosSTM p vLedger

--
-- Factories
--

mkMemberId :: IO MemberId
mkMemberId = fmap MemberId R.randomIO

mkTLedger :: (Decreeable d) => InstanceId -> Members -> MemberId -> IO (TLedger d)
mkTLedger instanceId members me = do
  let ledger = Ledger {
    paxosInstanceId = instanceId,
    paxosMembers = members,
    paxosMemberId = me,
    lastProposedBallotNumber = BallotNumber 0,
    nextExpectedBallotNumber = BallotNumber 0,
    lastVote = Nothing,
    acceptedDecree = Nothing
  }
  atomically $ newTVar ledger

--
-- Utility
--

{-|
Return true if there are enough responses that are not Nothing and which pass
the supplied test.
-}
isMajority :: Members -> M.Map MemberId (Maybe v) -> (v -> Bool)-> Bool
isMajority members votes test =
  let actualVotes = filter isJust $ M.elems votes
      countedVotes = filter (\(Just v) -> test v) actualVotes
  in (toInteger . length) countedVotes >= (toInteger . S.size $ members) `quot` 2

maxBallotNumber :: Votes d -> PaxosSTM d BallotNumber
maxBallotNumber votes = do
  ledger <- get
  let maxVote = maximum votes
      ballotNumber = nextExpectedBallotNumber ledger
  return $ case maxVote of
    Just vote -> case vote of
      Dissent{} -> max (dissentBallotNumber vote) ballotNumber
      Assent -> ballotNumber
      Vote{} -> max (voteBallotNumber vote) ballotNumber
    Nothing -> ballotNumber
