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

  -- leadBasicPaxosInstance,
  leadBasicPaxosBallot,

  onPrepare,
  onPropose,
  onAccept,

  get,
  set,
  io,
  mkMemberId,

  module Control.Consensus.Paxos.Types

) where

-- local imports
import Control.Consensus.Paxos.Types

-- external imports

import qualified Data.Map as M
import Data.Maybe
import qualified Data.Set as S

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
  get >>= \l -> do
    let maybeChosenDecree = chooseDecree (paxosMembers l) d earlierVotes
    case maybeChosenDecree of
      Nothing -> return Nothing
      Just chosenDecree -> do
        promised <- proposition p d
        if promised
          then acceptance p chosenDecree
          else return Nothing

preparation :: (Decreeable d) => Protocol d -> Paxos d (Votes d)
preparation proposer = do
  b <- incrementNextProposedBallotNumber
  votes <- get >>= \l -> do
    let prep = Prepare {
          prepareInstanceId = paxosInstanceId l,
          tentativeBallotNumber = b
          }
    io $ prepare proposer l prep
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
  proposed <- getNextProposedBallotNumber
  votes <- get >>= \l -> do
    let proposal = Proposal {
          proposalInstanceId = paxosInstanceId l,
          proposedBallotNumber = proposed,
          proposedDecree = d
        }
    io $ propose proposer l proposal
  maxBallotNumber votes >>= setNextExpectedBallotNumber
  get >>= \l -> do
    let success = isMajority (paxosMembers l) votes $ \vote ->
          case vote of
            Vote {} ->
              (voteBallotNumber vote == proposed) &&
               (voteInstanceId vote == paxosInstanceId l)
            Assent -> True
            Dissent {} -> False
    return success

acceptance :: (Decreeable d) => Protocol d -> Decree d -> Paxos d (Maybe (Decree d))
acceptance p d = do
  responses <- get >>= \l -> io $ accept p l d
  get >>= \l ->
    if isMajority (paxosMembers l) responses $ const True
      then return $ Just d
      else return Nothing

--
-- Actual protocol
--

-- callee

onPrepare :: (Decreeable d) => Prepare -> Paxos d (Vote d)
onPrepare prep =
  get >>= \l -> do
    let expectedBallotNumber = nextExpectedBallotNumber l
        preparedBallotNumber = tentativeBallotNumber prep
    if preparedBallotNumber > expectedBallotNumber
      then
        case lastVote l of
          Just vote -> do
            setNextExpectedBallotNumber preparedBallotNumber
            return vote
          Nothing -> return Assent
      else
        return Dissent {
          dissentInstanceId = paxosInstanceId l,
          dissentBallotNumber = expectedBallotNumber
        }

onPropose :: (Decreeable d) => Proposal d -> Paxos d (Vote d)
onPropose prop = do
  ballotNumber <-getNextProposedBallotNumber
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

onAccept :: (Decreeable d) => Decree d -> Paxos d ()
onAccept d =
  modify $ \l ->
    l { acceptedDecree = Just d}

---
--- Ledger functions
---

incrementNextProposedBallotNumber :: Paxos d BallotNumber
incrementNextProposedBallotNumber = do
  l <- get
  let BallotNumber lastProposed = lastProposedBallotNumber l
      BallotNumber nextExpected = nextExpectedBallotNumber l
      newBallotNumber = BallotNumber $ 1 + max lastProposed nextExpected
  set l {
      lastProposedBallotNumber = newBallotNumber
      }
  return newBallotNumber

getNextProposedBallotNumber :: Paxos d BallotNumber
getNextProposedBallotNumber = do
  l <- get
  return $ lastProposedBallotNumber l

setLastVote :: Vote d -> Paxos d ()
setLastVote vote =
  modify $ \l ->
    l {
      lastVote = Just vote
      }

setNextExpectedBallotNumber :: BallotNumber -> Paxos d ()
setNextExpectedBallotNumber nextBallotNumber =
  modify $ \l ->
    let nextExpected = nextExpectedBallotNumber l
    in l {
      nextExpectedBallotNumber = max nextExpected nextBallotNumber
      }

-- Doing the state monad thing here explicitly, because no interest in pulling in mtl
set :: Ledger d -> Paxos d ()
set l = Paxos $ \_ -> return (l,())

get :: Paxos d (Ledger d)
get = Paxos $ \l -> return (l,l)

modify :: (Ledger d -> Ledger d) -> Paxos d ()
modify fn = do
  l <- get
  set $ fn l

io :: IO a -> Paxos d a
io fn = Paxos $ \l -> do
  a <- fn
  return (l,a)

--
-- Factories
--

mkMemberId :: IO MemberId
mkMemberId = fmap MemberId R.randomIO

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

maxBallotNumber :: Votes d -> Paxos d BallotNumber
maxBallotNumber votes = Paxos $ \l -> do
  let maxVote = maximum votes
      ballotNumber = nextExpectedBallotNumber l
  return (l, case maxVote of
    Just vote -> case vote of
      Dissent{} -> max (dissentBallotNumber vote) ballotNumber
      Assent -> ballotNumber
      Vote{} -> max (voteBallotNumber vote) ballotNumber
    Nothing -> ballotNumber)
