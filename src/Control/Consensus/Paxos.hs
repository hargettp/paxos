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
  l1 <- get
  let maybeChosenDecree = chooseDecree (paxosMembers l1) d earlierVotes
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
  l1 <- get
  let prep = Prepare {
        prepareInstanceId = paxosInstanceId l1,
        tentativeBallotNumber = b
        }
  votes <- io $ prepare proposer l1 prep
  ballotNumber <- maxBallotNumber votes
  setNextExpectedBallotNumber ballotNumber
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
  l1 <- get
  let proposal = Proposal {
        proposalInstanceId = paxosInstanceId l1,
        proposedBallotNumber = proposed,
        proposedDecree = d
      }
  votes <- io $ propose proposer l1 proposal
  ballotNumber <- maxBallotNumber votes
  setNextExpectedBallotNumber ballotNumber
  l2 <- get
  let success = isMajority (paxosMembers l2) votes $ \vote ->
        case vote of
          Vote {} ->
            (voteBallotNumber vote == proposed) &&
             (voteInstanceId vote == paxosInstanceId l2)
          Assent -> True
          Dissent {} -> False
  return success

acceptance :: (Decreeable d) => Protocol d -> Decree d -> Paxos d (Maybe (Decree d))
acceptance p d = do
  l1 <- get
  responses <- io $ accept p l1 d
  l2 <- get
  if isMajority (paxosMembers l2) responses $ const True
    then return $ Just d
    else return Nothing

--
-- Actual protocol
--

-- callee

onPrepare :: (Decreeable d) => Prepare -> Paxos d (Vote d)
onPrepare prep = do
  l1 <- get
  let expectedBallotNumber = nextExpectedBallotNumber l1
      preparedBallotNumber = tentativeBallotNumber prep
  if preparedBallotNumber > expectedBallotNumber
    then
      case lastVote l1 of
        Just vote -> do
          setNextExpectedBallotNumber preparedBallotNumber
          return vote
        Nothing -> return Assent
    else
      return Dissent {
        dissentInstanceId = paxosInstanceId l1,
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

-- onAccept :: (Decreeable d) => Ledger d -> Decree d -> IO (Ledger d,())
onAccept :: (Decreeable d) => Decree d -> Paxos d ()
onAccept _ = return ()

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
setLastVote vote = do
  l <- get
  set l {
    lastVote = Just vote
    }
  return ()

setNextExpectedBallotNumber :: BallotNumber -> Paxos d ()
setNextExpectedBallotNumber nextBallotNumber = do
  l <- get
  let nextExpected = nextExpectedBallotNumber l
  set l {
    nextExpectedBallotNumber = max nextExpected nextBallotNumber
    }
  return ()

-- Doing the state monad thing here explicitly, because no interest in pulling in mtl
set :: Ledger d -> Paxos d ()
set l = Paxos $ \_ -> return (l,())

get :: Paxos d (Ledger d)
get = Paxos $ \l -> return (l,l)

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
