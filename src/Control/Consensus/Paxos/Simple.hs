-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Paxos.Simple
-- Copyright   :  (c) Phil Hargett 2015
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Control.Consensus.Paxos.Simple (

  mkProposer

) where

-- local imports

import Control.Consensus.Paxos

-- external imports

import qualified Data.Map as M
import Data.Serialize
import qualified Data.Set as S

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

type Members d = M.Map MemberId (Member d)

mkProposer :: (Decreeable d) => Members d -> Proposer d
mkProposer members = Proposer {
  prepare = simplePrepare members,
  propose = simplePropose members,
  accept = simpleAccept members
}

simplePrepare :: (Decreeable d) => Members d -> Member d -> Prepare -> IO (Votes d)
simplePrepare members member prep = mcall members member prep onPrepare

simplePropose :: (Decreeable d) => Members d -> Member d -> Proposal d-> IO (Votes d)
simplePropose members member prop = mcall members member prop onPropose

simpleAccept :: (Decreeable d) => Members d -> Member d -> Decree d -> IO (M.Map MemberId (Maybe Bool))
simpleAccept members member d = mcall members member d onAccept

mcall :: (Serialize a,Serialize r) => Members d -> Member d -> a -> (Member d -> a -> IO r) -> IO (M.Map MemberId (Maybe r))
mcall members member arg fn = do
  results <- mapM call $ S.elems $ paxosMembers member
  return $ M.fromList results
  where
    call memberId = case M.lookup memberId members of
      Nothing -> return (memberId,Nothing)
      Just acceptor -> do
        result <- fn acceptor arg
        return (memberId, Just result)
