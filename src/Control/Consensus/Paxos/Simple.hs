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

type Members d = M.Map MemberId (Paxos d)

mkProposer :: (Decreeable d) => Members d -> Proposer d
mkProposer members = Proposer {
  prepare = mcall members onPrepare,
  propose = mcall members onPropose,
  accept = mcall members onAccept
}

mcall :: (Serialize a,Serialize r) => Members d -> (Paxos d -> a -> IO r) -> Paxos d -> a -> IO (M.Map MemberId (Maybe r))
mcall members fn member arg = do
  results <- mapM call $ S.elems $ paxosMembers member
  return $ M.fromList results
  where
    call memberId = case M.lookup memberId members of
      Nothing -> return (memberId,Nothing)
      Just otherMember -> do
        result <- fn otherMember arg
        return (memberId, Just result)
