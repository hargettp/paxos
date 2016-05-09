-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Paxos.Endpoints
-- Copyright   :  (c) Phil Hargett 2016
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  Experimental
-- Portability :  Non-portable (uses STM)
--
--
-----------------------------------------------------------------------------

module Control.Consensus.Paxos.Endpoints (

  MemberNames,
  mkProposer

) where

-- local imports
import Control.Consensus.Paxos.Types

-- external imports

import qualified Data.Map as M
import Data.Maybe (fromJust)
import Data.Serialize

import Network.Endpoints
import Network.RPC.Typed

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

type MemberNames = M.Map Name MemberId

mkProposer :: (Decreeable d) => Endpoint -> MemberNames -> Name -> Proposer d
mkProposer endpoint members name = Proposer {
  prepare = pcall endpoint members name "prepare",
  propose = pcall endpoint members name "propose",
  accept = pcall endpoint members name "accept"
}

{-|
Invoke a method on members of the Paxos instance. Because of the semantics of `gcallWithTimeout`, there
will be a response for every `Member`, even if it's just `Nothing`.
-}
pcall :: (Decreeable d,Serialize a,Serialize r) => Endpoint -> MemberNames -> Name -> String -> Member d -> a -> IO (M.Map MemberId (Maybe r))
pcall endpoint names name method m args = do
  let cs = newCallSite endpoint name
      members = M.keys names
  responses <- gcallWithTimeout cs members method (fromInteger $ paxosTimeout m) args
  return $ M.mapKeys (\n -> fromJust $ M.lookup n names) responses
