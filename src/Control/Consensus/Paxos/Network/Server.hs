-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Paxos.Network.Server
-- Copyright   :  (c) Phil Hargett 2016
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Control.Consensus.Paxos.Network.Server (

  MemberNames,
  memberName,
  protocol

) where

-- local imports

-- external imports

import Control.Consensus.Paxos

import qualified Data.Map as M
import Data.Maybe (isJust)
import Data.Serialize
import qualified Data.Set as S

import Debug.Trace

import Network.Endpoints
import Network.RPC.Typed

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

type MemberNames = M.Map MemberId Name

memberName :: Ledger d -> MemberNames -> Name
memberName ledger memberNames =
  let me = paxosMemberId ledger
      Just name = M.lookup me memberNames
      in name


protocol :: (Decreeable d) => Endpoint -> MemberNames -> Name -> Protocol d
protocol endpoint members name = Protocol {
  prepare = pcall endpoint members name "prepare",
  propose = pcall endpoint members name "propose",
  accept = pcall endpoint members name "accept",
  expectPrepare = pack endpoint name "prepare",
  expectPropose = pack endpoint name "propose",
  expectAccept = phear endpoint name "accept"
}

{-|
Invoke a method on members of the Paxos instance. Because of the semantics of `gcallWithTimeout`, there
will be a response for every `Member`, even if it's just `Nothing`.
-}
pcall :: (Decreeable d,Serialize a,Serialize r) => Endpoint -> MemberNames -> Name -> String -> Members -> a -> Paxos d (M.Map MemberId (Maybe r))
pcall endpoint memberNames name method m args = io $ do
  let cs = newCallSite endpoint name
      members = lookupMany (S.elems m) memberNames
      names = M.elems members
  traceIO $ "pcalling " ++ method ++ " on " ++ show name ++ " to " ++ show names
  responses <- gcallWithTimeout cs names method pcallTimeout args
  return $ composeMaps members responses

pcallTimeout :: Int
pcallTimeout = 150000

pack :: (Serialize a, Serialize r, Decreeable d) => Endpoint -> Name -> Method -> (a -> Paxos d r) -> Paxos d Bool
pack endpoint name method fn = do
  maybeResult <- phear endpoint name method fn
  case maybeResult of
    Just _ -> return True
    Nothing -> return False

phear :: (Serialize a, Serialize r, Decreeable d) => Endpoint -> Name -> Method -> (a -> Paxos d r) -> Paxos d (Maybe r)
phear endpoint name method fn = do
  io $ traceIO $ "expecting " ++ method ++ " on " ++ show name
  maybeArg <- io $ hearTimeout endpoint name method pcallTimeout
  case maybeArg of
    Just (arg,reply) -> do
      r <- fn arg
      io $ reply r
      return $ Just r
    Nothing -> return Nothing

-------------------------------------------------------------------------------
-- Utility
-------------------------------------------------------------------------------

{-|
Given a list of keys and a `M.Map` of keys to values,
return a new `M.Map` that only has keys from the original list

and where the original map has a value for the key. This is quick way
of looking up a bunch of keys at once and getting a (possibly empty)
map with the results.
-}
lookupMany :: (Ord k) => [k] -> M.Map k v -> M.Map k v
lookupMany keys aMap = M.fromList
  . map (\(key,Just value) -> (key,value))
  . filter (\(_,maybeValue) -> isJust maybeValue)
  $ map (\key -> (key,M.lookup key aMap)) keys


{-|
Compose 2 `M.Map`s: for each key in map 1, use the associated value for the key in map 1
as a key to find a value in map 2. Return a map with only keys from map 1 such that a value was found
in map 2 the value for that key in map 1.
-}
composeMaps :: (Ord k1, Ord k2) => M.Map k1 k2 -> M.Map k2 v -> M.Map k1 v
composeMaps m1 m2 = M.fromList
  . map (\(key,Just value) -> (key,value))
  . filter (\(_,maybeValue) -> isJust maybeValue)
  . map (\(key1,key2) -> (key1,M.lookup key2 m2))
  $ M.toList m1
