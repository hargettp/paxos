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
mkLedger,
followBasicPaxosBallot

) where

-- local imports

-- external imports

import Control.Consensus.Paxos

import qualified Data.Map as M
import Data.Maybe (isJust)
import Data.Serialize
import qualified Data.Set as S

import Network.Endpoints
import Network.RPC.Typed

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

followBasicPaxosBallot :: (Decreeable d) => Endpoint -> Ledger d -> Name -> IO (Ledger d,Maybe (Decree d))
followBasicPaxosBallot endpoint p name = do
  maybePrepare <- hearTimeout endpoint name "prepare" pcallTimeout
  case maybePrepare of
    Just (prep,reply1) -> do
      (p1,vote1) <- onPrepare p prep
      reply1 vote1
      maybePropose <- hearTimeout endpoint name "propose" pcallTimeout
      case maybePropose of
        Just (prop,reply2) -> do
          (p2,vote2) <- onPropose p1 prop
          reply2 vote2
          maybeDecree <- hearTimeout endpoint name "accept" pcallTimeout
          case maybeDecree of
            Just (decree,reply3) -> do
              (p3,()) <- onAccept p2 decree
              reply3 ()
              return (p3,Just decree)
            _ -> return (p2,Nothing)
        _ -> return (p1,Nothing)
    _ -> return (p,Nothing)

type MemberNames = M.Map MemberId Name

mkLedger :: (Decreeable d) => Endpoint -> MemberNames -> Name -> Paxos d
mkLedger endpoint members name = Paxos {
  prepare = pcall endpoint members name "prepare",
  propose = pcall endpoint members name "propose",
  accept = pcall endpoint members name "accept"
}

{-|
Invoke a method on members of the Paxos instance. Because of the semantics of `gcallWithTimeout`, there
will be a response for every `Member`, even if it's just `Nothing`.
-}
pcall :: (Decreeable d,Serialize a,Serialize r) => Endpoint -> MemberNames -> Name -> String -> Ledger d -> a -> IO (M.Map MemberId (Maybe r))
pcall endpoint memberNames name method m args = do
  let cs = newCallSite endpoint name
      members = lookupMany (S.elems $ paxosMembers m) memberNames
      names = M.elems members
  responses <- gcallWithTimeout cs names method pcallTimeout args
  return $ composeMaps members responses

pcallTimeout :: Int
pcallTimeout = 150

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
