-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Paxos.Protocol.Courier
-- Copyright   :  (c) Phil Hargett 2016
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Control.Consensus.Paxos.Protocol.Courier (

  MemberNames,
  memberName,
  protocol,

  Timeouts(..),
  defaultTimeouts

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

type MemberNames = M.Map MemberId Name

data Timeouts = Timeouts {
  leaderTimeout :: Int,
  followerTimeout :: Int
}

defaultTimeouts :: Timeouts
defaultTimeouts =
  let baseTimeout = 250 * 1000 -- 250ms
  in Timeouts {
    leaderTimeout = baseTimeout,
    followerTimeout = 10 * baseTimeout
  }

memberName :: Instance d -> MemberNames -> Name
memberName inst memberNames =
  let me = instanceMe inst
      Just name = M.lookup me memberNames
      in name

protocol :: (Decreeable d) => Timeouts -> Endpoint -> MemberNames -> Name -> Protocol d
protocol timeouts endpoint members name = Protocol {
  prepare = pcall timeouts endpoint members name "prepare",
  propose = pcall timeouts endpoint members name "propose",
  accept = pcall timeouts endpoint members name "accept",
  expectPrepare = pack timeouts endpoint name "prepare",
  expectPropose = pack timeouts endpoint name "propose",
  expectAccept = pack timeouts endpoint name "accept"
}

{-|
Invoke a method on members of the Paxos instance. Because of the semantics of `gcallWithTimeout`, there
will be a response for every `Member`, even if it's just `Nothing`.
-}
pcall :: (Serialize a,Serialize r) => Timeouts -> Endpoint -> MemberNames -> Name -> String -> Members -> a -> Paxos d (M.Map MemberId (Maybe r))
pcall timeouts endpoint memberNames name method m args = io $ do
  let cs = newCallSite endpoint name
      members = lookupMany (S.elems m) memberNames
      names = M.elems members
  responses <- gcallWithTimeout cs names method (leaderTimeout timeouts) args
  return $ composeMaps members responses

pack :: (Instanced a, Serialize a, Serialize r) => Timeouts -> Endpoint -> Name -> Method -> InstanceId -> (a -> Paxos d r) -> Paxos d Bool
pack timeouts endpoint name method instId fn = do
  maybeResult <- phear timeouts endpoint name method instId fn
  case maybeResult of
    Just _ -> return True
    Nothing -> return False

phear :: (Instanced a, Serialize a, Serialize r) => Timeouts -> Endpoint -> Name -> Method -> InstanceId -> (a -> Paxos d r) -> Paxos d (Maybe r)
phear timeouts endpoint name method instId fn = do
  maybeArg <- io $ phearTimeout endpoint name method instId (followerTimeout timeouts)
  case maybeArg of
    Just (arg,reply) -> do
      r <- fn $! arg
      io $ reply r
      return $ Just r
    Nothing -> return Nothing

phearTimeout :: (Instanced a, Serialize a, Serialize r) => Endpoint -> Name -> Method -> InstanceId -> Int -> IO (Maybe (a, Reply r))
phearTimeout endpoint name method instId timeout = do
  req <- selectMessageTimeout endpoint timeout $ typedInstancedMethodSelector method instId
  case req of
    Just (caller,rid,args) ->
      return $ Just (args, reply caller rid)
    Nothing ->
      return Nothing
  where
  reply caller rid result = sendMessage endpoint caller $ encode $ Response rid name $ encode result

typedInstancedMethodSelector :: (Instanced a, Serialize a) => Method -> InstanceId -> Message -> Maybe (Name,RequestId,a)
typedInstancedMethodSelector method instId msg =
  case decode msg of
    Left _ ->
      Nothing
    Right (Request rid caller rmethod bytes) ->
      if rmethod == method
        then case decode bytes of
          Left _ -> Nothing
          Right args -> if currentInstanceId args == instId
            then Just (caller,rid,args)
            else Nothing
        else Nothing

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
