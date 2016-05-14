{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Network.RPC.Typed
-- Copyright   :  (c) Phil Hargett 2015
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  experimental
-- Portability :  non-portable (uses STM)
--
--
-----------------------------------------------------------------------------

module Network.RPC.Typed (

  newCallSite,

  call,
  gcallWithTimeout,
  hear,
  typedMethodSelector

) where

-- local imports

import Network.Endpoints
import qualified Network.RPC as R

-- external imports

import qualified Data.Map as M
import Data.Serialize

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

newCallSite :: Endpoint -> Name -> R.CallSite
newCallSite = R.newCallSite

call :: (Serialize a,Serialize b) => R.CallSite -> Name -> R.Method -> a -> IO b
call cs name method args = do
  result <- call cs name method $ encode args
  let Right value = decode result
  return value

gcallWithTimeout :: (Serialize a,Serialize b) => R.CallSite -> [Name] -> R.Method -> Int -> a -> IO (M.Map Name (Maybe b))
gcallWithTimeout cs names method delay args = do
  responses <- gcallWithTimeout cs names method delay (encode args)
  return $ decodeResponses responses

decodeResponses :: (Serialize r) => M.Map Name (Maybe Message)  -> M.Map Name (Maybe r)
decodeResponses = M.map decodeResponse
  where
    decodeResponse maybeMsg = case maybeMsg of
      Nothing -> Nothing
      Just msg -> case decode msg of
        Left _ -> Nothing
        Right response -> Just response

hear :: (Serialize a, Serialize r) => Endpoint -> Name -> R.Method -> IO (a,R.Reply r)
hear endpoint name method = do
  (caller,rid,args) <- selectMessage endpoint $ typedMethodSelector method
  return (args, reply caller rid)
  where
    reply caller rid result =
      sendMessage endpoint caller $ encode $ R.Response rid name $ encode result

typedMethodSelector :: (Serialize a) => R.Method -> Message -> Maybe (Name,R.RequestId,a)
typedMethodSelector method msg =
  case decode msg of
    Left _ -> Nothing
    Right (R.Request rid caller rmethod bytes) ->
      if rmethod == method
        then case decode bytes of
          Left _ -> Nothing
          Right args -> Just (caller,rid,args)
        else Nothing
