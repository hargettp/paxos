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

  call,
  gcallWithTimeout,
  hear,
  hearTimeout,
  typedMethodSelector,

  module Network.RPC

) where

-- local imports

import Network.Endpoints
import Network.RPC hiding (call,gcallWithTimeout,hear,hearTimeout)
import qualified Network.RPC as R

-- external imports

import qualified Data.Map as M
import Data.Serialize

-- import Debug.Trace

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

call :: (Serialize a,Serialize b) => CallSite -> Name -> Method -> a -> IO b
call cs name method args = do
  result <- call cs name method $ encode args
  let Right value = decode result
  return value

gcallWithTimeout :: (Serialize a,Serialize b) => CallSite -> [Name] -> Method -> Int -> a -> IO (M.Map Name (Maybe b))
gcallWithTimeout cs names method delay args = do
  responses <- R.gcallWithTimeout cs names method delay $ encode args
  return $ decodeResponses responses

decodeResponses :: (Serialize r) => M.Map Name (Maybe Message)  -> M.Map Name (Maybe r)
decodeResponses = M.map decodeResponse
  where
    decodeResponse maybeMsg = case maybeMsg of
      Nothing -> Nothing
      Just msg -> case decode msg of
        Left _ -> Nothing
        Right response -> Just response

hear :: (Serialize a, Serialize r) => Endpoint -> Name -> Method -> IO (a,Reply r)
hear endpoint name method = do
  (caller,rid,args) <- selectMessage endpoint $ typedMethodSelector method
  return (args, reply caller rid)
  where
    reply caller rid result =
      sendMessage endpoint caller $ encode $ Response rid name $ encode result

hearTimeout :: (Serialize a, Serialize r) => Endpoint -> Name -> Method -> Int -> IO (Maybe (a, Reply r))
hearTimeout endpoint name method timeout = do
  -- traceIO $ "listening for " ++ show method ++ " on " ++ show name
  req <- selectMessageTimeout endpoint timeout $ typedMethodSelector method
  case req of
    Just (caller,rid,args) -> do
      -- traceIO $ "heard " ++ show method ++ " on " ++ show name
      return $ Just (args, reply caller rid)
    Nothing -> do
      -- traceIO $ "did not hear " ++ method ++ " on " ++ show name
      return Nothing
  where
  reply caller rid result = sendMessage endpoint caller $ encode $ Response rid name $ encode result

typedMethodSelector :: (Serialize a) => Method -> Message -> Maybe (Name,RequestId,a)
typedMethodSelector method msg =
  case decode msg of
    Left _ ->
      Nothing
    Right (Request rid caller rmethod bytes) ->
      if rmethod == method
        then case decode bytes of
          Left _ -> Nothing
          Right args -> Just (caller,rid,args)
        else Nothing
