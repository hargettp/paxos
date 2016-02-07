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
--   hearAll,

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
