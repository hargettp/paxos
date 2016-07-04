-----------------------------------------------------------------------------
-- |
-- Module      :  Network.Server
-- Copyright   :  (c) Phil Hargett 2016
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Network.Server (

  withServer

) where

-- local imports

-- external imports

import Control.Concurrent.STM

import Network.Endpoints
import Network.Transport

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

withServer :: IO Transport -> Name -> (Transport -> Endpoint -> IO a) -> IO a
withServer transportFactory name serverFn = do
  -- just a hack until the underlying functions are rewritten
  vResult <- atomically newEmptyTMVar
  withTransport transportFactory $ \transport -> do
    endpoint <- newEndpoint
    withEndpoint transport endpoint $
      withBinding transport endpoint name $ do
        val <- serverFn transport endpoint
        atomically $ putTMVar vResult val
  atomically $ readTMVar vResult
