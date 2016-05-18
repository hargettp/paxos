-----------------------------------------------------------------------------
-- |
-- Module      :  Network.Client
-- Copyright   :  (c) Phil Hargett 2016
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Network.Client (

  withClient

) where

-- local imports

-- external imports

import Network.Endpoints
import Network.Transport

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

withClient :: IO Transport -> Name -> (Endpoint -> IO ()) -> IO ()
withClient transportFactory name clientFn =
  withTransport transportFactory $ \transport -> do
    endpoint <- newEndpoint
    withEndpoint transport endpoint $
      withName endpoint name $
        clientFn endpoint
