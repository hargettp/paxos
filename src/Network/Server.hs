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

import Network.Endpoints
import Network.Transport

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

withServer :: IO Transport -> Name -> (Endpoint -> IO ()) -> IO ()
withServer transportFactory name serverFn = 
  withTransport transportFactory $ \transport -> do
    endpoint <- newEndpoint
    withEndpoint transport endpoint $
      withBinding transport endpoint name $
        serverFn endpoint
