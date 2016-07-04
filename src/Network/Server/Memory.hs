-----------------------------------------------------------------------------
-- |
-- Module      :  Network.Server.Memory
-- Copyright   :  (c) Phil Hargett 2016
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Network.Server.Memory (

  withMemoryServer

) where

-- local imports

import Network.Server

-- external imports

import Network.Endpoints
import Network.Transport.Memory

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

withMemoryServer :: Name -> (Transport -> Endpoint -> IO a) -> IO a
withMemoryServer = withServer newMemoryTransport
