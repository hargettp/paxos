-----------------------------------------------------------------------------
-- |
-- Module      :  Network.Server.TCP
-- Copyright   :  (c) Phil Hargett 2016
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Network.Server.TCP (

  withTCP4Server,
  withTCP6Server

) where

-- local imports

import Network.Server

-- external imports

import Network.Endpoints
import Network.Transport.Sockets.TCP

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

withTCP4Server :: Name -> (Endpoint -> IO ()) -> IO ()
withTCP4Server = withServer (newTCPTransport4 tcpSocketResolver4)

withTCP6Server :: Name -> (Endpoint -> IO ()) -> IO ()
withTCP6Server = withServer (newTCPTransport6 tcpSocketResolver6)
