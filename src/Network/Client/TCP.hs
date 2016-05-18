-----------------------------------------------------------------------------
-- |
-- Module      :  Network.Client.TCP
-- Copyright   :  (c) Phil Hargett 2016
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Network.Client.TCP (

  withTCP4Client,
  withTCP6Client

) where

-- local imports

import Network.Client

-- external imports
import Network.Endpoints
import Network.Transport.Sockets.TCP

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

withTCP4Client :: Name -> (Endpoint -> IO ()) -> IO ()
withTCP4Client = withClient (newTCPTransport4 tcpSocketResolver4)

withTCP6Client :: Name -> (Endpoint -> IO ()) -> IO ()
withTCP6Client = withClient (newTCPTransport6 tcpSocketResolver6)
