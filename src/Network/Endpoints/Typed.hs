-----------------------------------------------------------------------------
-- |
-- Module      :  Network.Endpoints.Typed
-- Copyright   :  (c) Phil Hargett 2016
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Network.Endpoints.Typed (

  selectMessage,
  selectMessageTimeout,
  detectMessage,
  detectMessageTimeout,

  module Network.Endpoints
) where

-- local imports

-- external imports

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.Mailbox
import Control.Concurrent.STM

import Data.Serialize

import Network.Endpoints hiding (selectMessage,selectMessageTimeout,detectMessage,detectMessageTimeout)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
Wait for a message to be selected within the timeout, blocking until either a message
is available or the timeout has occurred.  If a message was available, returns @Just message@,
but returns @Nothing@ if no message available before the timeout occurred. Like
'selectMessage', this function enables out of order message reception.
-}
selectMessage :: (Serialize a) => Endpoint -> (a -> Maybe r) -> IO r
selectMessage endpoint testFn = atomically $ selectMailbox (endpointInbound endpoint) $ typed testFn

{-|
Wait for a message to be selected within the timeout, blocking until either a message
is available or the timeout has occurred.  If a message was available, returns @Just message@,
but returns @Nothing@ if no message available before the timeout occurred. Like
'selectMessage', this function enables out of order message reception.
-}
selectMessageTimeout :: (Serialize a) => Endpoint -> Int -> (a -> Maybe r) -> IO (Maybe r)
selectMessageTimeout endpoint delay testFn = do
  resultOrTimeout <- race (selectMessage endpoint testFn) (threadDelay delay)
  case resultOrTimeout of
    Left result -> return $ Just result
    Right () -> return Nothing

{-|
Find a 'Message' in the 'Endpoint' 'Mailbox' matching the supplied
test function, or block until one is available.  Note that any such message
is left in the mailbox, and thus repeated calls to this function could find the
message if it is not consumed immediately.
-}
detectMessage :: (Serialize a) => Endpoint -> (a -> Maybe r) -> IO r
detectMessage endpoint testFn = atomically $ findMailbox (endpointInbound endpoint) $ typed testFn

{-|
Find a 'Message' in the 'Endpoint' 'Mailbox' matching the supplied
test function, or block until either one is available or the timeout expires.
Note that any such message is left in the mailbox, and thus repeated calls
to this function could find the message if it is not consumed immediately.
-}
detectMessageTimeout :: (Serialize a) => Endpoint -> Int -> (a -> Maybe r) -> IO (Maybe r)
detectMessageTimeout endpoint delay testFn = do
  resultOrTimeout <- race (detectMessage endpoint testFn) (threadDelay delay)
  case resultOrTimeout of
    Left result -> return $ Just result
    Right () -> return Nothing

typed :: (Serialize a) => (a -> Maybe r) -> Message -> Maybe r
typed testFn message = case decode message of
  Left _ -> Nothing
  Right args -> testFn args
