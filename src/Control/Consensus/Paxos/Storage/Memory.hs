-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Paxos.Storage.Memory
-- Copyright   :  (c) Phil Hargett 2016
-- License     :  MIT (see LICENSE file)
--
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  $(Stability)
-- Portability :  $(Portability)
--
--
-----------------------------------------------------------------------------

module Control.Consensus.Paxos.Storage.Memory (
  storage
) where

-- local imports

import Control.Consensus.Paxos.Types

-- external imports

import Control.Concurrent.STM

import qualified Data.Map as M

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

type MemoryStore d = TVar (M.Map InstanceId (Ledger d))

storage :: (Decreeable d) => IO (Storage d)
storage = do
  store <- atomically $ newTVar M.empty
  return Storage {
    loadLedger = memoryLoad store,
    saveLedger = memorySave store
    }

memoryLoad :: MemoryStore d -> InstanceId -> IO (Maybe (Ledger d))
memoryLoad store instanceId = atomically $ do
  ledgers <- readTVar store
  return $ M.lookup instanceId ledgers

memorySave :: MemoryStore d -> Ledger d -> IO ()
memorySave store ledger = atomically $
  modifyTVar store $ \ledgers -> M.insert (paxosInstanceId ledger) ledger ledgers
