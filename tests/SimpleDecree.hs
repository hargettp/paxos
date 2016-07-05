{-# LANGUAGE DeriveGeneric #-}

module SimpleDecree (
  IntegerOperation(..)
)
where

  -- local imports

import Control.Consensus.Paxos

-- external imports

import qualified Data.Serialize as C

import GHC.Generics

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data IntegerOperation =
  SetValue Integer |
  GetValue Integer |
  AddDelta Integer |
  SubtractDelta Integer |
  MultiplyFactor Integer |
  DivideByFactor Integer
  deriving (Generic, Eq, Show)

instance C.Serialize IntegerOperation

instance Decreeable IntegerOperation
