module Data.Protocol.Array (arrayToBuilder, getArray) where

import Data.Binary (Get)
import Data.Binary.Get (getInt32be)
import Control.Monad (replicateM)
import Data.Binary.Builder (Builder)
import Data.ByteString.Builder (int32BE)


arrayToBuilder :: (a -> Builder) -> [a] -> Builder
arrayToBuilder toBuilder items = 
  let
    part1 = int32BE $ (fromIntegral . length) items
    part2 = (mconcat $ map toBuilder items)
  in part1 <> part2


getArray :: Get a -> Get [a]
getArray decoder = do
  n <- fromIntegral <$> getInt32be

  replicateM n decoder
