module Data.Protocol.Classes where

import Data.Binary.Builder (Builder)
import Data.Protocol.MessageHeader


class KafkaRequest a where
  header :: CorrelationId -> a -> MessageHeader
  body :: a -> Builder


class FromNum a where
  fromNum :: (Num b, Eq b) => b -> a