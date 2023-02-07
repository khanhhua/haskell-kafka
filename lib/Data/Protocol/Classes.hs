module Data.Protocol.Classes where

import Data.Binary.Builder (Builder)
import Data.ByteString (ByteString)
import Data.Protocol.MessageHeader


class KafkaRequest a where
  header :: CorrelationId -> a -> MessageHeader
  body :: a -> Builder


class KafkaResponse a where
  fromByteString :: ByteString -> a


class FromNum a where
  fromNum :: (Num b, Eq b) => b -> a