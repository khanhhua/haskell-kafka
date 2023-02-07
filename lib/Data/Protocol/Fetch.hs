{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE InstanceSigs #-}
module Data.Protocol.Fetch where

import Data.Int
-- import Data.ByteString (ByteString)
import Data.ByteString.Builder (Builder, int32BE, int64BE)

import Data.Protocol.Types
  ( ReplicaId
  , MaxWaitMs
  , MinBytes
  , TopicName
  , ErrorCode
  , Records
  )
import qualified Data.ByteString.Char8 as Char8
import Data.Protocol.MessageHeader (CorrelationId, MessageHeader (RequestHeaderV0, RequestHeaderV1), ApiKey (Fetch))
import Data.Protocol.Classes
import Data.Protocol.NullableString (stringToBuilder)
import Data.Binary (Get)
import Data.Binary.Get (getInt16be, getByteString, getInt32be, getInt64be)

type Partition = Int32
type FetchOffset = Int64
type PartitionMaxBytes = Int32

type PartitionIndex = Int32
type HighWatermark = Int64

type PartitionRequest = (Partition, FetchOffset, PartitionMaxBytes)

type Topic = (TopicName, PartitionRequest)

data FetchRequest
  = FetchRequestV0 ReplicaId MaxWaitMs MinBytes Topic
  | FetchRequestV1 ReplicaId MaxWaitMs MinBytes Topic

data FetchResponse
  = FetchResponseV0 TopicName PartitionIndex ErrorCode HighWatermark Records


instance KafkaRequest FetchRequest where
  header :: CorrelationId -> FetchRequest -> MessageHeader
  header correlationId (FetchRequestV0 {}) =
    RequestHeaderV0 Fetch 1 correlationId
  header correlationId _ =
    RequestHeaderV1 Fetch 1 correlationId Nothing
  body = toBuilder


toBuilder :: FetchRequest -> Builder
toBuilder (FetchRequestV0 replicaId maxWaitMs minBytes (topicName, (partition_, fetchOffset, partitionMaxBytes))) =
  int32BE replicaId
  <> int32BE maxWaitMs
  <> int32BE minBytes
  <> stringToBuilder topicName
  <> int32BE partition_
  <> int64BE fetchOffset
  <> int32BE partitionMaxBytes
toBuilder (FetchRequestV1 {}) = undefined

getFetchResponse :: Int -> Get FetchResponse
getFetchResponse size = do
  topicLength <- getInt16be
  topicName <- Char8.unpack <$> getByteString (fromIntegral topicLength)
  partitionIndex <- getInt32be
  errorCode <- toEnum. fromIntegral <$> getInt16be
  highWatermark <- getInt64be
  records <- getByteString (size - 16 - 16 - 32 - 16 - 64)
  
  return $ FetchResponseV0 topicName partitionIndex errorCode highWatermark records