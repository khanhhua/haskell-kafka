{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE InstanceSigs #-}
module Data.Protocol.Fetch where

import Data.Int ( Int32, Int64 )
import Data.ByteString.Builder (Builder, int32BE, int64BE)

import Data.Protocol.Types
  ( ReplicaId
  , MaxWaitMs
  , MinBytes
  , TopicName
  , ErrorCode
  , Records, PartitionIndex
  )
import Data.Protocol.MessageHeader (CorrelationId, MessageHeader (RequestHeaderV0, RequestHeaderV1))
import Data.Protocol.Classes
import Data.Protocol.NullableString (stringToBuilder, getString)
import Data.Binary (Get)
import Data.Binary.Get (getInt16be, getInt32be, getInt64be)
import Data.Protocol.ApiKey (ApiKey(Fetch), ApiVersion)
import Data.Protocol.Array (arrayToBuilder, getArray)
import Data.Protocol.NullableByte (getNullableBytes)

type Partition = Int32
type FetchOffset = Int64
type PartitionMaxBytes = Int32

type HighWatermark = Int64

type PartitionRequest = (Partition, FetchOffset, PartitionMaxBytes)

type Topic = (TopicName, [PartitionRequest])

data FetchRequest
  = FetchRequestV0 ReplicaId MaxWaitMs MinBytes [Topic]
  | FetchRequestV1 ReplicaId MaxWaitMs MinBytes [Topic]

type PartitionData = (PartitionIndex, ErrorCode, HighWatermark, Records)

data FetchResponse
  = FetchResponseV0 TopicName [PartitionData]


instance KafkaRequest FetchRequest where
  header :: CorrelationId -> FetchRequest -> MessageHeader
  header correlationId (FetchRequestV0 {}) =
    RequestHeaderV0 Fetch 1 correlationId
  header correlationId _ =
    RequestHeaderV1 Fetch 1 correlationId Nothing
  body = toBuilder


topicToBuilder :: Topic -> Builder
topicToBuilder (topicName, partitionRequests) =
  stringToBuilder topicName
  <> arrayToBuilder partitionRequestToBuilder partitionRequests

partitionRequestToBuilder :: PartitionRequest -> Builder
partitionRequestToBuilder (partition_, fetchOffset, partitionMaxBytes) =
  int32BE partition_
  <> int64BE fetchOffset
  <> int32BE partitionMaxBytes

toBuilder :: FetchRequest -> Builder
toBuilder (FetchRequestV0 replicaId maxWaitMs minBytes topics) =
  int32BE replicaId
  <> int32BE maxWaitMs
  <> int32BE minBytes
  <> arrayToBuilder topicToBuilder topics
toBuilder (FetchRequestV1 {}) = undefined

getFetchResponse :: ApiVersion -> Get [FetchResponse]
getFetchResponse = getFetchResponseArray

getFetchResponseArray :: ApiVersion -> Get [FetchResponse]
getFetchResponseArray = getArray . getFetchResponseItem

getFetchResponseItem :: ApiVersion -> Get FetchResponse
getFetchResponseItem apiVersion = do
  topicName <- getString
  partitionData <- getArray getPartitionData
  
  case apiVersion of
    0 -> return $ FetchResponseV0 topicName partitionData
    _ -> undefined

getPartitionData :: Get PartitionData
getPartitionData = do
  partitionIndex <- getInt32be
  errorCode <- toEnum. fromIntegral <$> getInt16be
  highWatermark <- getInt64be
  records <- getNullableBytes

  return (partitionIndex, errorCode, highWatermark, records)