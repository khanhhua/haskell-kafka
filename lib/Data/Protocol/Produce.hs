{-# LANGUAGE InstanceSigs #-}
module Data.Protocol.Produce where

import Data.Maybe (fromMaybe)
import Data.ByteString.Builder
import Data.Binary.Get

import Data.Protocol.Classes
    ( KafkaRequest(..) )
import Data.Protocol.Types
import Data.Protocol.MessageHeader (MessageHeader (RequestHeaderV0, RequestHeaderV1), CorrelationId)
import Data.Protocol.NullableString (nullableStringToBuilder, getNullableString, stringToBuilder)
import Data.Protocol.ApiKey
import Data.Protocol.Array (getArray, arrayToBuilder)
import Data.Protocol.NullableByte (nullableBytesToBuilder)


type TopicData = (TopicName, [PartitionData])

type PartitionResponse = (Index, ErrorCode, BaseOffset)

data ProduceRequest
  = ProduceRequestV0 Acks TimeoutMs [TopicData]
  | ProduceRequestV1 Acks TimeoutMs [TopicData]
  | ProduceRequestV3 TransactionId Acks TimeoutMs [TopicData]
  deriving Show

data ProduceResponse
  = ProduceResponseV0 TopicName [PartitionResponse]
  | ProduceResponseV1 TopicName [PartitionResponse] ThrottleTimeMs
  | ProduceResponseV3 TopicName [PartitionResponse] ThrottleTimeMs
  deriving Show


instance KafkaRequest ProduceRequest where
  header :: CorrelationId -> ProduceRequest -> MessageHeader
  header correlationId (ProduceRequestV0 {}) = RequestHeaderV0 Produce 0 correlationId
  header correlationId (ProduceRequestV1 {}) = RequestHeaderV1 Produce 1 correlationId Nothing
  header correlationId (ProduceRequestV3 {}) = RequestHeaderV1 Produce 3 correlationId Nothing
  body :: ProduceRequest -> Builder
  body = toBuilder


topicDataToBuilder :: TopicData -> Builder
topicDataToBuilder (name, partitionData) =
  stringToBuilder name
  <> arrayToBuilder partitionDataToBuilder partitionData

partitionDataToBuilder :: PartitionData -> Builder
partitionDataToBuilder (index, records) =
  int32BE index
  <> nullableBytesToBuilder records


toBuilder :: ProduceRequest -> Builder
toBuilder (ProduceRequestV0 acks timeoutMs topicData) =
  int16BE acks 
  <> int32BE timeoutMs
  <> arrayToBuilder topicDataToBuilder topicData
toBuilder (ProduceRequestV1 acks timeoutMs topicData) =
  int16BE acks 
  <> int32BE timeoutMs
  <> arrayToBuilder topicDataToBuilder topicData
toBuilder (ProduceRequestV3 transactionid acks timeoutMs topicData) =
  nullableStringToBuilder transactionid
  <> int16BE acks 
  <> int32BE timeoutMs
  <> arrayToBuilder topicDataToBuilder topicData


getProduceResponse :: ApiVersion -> Get [ProduceResponse]
getProduceResponse = getArray . getProduceResponseItem


getProduceResponseItem :: ApiVersion -> Get ProduceResponse
getProduceResponseItem apiVersion = do
  topicName <- fromMaybe "" <$> getNullableString

  case apiVersion of
    0 -> do
      ProduceResponseV0 topicName <$> getPartitionResponseArray
    1 -> do
      partitionResponseArray <- getPartitionResponseArray
      ProduceResponseV1 topicName partitionResponseArray <$> getInt32be
    3 -> do
      partitionResponseArray <- getPartitionResponseArray
      ProduceResponseV3 topicName partitionResponseArray <$> getInt32be
    _ -> undefined


getPartitionResponse :: Get [PartitionResponse]
getPartitionResponse = getPartitionResponseArray

getPartitionResponseArray :: Get [PartitionResponse]
getPartitionResponseArray = getArray getPartitionResponseItem


getPartitionResponseItem :: Get PartitionResponse
getPartitionResponseItem = do
  index <- getInt32be
  errorCode <- getErrorCode
  baseOffset <- getInt64be

  return (index, errorCode, baseOffset)
