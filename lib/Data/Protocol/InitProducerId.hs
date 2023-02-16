{-# LANGUAGE InstanceSigs #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use <$>" #-}
module Data.Protocol.InitProducerId
  ( InitProducerIdRequest(..)
  , InitProducerIdResponse(..)
  , ProducerId
  , ProducerEpoch
  , getInitProducerIdResponse
  ) where
import Data.Protocol.Types (TransactionId, TimeoutMs, ThrottleTimeMs, ErrorCode, getErrorCode)
import Data.Protocol.NullableString (CompactNullableString, nullableStringToBuilder, compactNullableStringToBuilder)
import Data.Int (Int64, Int16)
import Data.Protocol.Classes (KafkaRequest (header, body))
import Data.Protocol.MessageHeader (MessageHeader (RequestHeaderV1), CorrelationId)
import Data.Binary.Builder (Builder)
import Data.ByteString.Builder (int32BE, int64BE, int16BE)
import Data.Protocol.ApiKey (ApiKey(InitProducerId), ApiVersion)
import Data.Binary (Get)
import Data.Binary.Get (getInt32be, getInt64be, getInt16be)


type TransactionTimeoutMs = TimeoutMs
type CompactTransactionId = CompactNullableString
type ProducerId = Int64
type ProducerEpoch = Int16

data InitProducerIdRequest
  = InitProducerIdRequestV0 TransactionId TransactionTimeoutMs
  | InitProducerIdRequestV4 CompactTransactionId TransactionTimeoutMs ProducerId ProducerEpoch

data InitProducerIdResponse
  = InitProducerIdResponseV0 ThrottleTimeMs ErrorCode ProducerId ProducerEpoch
  | InitProducerIdResponseV4 ThrottleTimeMs ErrorCode ProducerId ProducerEpoch


instance KafkaRequest InitProducerIdRequest where
  header :: CorrelationId -> InitProducerIdRequest -> MessageHeader
  header correlationId (InitProducerIdRequestV0 {}) =
    RequestHeaderV1 InitProducerId 0 correlationId Nothing
  header correlationId (InitProducerIdRequestV4 {}) =
    RequestHeaderV1 InitProducerId 4 correlationId Nothing
  body :: InitProducerIdRequest -> Builder
  body = toBuilder


toBuilder :: InitProducerIdRequest -> Builder
toBuilder (InitProducerIdRequestV0 transactionId transactionTimeoutMs) =
  nullableStringToBuilder transactionId
  <> int32BE transactionTimeoutMs
toBuilder (InitProducerIdRequestV4 transactionId transactionTimeoutMs producerId producerEpoch) =
  compactNullableStringToBuilder transactionId
  <> int32BE transactionTimeoutMs
  <> int64BE producerId
  <> int16BE producerEpoch


getInitProducerIdResponse :: ApiVersion -> Get InitProducerIdResponse
getInitProducerIdResponse 0 = do
  throttleTimeMs <- getInt32be
  errorCode <- getErrorCode
  producerId <- getInt64be
  producerEpoch <- getInt16be

  return $ InitProducerIdResponseV0 throttleTimeMs errorCode producerId producerEpoch

getInitProducerIdResponse 4 = do
  throttleTimeMs <- getInt32be
  errorCode <- getErrorCode
  producerId <- getInt64be
  producerEpoch <- getInt16be

  return $ InitProducerIdResponseV4 throttleTimeMs errorCode producerId producerEpoch

getInitProducerIdResponse _ = undefined