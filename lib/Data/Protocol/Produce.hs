{-# LANGUAGE InstanceSigs #-}
module Data.Protocol.Produce where

import Data.ByteString.Builder
import Data.Binary.Get
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as BL

import Data.Protocol.Classes
    ( KafkaResponse(..), KafkaRequest(..) )
import Data.Protocol.Types
import Data.ByteString ( fromStrict )
import Data.Protocol.MessageHeader (MessageHeader (RequestHeaderV0, RequestHeaderV1), CorrelationId, ApiKey (Produce))
import Data.Protocol.NullableString (nullableStringToBuilder)


type TopicData = (TopicName, Index, Records)

data ProduceRequest
  = ProduceRequestV0 Acks TimeoutMs TopicData
  | ProduceRequestV1 Acks TimeoutMs TopicData
  | ProduceRequestV3 TransactionId Acks TimeoutMs TopicData


data ProduceResponse
  = ProduceResponseV0 TopicName (Index, ErrorCode, BaseOffset)
  | ProduceResponseV1 TopicName (Index, ErrorCode, BaseOffset) ThrottleTimeMs
  | ProduceResponseV3 TopicName (Index, ErrorCode, BaseOffset, LogAppendTimeMs) ThrottleTimeMs


instance KafkaRequest ProduceRequest where
  header :: CorrelationId -> ProduceRequest -> MessageHeader
  header correlationId (ProduceRequestV0 {}) = RequestHeaderV0 Produce 0 correlationId
  header correlationId (ProduceRequestV1 {}) = RequestHeaderV1 Produce 1 correlationId Nothing
  header correlationId (ProduceRequestV3 {}) = RequestHeaderV1 Produce 3 correlationId Nothing
  body :: ProduceRequest -> Builder
  body = toBuilder


instance KafkaResponse ProduceResponse where
  fromByteString = fromByteString_ . fromStrict


toBuilder :: ProduceRequest -> Builder
toBuilder (ProduceRequestV0 acks timeoutMs (name, index, records)) =
  int16BE acks 
  <> int32BE timeoutMs
  <> string8 name
  <> int32BE index
  <> byteString records
toBuilder (ProduceRequestV1 acks timeoutMs (name, index, records)) =
  int16BE acks 
  <> int32BE timeoutMs
  <> string8 name
  <> int32BE index
  <> byteString records
toBuilder (ProduceRequestV3 transactionid acks timeoutMs (name, index, records)) =
  nullableStringToBuilder transactionid
  <> int16BE acks 
  <> int32BE timeoutMs
  <> string8 name
  <> int32BE index
  <> byteString records


fromByteString_ :: BL.ByteString -> ProduceResponse
fromByteString_ =
  runGet byteDecoder
  where
    byteDecoder = do
      topicLength <- getInt16be
      topicName <- Char8.unpack <$> getByteString (fromIntegral topicLength)
      index <- getInt32be
      errorCode <- toEnum . fromIntegral <$> getInt16be
      baseOffset <- getInt64be

      return $ ProduceResponseV0 topicName (index, errorCode, baseOffset)