module Data.Protocol.MessageHeader where

import Data.Int (Int32)
import Data.Binary.Builder (Builder)
import Data.Protocol.NullableString (NullableString, nullableStringToBuilder)
import Data.ByteString.Builder (int16BE, int32BE)
import Data.Protocol.ApiKey


type CorrelationId = Int32

{-| TODO: MessageHeader must be a monoid
-}
data MessageHeader
  = RequestHeaderV0 ApiKey ApiVersion CorrelationId
  | RequestHeaderV1 ApiKey ApiVersion CorrelationId NullableString
  deriving Show

headerToBuilder :: MessageHeader -> Builder
headerToBuilder (RequestHeaderV0 apiKey apiVersion correlationId) =
  apiKeyToBuilder apiKey
  <> int16BE (fromIntegral apiVersion)
  <> int32BE (fromIntegral correlationId)

headerToBuilder (RequestHeaderV1 apiKey apiVersion correlationId clientId) =
  apiKeyToBuilder apiKey
  <> int16BE (fromIntegral apiVersion)
  <> int32BE (fromIntegral correlationId)
  <> nullableStringToBuilder clientId
