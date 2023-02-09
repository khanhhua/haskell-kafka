{-# LANGUAGE InstanceSigs #-}
module Data.Protocol.ApiVersions where

import Data.Protocol.ApiKey ( ApiKey(ApiVersions), ApiVersion, getApiKey )
import Data.Protocol.MessageHeader (CorrelationId, MessageHeader (RequestHeaderV0))
import Data.Protocol.Classes
import Data.Binary.Builder (empty)
import Data.Protocol.Types (ErrorCode, ThrottleTimeMs, getErrorCode)
import Data.Int (Int16)
import Data.Binary (Get)
import Data.Binary.Get (getInt16be, getInt32be)
import Control.Monad (replicateM)


type MinVersion = Int16

type MaxVersion = Int16

type ApiKeyVersion = (ApiKey, MinVersion, MaxVersion)

data ApiVersionsRequest
  = ApiVersionsRequestV0

data ApiVersionsResponse
  = ApiVersionsReponseV0 ErrorCode [ApiKeyVersion]
  | ApiVersionsReponseV1 ErrorCode [ApiKeyVersion] ThrottleTimeMs


instance KafkaRequest ApiVersionsRequest where
  header :: CorrelationId -> ApiVersionsRequest -> MessageHeader
  header correlationId ApiVersionsRequestV0 =
    RequestHeaderV0 ApiVersions 1 correlationId
  body = const empty


getApiVersionsResponse :: ApiVersion -> Get ApiVersionsResponse
getApiVersionsResponse apiVersion = do
  errorCode <- getErrorCode
  n <- fromIntegral <$> getInt32be
  apiKeyVersions <- replicateM n getApiKeyVersion

  case apiVersion of
    0 -> do
      return $ ApiVersionsReponseV0 errorCode apiKeyVersions
    1 -> do
      ApiVersionsReponseV1 errorCode apiKeyVersions <$> getInt32be
    _ ->
      undefined


getApiKeyVersion :: Get ApiKeyVersion
getApiKeyVersion = do
  apiKey <- getApiKey
  minVersion <- getInt16be
  maxVersion <- getInt16be

  return (apiKey, minVersion, maxVersion)