module Client.Broker (getSupportedApis, getMetadata) where

import Data.ByteString.Lazy as BL ( fromStrict )
import Network.Socket (Socket)

import Data.Protocol.ApiVersions (ApiKeyVersion, ApiVersionsRequest (ApiVersionsRequestV2), ApiVersionsResponse (ApiVersionsReponseV0, ApiVersionsReponseV1), getApiVersionsResponse)
import Data.Protocol (decodeResponse)
import Data.Binary (Get)
import Data.Protocol.MessageHeader (CorrelationId)
import Data.Binary.Get (runGet)
import Data.Protocol.Metadata (Broker, MetadataRequest (MetadataRequestV0), MetadataResponse (MetadataResponseV0, MetadataResponseV1), getMetadataResponse)
import Data.Protocol.Types (TopicName)

import Client.Networking (sendAndRecv)
import Data.Protocol.ApiKey (ApiVersion)


getSupportedApis :: Socket -> IO [ApiKeyVersion]
getSupportedApis sock = do
  let
    -- TODO correlationId MUST BE incremental, possibly via IO
    correlationId = 1
    req = ApiVersionsRequestV2

  bytes <- BL.fromStrict <$> sendAndRecv sock req correlationId
  -- (print . byteStringHex . BL.toStrict) bytes
  let
    (_correlationId, apiVersionsResponse) = runGet decoder bytes
  -- TODO check if correlationId matches, raise Error if foreign correlationId is returned on this socket
  case apiVersionsResponse of
    ApiVersionsReponseV0 _errorCode apiVersions -> return apiVersions
    ApiVersionsReponseV1 _errorCode apiVersions _throttleTimeoutMs -> return apiVersions

  where
    decoder :: Get (CorrelationId, ApiVersionsResponse)
    decoder = decodeResponse (getApiVersionsResponse 0)


getMetadata :: Socket -> [TopicName] -> IO [Broker]
getMetadata sock topicName = do
  let
    correlationId = 1
    req = MetadataRequestV0 topicName

  bytes <- sendAndRecv sock req correlationId

  let
    (_correlationId, metadataResponse) = runGet (decoder 1) (BL.fromStrict bytes)
  print metadataResponse
  case metadataResponse of
    MetadataResponseV0 brokers _ -> return brokers
    MetadataResponseV1 brokers _ _ -> return brokers

  where
    decoder :: ApiVersion -> Get (CorrelationId, MetadataResponse)
    decoder = decodeResponse . getMetadataResponse
