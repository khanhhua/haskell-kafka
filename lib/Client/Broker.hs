module Client.Broker (getSupportedApis, getMetadata) where

import Network.Socket (Socket)
import Network.Socket.ByteString (recv, sendAll)

import Data.Protocol.ApiVersions (ApiKeyVersion, ApiVersionsRequest (ApiVersionsRequestV0), ApiVersionsResponse (ApiVersionsReponseV0, ApiVersionsReponseV1), getApiVersionsResponse)
import Data.Protocol (encodeMessage, decodeResponse)
import Data.Binary (Get)
import Data.Protocol.MessageHeader (CorrelationId)
import Data.Binary.Get (runGet)
import qualified Data.ByteString.Lazy as B
import Data.Protocol.Classes (KafkaRequest)
import Data.Protocol.Metadata (Broker, MetadataRequest (MetadataRequestV0), MetadataResponse (MetadataResponseV0), getMetadataResponse)
import Data.Protocol.Types (TopicName)


dEFAULT_BUFFER_SIZE = 4096 :: Int


sendAndRecv :: KafkaRequest a => Socket -> a -> CorrelationId -> IO B.ByteString
sendAndRecv sock req correlationId = do
  sendAll sock (encodeMessage req correlationId)
  B.fromStrict <$> recv sock dEFAULT_BUFFER_SIZE


getSupportedApis :: Socket -> IO [ApiKeyVersion]
getSupportedApis sock = do
  let
    -- TODO correlationId MUST BE incremental, possibly via IO
    correlationId = 1
    req = ApiVersionsRequestV0

  bytes <- sendAndRecv sock req correlationId

  let
    (_messageHeader, apiVersionsResponse) = runGet decoder bytes
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
    (_messageHeader, metadataResponse) = runGet decoder bytes
  case metadataResponse of
    MetadataResponseV0 brokers _ -> return brokers

  where
    decoder :: Get (CorrelationId, MetadataResponse)
    decoder = decodeResponse (getMetadataResponse 0)
