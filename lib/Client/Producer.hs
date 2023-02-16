{-# LANGUAGE LambdaCase #-}
module Client.Producer where

import Data.Binary.Builder (toLazyByteString)
import Data.Binary (Get)
import Data.Binary.Get (runGet)

import Network.Socket (Socket)
import Data.Protocol.MessageHeader (CorrelationId)
import Data.Protocol.Types (TopicName, PartitionIndex, TimeoutMs)
import qualified Data.ByteString.Lazy as BL ( fromStrict, length )
import qualified Data.ByteString as B (ByteString)
import Data.Protocol.Produce (PartitionResponse, ProduceResponse (ProduceResponseV0, ProduceResponseV1, ProduceResponseV3), ProduceRequest (ProduceRequestV1), getProduceResponse)
import Data.Protocol.ApiKey (ApiVersion)
import Client.Networking (sendAndRecv)
import Data.Protocol (decodeResponse)
import Data.Protocol.InitProducerId
  ( InitProducerIdResponse(..)
  , ProducerId
  , ProducerEpoch
  , getInitProducerIdResponse, InitProducerIdRequest (InitProducerIdRequestV0)
  )
import Data.Protocol.MessageSet (Message(MessageV1, MessageV0), messageToBuilder, messageSetToBuilder)
import Data.ByteString (toStrict)
import Data.Time.Clock.POSIX (getPOSIXTime)


tIMEOUT :: TimeoutMs
tIMEOUT = 30000

init :: Socket -> IO (ProducerId, ProducerEpoch)
init sock = do
  let
    correlationId = 1
    req = InitProducerIdRequestV0 Nothing tIMEOUT

  bytes <- BL.fromStrict <$> sendAndRecv sock req correlationId

  let
    (_correlationId, initProducerIdResponse) = runGet (decoder 4) bytes

  case initProducerIdResponse of
    InitProducerIdResponseV0 _throttleTimeoutMs _errorCode producerId producerEpoch -> return (producerId, producerEpoch)
    InitProducerIdResponseV4 _throttleTimeoutMs _errorCode producerId producerEpoch -> return (producerId, producerEpoch)

  where
    decoder :: ApiVersion -> Get (CorrelationId, InitProducerIdResponse)
    decoder = decodeResponse . getInitProducerIdResponse


produce :: Socket -> TopicName -> PartitionIndex -> B.ByteString -> B.ByteString -> IO [PartitionResponse]
produce sock topicName partitionIndex recordKey recordValue = do
  timestamp <- floor <$> getPOSIXTime

  let
    msg = MessageV1 0 timestamp recordKey recordValue

    offset = 0
    messageSetItem = (offset, msg)
    recordByteString = (toStrict . toLazyByteString . messageSetToBuilder) [messageSetItem]

    -- TODO autoincremental correlationId
    correlationId = 1
    topicData = (topicName, [(partitionIndex, Just recordByteString)])
    req = ProduceRequestV1 1 tIMEOUT [topicData]

  bytes <- BL.fromStrict <$> sendAndRecv sock req correlationId

  let
    (_correlationId, produceResponses) = runGet (decoder 1) bytes
  
  return $ concatMap (\case
    ProduceResponseV0 _ partitions -> partitions
    ProduceResponseV1 _ partitions _ -> partitions
    ProduceResponseV3 _ partitions _ -> partitions) produceResponses

  where
    decoder :: ApiVersion -> Get (CorrelationId, [ProduceResponse])
    decoder = decodeResponse . getProduceResponse
