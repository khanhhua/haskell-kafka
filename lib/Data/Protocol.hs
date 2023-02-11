module Data.Protocol where

import Data.Binary.Builder (toLazyByteString)
import Data.Binary (encode)
import Data.Binary.Get
    ( Get
    , getInt32be
    )

import Prelude hiding (length)
import Data.Protocol.MessageHeader (headerToBuilder, CorrelationId)
import Data.Protocol.Classes
import qualified Data.ByteString.Lazy as B
import Data.ByteString.Builder (int32BE)

{-|
Consumer of Request is responsible for generating network friendly bytestring
in two steps:
- Construct the builders for header and body
- and apply them to the function as parameters
-}
encodeMessage :: KafkaRequest a => a -> CorrelationId -> B.ByteString
encodeMessage message correlationId =
  let
    headerBs = (toLazyByteString . headerToBuilder . header correlationId) message
    bodyBs = (toLazyByteString . body) message
    payload = headerBs <> bodyBs
    size = (toLazyByteString . int32BE . fromIntegral . B.length) payload
  in size <> payload


decodeResponse :: Get a -> Get (CorrelationId, a)
decodeResponse responseBodyDecoder = do
  _size <- getInt32be
  correlationId <- getInt32be
  response <- responseBodyDecoder

  return (correlationId, response)

