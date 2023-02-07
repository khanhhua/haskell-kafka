module Data.Protocol where

import Data.Binary.Builder (toLazyByteString)
import Data.Binary (encode)
import Data.Binary.Get
    ( getInt16be, getInt32be, getByteString, Get )

import Data.ByteString (ByteString, length)

import Prelude hiding (length)
import Data.Protocol.MessageHeader (MessageHeader(..), headerToBuilder, CorrelationId)
import Data.Protocol.Classes
import Data.ByteString.Lazy (toStrict)


{-|
Consumer of Request is responsible for generating network friendly bytestring
in two steps:
- Construct the builders for header and body
- and apply them to the function as parameters
-}
encodeMessage :: KafkaRequest a => a -> CorrelationId -> ByteString
encodeMessage message correlationId =
  let
    headerBs = (toStrict . toLazyByteString . headerToBuilder . header correlationId) message
    bodyBs = (toStrict . toLazyByteString . body) message
    payload = headerBs <> bodyBs
    size = length payload
  in (toStrict . encode) size <> payload


decodeHeader :: Get (MessageHeader, Int)
decodeHeader = do
  size <- getInt32be
  apiKey <- toEnum . fromIntegral <$> getInt16be
  apiVersion <- getInt16be
  correlationId <- getInt32be

  let
    messageHeader = RequestHeaderV0 apiKey apiVersion correlationId 
  return (messageHeader, fromIntegral size - 64)

