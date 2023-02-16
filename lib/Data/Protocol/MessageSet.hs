module Data.Protocol.MessageSet where

import Data.Int (Int64, Int32, Int8)
import qualified Data.ByteString as B
import Data.Binary.Builder (Builder, toLazyByteString, empty)
import Data.ByteString.Builder
  ( int64BE
  , int32BE
  , int8
  , byteString, int16BE
  )

import Data.Digest.CRC32 (crc32)

import Data.Protocol.NullableByte (nullableBytesToBuilder)


type Offset = Int64
type MessageSize = Int32
type Crc = Int32
type MagicByte = Int8
type Attributes = Int8
type TimestampMs = Int64
type Key = B.ByteString
type Value = B.ByteString
data Message
    = MessageV0 Attributes Key Value
    | MessageV1 Attributes TimestampMs Key Value
type MessageSetItem = (Offset, Message)

type MessageSet = [MessageSetItem]

mAGIC_BYTE_V0 = 0
mAGIC_BYTE_V1 = 1

messageSetToBuilder :: MessageSet -> Builder
messageSetToBuilder =
  mconcat . map messageSetItemToBuilder


messageSetItemToBuilder :: MessageSetItem -> Builder
messageSetItemToBuilder (offset, message) =
  int64BE offset
  <> messageToBuilder message

messageToBuilder :: Message -> Builder
messageToBuilder (MessageV0 attributes key value) =
  let
    payloadBuilder =
      int8 mAGIC_BYTE_V0
      <> int8 attributes
      <> nullableBytesToBuilder (Just key)
      <> nullableBytesToBuilder (Just value)
    payloadBs = (B.toStrict . toLazyByteString) payloadBuilder
    crc = crc32 payloadBs
    messageSize = B.length payloadBs + 4
  in
    (int32BE . fromIntegral) messageSize
    <> (int32BE . fromIntegral) crc
    <> payloadBuilder

messageToBuilder (MessageV1 attributes timestampMs key value) =
  let
    payloadBuilder =
      int8 mAGIC_BYTE_V1
      <> int8 attributes
      <> int64BE timestampMs
      <> nullableBytesToBuilder (Just key)
      <> nullableBytesToBuilder (Just value)
    payloadBs = (B.toStrict . toLazyByteString) payloadBuilder
    crc = crc32 payloadBs
    messageSize = B.length payloadBs + 4
  in
    (int32BE . fromIntegral) messageSize
    <> (int32BE . fromIntegral) crc
    <> payloadBuilder