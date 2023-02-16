{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE TupleSections #-}
module Data.Protocol.OffsetForLeaderEpoch
  ( OffsetForLeaderEpochRequest(..)
  , OffsetForLeaderEpochResponse(..)
  , getOffsetForLeaderEpochResponse
  ) where

import Data.Protocol.Types (TopicName, PartitionIndex, ReplicaId, ErrorCode, ThrottleTimeMs, getErrorCode)
import Data.Int (Int32, Int64)
import Data.Protocol.Classes (KafkaRequest (header, body))
import Data.Protocol.ApiKey (ApiKey(OffsetForLeaderEpoch), ApiVersion)
import Data.Protocol.MessageHeader (MessageHeader(..), CorrelationId)
import Data.Protocol.Array (arrayToBuilder, getArray)
import Data.Binary.Builder (Builder)
import Data.ByteString.Builder (int32BE)
import Data.Protocol.NullableString (stringToBuilder, getString)
import Data.Binary (Get)
import Data.Binary.Get (getInt32be, getInt64be)


type LeaderEpoch = Int32
type CurrentLeaderEpoch = Int32
type EndOffset = Int64
type TopicDataV1 = (TopicName, [(PartitionIndex, LeaderEpoch)])
type TopicDataV3 = (TopicName, [(PartitionIndex, CurrentLeaderEpoch, LeaderEpoch)])


data OffsetForLeaderEpochRequest
  = OffsetForLeaderEpochRequestV1 [TopicDataV1]
  | OffsetForLeaderEpochRequestV2 [TopicDataV1]
  | OffsetForLeaderEpochRequestV3 ReplicaId [TopicDataV3]


data OffsetForLeaderEpochResponse
  = OffsetForLeaderEpochResponseV1 [(TopicName, [(ErrorCode, PartitionIndex, LeaderEpoch, EndOffset)])]
  | OffsetForLeaderEpochResponseV2 ThrottleTimeMs [(TopicName, [(ErrorCode, PartitionIndex, LeaderEpoch, EndOffset)])]
  | OffsetForLeaderEpochResponseV3 ThrottleTimeMs [(TopicName, [(ErrorCode, PartitionIndex, LeaderEpoch, EndOffset)])]


instance KafkaRequest OffsetForLeaderEpochRequest where
  header :: CorrelationId -> OffsetForLeaderEpochRequest -> MessageHeader
  header correlationId _ = RequestHeaderV1 OffsetForLeaderEpoch 1 correlationId Nothing
  body :: OffsetForLeaderEpochRequest -> Builder
  body (OffsetForLeaderEpochRequestV1 topicData) =
    arrayToBuilder topicDataToBuilder topicData
  body (OffsetForLeaderEpochRequestV2 topicData) =
    arrayToBuilder topicDataToBuilder topicData
  body (OffsetForLeaderEpochRequestV3 replicaId topicData) =
    int32BE replicaId
    <> arrayToBuilder topicDataV3ToBuilder topicData


topicDataToBuilder :: (TopicName, [(PartitionIndex, LeaderEpoch)]) -> Builder
topicDataToBuilder (topicName, partitionLeaderEpochs) =
  stringToBuilder topicName
  <> arrayToBuilder partitionLeaderEpochsToBuilder partitionLeaderEpochs
  where
    partitionLeaderEpochsToBuilder (partitionIndex, leaderEpoch) =
      int32BE partitionIndex
      <> int32BE leaderEpoch


topicDataV3ToBuilder :: (TopicName, [(PartitionIndex, CurrentLeaderEpoch, LeaderEpoch)]) -> Builder
topicDataV3ToBuilder (topicName, partitionLeaderEpochs) =
  stringToBuilder topicName
  <> arrayToBuilder partitionLeaderEpochsToBuilder partitionLeaderEpochs
  where
    partitionLeaderEpochsToBuilder (partitionIndex, currentLeaderEpoch, leaderEpoch) =
      int32BE partitionIndex
      <> int32BE currentLeaderEpoch
      <> int32BE leaderEpoch


getOffsetForLeaderEpochResponse :: ApiVersion -> Get OffsetForLeaderEpochResponse
getOffsetForLeaderEpochResponse 1 = OffsetForLeaderEpochResponseV1 <$> getArray getTopicData
getOffsetForLeaderEpochResponse 2 = do
  throttleTimeMs <- getInt32be
  OffsetForLeaderEpochResponseV2 throttleTimeMs <$> getArray getTopicData

getOffsetForLeaderEpochResponse _ = undefined

getTopicData :: Get (TopicName, [(ErrorCode, PartitionIndex, LeaderEpoch, EndOffset)])
getTopicData = do
  topicName <- getString
  (topicName, ) <$> getArray getPartitionData

getPartitionData :: Get (ErrorCode, PartitionIndex, LeaderEpoch, EndOffset)
getPartitionData = do
  errorCode <- getErrorCode
  partitionIndex <- getInt32be
  leaderEpoch <- getInt32be
  endOffset <- getInt64be

  return (errorCode, partitionIndex, leaderEpoch, endOffset)
