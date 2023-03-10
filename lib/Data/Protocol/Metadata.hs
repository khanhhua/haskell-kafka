{-# LANGUAGE InstanceSigs #-}
module Data.Protocol.Metadata where

import Data.Int (Int32)
import Data.ByteString.Builder (int8)
import Data.Binary.Builder (Builder)

import Data.Protocol.Types (TopicName, ErrorCode, PartitionIndex, getErrorCode)
import Data.Protocol.Classes (KafkaRequest (header, body))
import Data.Protocol.MessageHeader (CorrelationId, MessageHeader (RequestHeaderV1))
import Data.Protocol.ApiKey (ApiKey(Metadata), ApiVersion)
import Data.Protocol.Array (arrayToBuilder, getArray)
import Data.Protocol.NullableString (stringToBuilder, getString)
import Data.Binary (Get)
import Data.Binary.Get (getInt32be)


type AllowAutoTopicCreation = Bool

type NodeId = Int32
type Host = String
type Port = Int32

type Broker = (NodeId, Host, Port)

type LeaderId = Int32
type ReplicaNodeId = Int32
type IsrNodeId = Int32
type Partition = (ErrorCode, PartitionIndex, LeaderId, [ReplicaNodeId], [IsrNodeId])
type Topic = (ErrorCode, TopicName, [Partition])
type ControllerId = Int32

data MetadataRequest
  = MetadataRequestV0 [TopicName]
  | MetadataRequestV4 [TopicName] AllowAutoTopicCreation
  deriving Show

data MetadataResponse
  = MetadataResponseV0 [Broker] [Topic]
  | MetadataResponseV1 [Broker] ControllerId [Topic]
  deriving Show

instance KafkaRequest MetadataRequest where
  header :: CorrelationId -> MetadataRequest -> MessageHeader
  header correlationId (MetadataRequestV0 {}) =
    RequestHeaderV1 Metadata 1 correlationId Nothing
  header correlationId _ =
    RequestHeaderV1 Metadata 1 correlationId Nothing
  body :: MetadataRequest -> Builder
  body = toBuilder


toBuilder :: MetadataRequest -> Builder
toBuilder (MetadataRequestV0 topicNames) =
  arrayToBuilder stringToBuilder topicNames
toBuilder (MetadataRequestV4 topicNames allowAutoTopicCreation) =
  arrayToBuilder stringToBuilder topicNames
  <> int8 (if allowAutoTopicCreation then 1 else 0)


getMetadataResponse :: ApiVersion -> Get MetadataResponse
getMetadataResponse 0 = do
  brokers <- getArray getBroker
  topics <- getArray getTopic

  return $ MetadataResponseV0 brokers topics

getMetadataResponse 1 = do
  brokers <- getArray getBroker
  controllerId <- getInt32be
  topics <- getArray getTopic

  return $ MetadataResponseV1 brokers controllerId topics

getMetadataResponse _ = undefined

getBroker :: Get Broker
getBroker = do
  nodeId <- getInt32be
  host <- getString
  port <- getInt32be

  return (nodeId, host, port)

getTopic :: Get Topic
getTopic = do
  errorCode <- getErrorCode
  topicName <- getString
  partitions <- getArray getPartition

  return (errorCode, topicName, partitions)

getPartition :: Get Partition
getPartition = do
  errorCode <- getErrorCode
  partitionIndex <- getInt32be
  leaderId <- getInt32be
  replicaNodeIds <- getArray getInt32be
  isrNodeIds <- getArray getInt32be

  return (errorCode, partitionIndex, leaderId, replicaNodeIds, isrNodeIds)
