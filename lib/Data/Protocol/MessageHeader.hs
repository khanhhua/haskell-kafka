module Data.Protocol.MessageHeader where

import Data.Int (Int16, Int32)
import Data.Binary.Builder (Builder)
import Data.Binary.Get (Get, getInt32be, getInt16be, getByteString)
import Data.Protocol.NullableString (NullableString, nullableStringToBuilder)
import Data.ByteString.Builder (int16BE, int32BE)


data ApiKey
    = Produce
    | Fetch
    | ListOffsets
    | Metadata
    | LeaderAndIsr
    | StopReplica
    | UpdateMetadata
    | ControlledShutdown
    | OffsetCommit
    | OffsetFetch
    | FindCoordinator
    | JoinGroup
    | Heartbeat
    | LeaveGroup
    | SyncGroup
    | DescribeGroups
    | ListGroups
    | SaslHandshake
    | ApiVersions
    | CreateTopics
    | DeleteTopics
    | DeleteRecords
    | InitProducerId
    | OffsetForLeaderEpoch
    | AddPartitionsToTxn
    | AddOffsetsToTxn
    | EndTxn
    | WriteTxnMarkers
    | TxnOffsetCommit
    | DescribeAcls
    | CreateAcls
    | DeleteAcls
    | DescribeConfigs
    | AlterConfigs
    | AlterReplicaLogDirs
    | DescribeLogDirs
    | SaslAuthenticate
    | CreatePartitions
    | CreateDelegationToken
    | RenewDelegationToken
    | ExpireDelegationToken
    | DescribeDelegationToken
    | DeleteGroups
    | ElectLeaders
    | IncrementalAlterConfigs
    | AlterPartitionReassignments
    | ListPartitionReassignments
    | OffsetDelete
    | DescribeClientQuotas
    | AlterClientQuotas
    | DescribeUserScramCredentials
    | AlterUserScramCredentials
    | DescribeQuorum
    | AlterPartition
    | UpdateFeatures
    | DescribeCluster
    | DescribeProducers
    | UnregisterBroker
    | DescribeTransactions
    | ListTransactions
    | AllocateProducerIds
  deriving (Show)

instance Enum ApiKey where
  fromEnum Produce = 0
  fromEnum Fetch = 1
  fromEnum ListOffsets = 2
  fromEnum Metadata = 3
  fromEnum LeaderAndIsr = 4
  fromEnum StopReplica = 5
  fromEnum UpdateMetadata = 6
  fromEnum ControlledShutdown = 7
  fromEnum OffsetCommit = 8
  fromEnum OffsetFetch = 9
  fromEnum FindCoordinator = 10
  fromEnum JoinGroup = 11
  fromEnum Heartbeat = 12
  fromEnum LeaveGroup = 13
  fromEnum SyncGroup = 14
  fromEnum DescribeGroups = 15
  fromEnum ListGroups = 16
  fromEnum SaslHandshake = 17
  fromEnum ApiVersions = 18
  fromEnum CreateTopics = 19
  fromEnum DeleteTopics = 20
  fromEnum DeleteRecords = 21
  fromEnum InitProducerId = 22
  fromEnum OffsetForLeaderEpoch = 23
  fromEnum AddPartitionsToTxn = 24
  fromEnum AddOffsetsToTxn = 25
  fromEnum EndTxn = 26
  fromEnum WriteTxnMarkers = 27
  fromEnum TxnOffsetCommit = 28
  fromEnum DescribeAcls = 29
  fromEnum CreateAcls = 30
  fromEnum DeleteAcls = 31
  fromEnum DescribeConfigs = 32
  fromEnum AlterConfigs = 33
  fromEnum AlterReplicaLogDirs = 34
  fromEnum DescribeLogDirs = 35
  fromEnum SaslAuthenticate = 36
  fromEnum CreatePartitions = 37
  fromEnum CreateDelegationToken = 38
  fromEnum RenewDelegationToken = 39
  fromEnum ExpireDelegationToken = 40
  fromEnum DescribeDelegationToken = 41
  fromEnum DeleteGroups = 42
  fromEnum ElectLeaders = 43
  fromEnum IncrementalAlterConfigs = 44
  fromEnum AlterPartitionReassignments = 45
  fromEnum ListPartitionReassignments = 46
  fromEnum OffsetDelete = 47
  fromEnum DescribeClientQuotas = 48
  fromEnum AlterClientQuotas = 49
  fromEnum DescribeUserScramCredentials = 50
  fromEnum AlterUserScramCredentials = 51
  fromEnum DescribeQuorum = 55
  fromEnum AlterPartition = 56
  fromEnum UpdateFeatures = 57
  fromEnum DescribeCluster = 60
  fromEnum DescribeProducers = 61
  fromEnum UnregisterBroker = 64
  fromEnum DescribeTransactions = 65
  fromEnum ListTransactions = 66
  fromEnum AllocateProducerIds = 67

  toEnum 0 = Produce
  toEnum 1 = Fetch
  toEnum 2 = ListOffsets
  toEnum 3 = Metadata
  toEnum 4 = LeaderAndIsr
  toEnum 5 = StopReplica
  toEnum 6 = UpdateMetadata
  toEnum 7 = ControlledShutdown
  toEnum 8 = OffsetCommit
  toEnum 9 = OffsetFetch
  toEnum 10 = FindCoordinator
  toEnum 11 = JoinGroup
  toEnum 12 = Heartbeat
  toEnum 13 = LeaveGroup
  toEnum 14 = SyncGroup
  toEnum 15 = DescribeGroups
  toEnum 16 = ListGroups
  toEnum 17 = SaslHandshake
  toEnum 18 = ApiVersions
  toEnum 19 = CreateTopics
  toEnum 20 = DeleteTopics
  toEnum 21 = DeleteRecords
  toEnum 22 = InitProducerId
  toEnum 23 = OffsetForLeaderEpoch
  toEnum 24 = AddPartitionsToTxn
  toEnum 25 = AddOffsetsToTxn
  toEnum 26 = EndTxn
  toEnum 27 = WriteTxnMarkers
  toEnum 28 = TxnOffsetCommit
  toEnum 29 = DescribeAcls
  toEnum 30 = CreateAcls
  toEnum 31 = DeleteAcls
  toEnum 32 = DescribeConfigs
  toEnum 33 = AlterConfigs
  toEnum 34 = AlterReplicaLogDirs
  toEnum 35 = DescribeLogDirs
  toEnum 36 = SaslAuthenticate
  toEnum 37 = CreatePartitions
  toEnum 38 = CreateDelegationToken
  toEnum 39 = RenewDelegationToken
  toEnum 40 = ExpireDelegationToken
  toEnum 41 = DescribeDelegationToken
  toEnum 42 = DeleteGroups
  toEnum 43 = ElectLeaders
  toEnum 44 = IncrementalAlterConfigs
  toEnum 45 = AlterPartitionReassignments
  toEnum 46 = ListPartitionReassignments
  toEnum 47 = OffsetDelete
  toEnum 48 = DescribeClientQuotas
  toEnum 49 = AlterClientQuotas
  toEnum 50 = DescribeUserScramCredentials
  toEnum 51 = AlterUserScramCredentials
  toEnum 55 = DescribeQuorum
  toEnum 56 = AlterPartition
  toEnum 57 = UpdateFeatures
  toEnum 60 = DescribeCluster
  toEnum 61 = DescribeProducers
  toEnum 64 = UnregisterBroker
  toEnum 65 = DescribeTransactions
  toEnum 66 = ListTransactions
  toEnum 67 = AllocateProducerIds
  toEnum _ = undefined

apiKeyToBuilder :: ApiKey -> Builder
apiKeyToBuilder Produce = int16BE 0
apiKeyToBuilder Fetch = int16BE 1
apiKeyToBuilder ListOffsets = int16BE 2
apiKeyToBuilder Metadata = int16BE 3
apiKeyToBuilder LeaderAndIsr = int16BE 4
apiKeyToBuilder StopReplica = int16BE 5
apiKeyToBuilder UpdateMetadata = int16BE 6
apiKeyToBuilder ControlledShutdown = int16BE 7
apiKeyToBuilder OffsetCommit = int16BE 8
apiKeyToBuilder OffsetFetch = int16BE 9
apiKeyToBuilder FindCoordinator = int16BE 10
apiKeyToBuilder JoinGroup = int16BE 11
apiKeyToBuilder Heartbeat = int16BE 12
apiKeyToBuilder LeaveGroup = int16BE 13
apiKeyToBuilder SyncGroup = int16BE 14
apiKeyToBuilder DescribeGroups = int16BE 15
apiKeyToBuilder ListGroups = int16BE 16
apiKeyToBuilder SaslHandshake = int16BE 17
apiKeyToBuilder ApiVersions = int16BE 18
apiKeyToBuilder CreateTopics = int16BE 19
apiKeyToBuilder DeleteTopics = int16BE 20
apiKeyToBuilder DeleteRecords = int16BE 21
apiKeyToBuilder InitProducerId = int16BE 22
apiKeyToBuilder OffsetForLeaderEpoch = int16BE 23
apiKeyToBuilder AddPartitionsToTxn = int16BE 24
apiKeyToBuilder AddOffsetsToTxn = int16BE 25
apiKeyToBuilder EndTxn = int16BE 26
apiKeyToBuilder WriteTxnMarkers = int16BE 27
apiKeyToBuilder TxnOffsetCommit = int16BE 28
apiKeyToBuilder DescribeAcls = int16BE 29
apiKeyToBuilder CreateAcls = int16BE 30
apiKeyToBuilder DeleteAcls = int16BE 31
apiKeyToBuilder DescribeConfigs = int16BE 32
apiKeyToBuilder AlterConfigs = int16BE 33
apiKeyToBuilder AlterReplicaLogDirs = int16BE 34
apiKeyToBuilder DescribeLogDirs = int16BE 35
apiKeyToBuilder SaslAuthenticate = int16BE 36
apiKeyToBuilder CreatePartitions = int16BE 37
apiKeyToBuilder CreateDelegationToken = int16BE 38
apiKeyToBuilder RenewDelegationToken = int16BE 39
apiKeyToBuilder ExpireDelegationToken = int16BE 40
apiKeyToBuilder DescribeDelegationToken = int16BE 41
apiKeyToBuilder DeleteGroups = int16BE 42
apiKeyToBuilder ElectLeaders = int16BE 43
apiKeyToBuilder IncrementalAlterConfigs = int16BE 44
apiKeyToBuilder AlterPartitionReassignments = int16BE 45
apiKeyToBuilder ListPartitionReassignments = int16BE 46
apiKeyToBuilder OffsetDelete = int16BE 47
apiKeyToBuilder DescribeClientQuotas = int16BE 48
apiKeyToBuilder AlterClientQuotas = int16BE 49
apiKeyToBuilder DescribeUserScramCredentials = int16BE 50
apiKeyToBuilder AlterUserScramCredentials = int16BE 51
apiKeyToBuilder DescribeQuorum = int16BE 55
apiKeyToBuilder AlterPartition = int16BE 56
apiKeyToBuilder UpdateFeatures = int16BE 57
apiKeyToBuilder DescribeCluster = int16BE 60
apiKeyToBuilder DescribeProducers = int16BE 61
apiKeyToBuilder UnregisterBroker = int16BE 64
apiKeyToBuilder DescribeTransactions = int16BE 65
apiKeyToBuilder ListTransactions = int16BE 66
apiKeyToBuilder AllocateProducerIds = int16BE 67


type ApiVersion = Int16

type CorrelationId = Int32

{-| TODO: MessageHeader must be a monoid
-}
data MessageHeader
  = RequestHeaderV0 ApiKey ApiVersion CorrelationId
  | RequestHeaderV1 ApiKey ApiVersion CorrelationId NullableString


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


getMessageHeader :: Get (MessageHeader, Int)
getMessageHeader = do
  size <- getInt32be
  apiKey <- toEnum . fromIntegral <$> getInt16be
  apiVersion <- getInt16be
  correlationId <- getInt32be

  let
    messageHeader = RequestHeaderV0 apiKey apiVersion correlationId
  return (messageHeader, fromIntegral size - 64)