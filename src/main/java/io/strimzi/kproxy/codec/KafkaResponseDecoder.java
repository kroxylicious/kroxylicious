/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.kproxy.codec;

import java.util.Map;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ControlledShutdownResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaResponseDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LogManager.getLogger(KafkaResponseDecoder.class);
    private final Map<Integer, KafkaRequestEncoder.VersionedApi> correlation;

    public KafkaResponseDecoder(Map<Integer, KafkaRequestEncoder.VersionedApi> correlation) {
        this.correlation = correlation;
    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

    @Override
    protected KafkaFrame decodeHeaderAndBody(ByteBuf in) {
        var correlationId = in.markReaderIndex().readInt();
        in.resetReaderIndex();
        KafkaRequestEncoder.VersionedApi versionedApi = correlation.remove(correlationId);
        if (versionedApi != null) {
            short apiKey = versionedApi.apiKey;
            short apiVersion = versionedApi.apiVersion;
            log().trace("Correlation for {} has apiKey={}, apiVersion={}",
                    correlationId, apiKey, apiVersion);
            var accessor = new ByteBufAccessor(in);
            short headerVersion = ApiKeys.forId(apiKey).responseHeaderVersion(apiVersion);
            log().trace("Header version: {}", headerVersion);
            ResponseHeaderData header = readHeader(headerVersion, accessor);
            log().trace("Header: {}", header);
            ApiMessage body = readBody(apiKey, apiVersion, accessor);
            log().trace("Body: {}", body);
            KafkaFrame kafkaFrame = new KafkaFrame(apiVersion, header, body);
            log().trace("Frame: {}", kafkaFrame);
            return kafkaFrame;
        } else {
            throw new RuntimeException("Unknown correlationId " + correlationId);
        }
    }

    private ResponseHeaderData readHeader(short headerVersion, ByteBufAccessor accessor) {
        return new ResponseHeaderData(accessor, headerVersion);
    }

    private ApiMessage readBody(short apiKey, short apiVersion, ByteBufAccessor accessor)  {
        switch (ApiKeys.forId(apiKey)) {
            case PRODUCE:
                return new ProduceResponseData(accessor, apiVersion);
            case FETCH:
                return new FetchResponseData(accessor, apiVersion);
            case LIST_OFFSETS:
                return new ListOffsetsResponseData(accessor, apiVersion);
            case METADATA:
                return new MetadataResponseData(accessor, apiVersion);
            case LEADER_AND_ISR:
                return new LeaderAndIsrResponseData(accessor, apiVersion);
            case STOP_REPLICA:
                return new StopReplicaRequestData(accessor, apiVersion);
            case UPDATE_METADATA:
                return new UpdateMetadataResponseData(accessor, apiVersion);
            case CONTROLLED_SHUTDOWN:
                return new ControlledShutdownResponseData(accessor, apiVersion);
            case OFFSET_COMMIT:
                return new OffsetCommitResponseData(accessor, apiVersion);
            case OFFSET_FETCH:
                return new OffsetFetchResponseData(accessor, apiVersion);
            case FIND_COORDINATOR:
                return new FindCoordinatorResponseData(accessor, apiVersion);
            case JOIN_GROUP:
                return new JoinGroupResponseData(accessor, apiVersion);
            case HEARTBEAT:
                return new HeartbeatResponseData(accessor, apiVersion);
            case LEAVE_GROUP:
                return new LeaveGroupResponseData(accessor, apiVersion);
            case SYNC_GROUP:
                return new SyncGroupResponseData(accessor, apiVersion);
            case DESCRIBE_GROUPS:
                return new DescribeGroupsResponseData(accessor, apiVersion);
            case LIST_GROUPS:
                return new ListGroupsResponseData(accessor, apiVersion);
            case SASL_HANDSHAKE:
                return new SaslHandshakeResponseData(accessor, apiVersion);
            case API_VERSIONS:
                return new ApiVersionsResponseData(accessor, apiVersion);
            case CREATE_TOPICS:
                return new CreateTopicsResponseData(accessor, apiVersion);
            case DELETE_TOPICS:
                return new DeleteTopicsResponseData(accessor, apiVersion);
            case DELETE_RECORDS:
                return new DeleteRecordsResponseData(accessor, apiVersion);
            case INIT_PRODUCER_ID:
                return new InitProducerIdResponseData(accessor, apiVersion);
            case OFFSET_FOR_LEADER_EPOCH:
                return new OffsetForLeaderEpochResponseData(accessor, apiVersion);
            case ADD_PARTITIONS_TO_TXN:
                return new AddPartitionsToTxnResponseData(accessor, apiVersion);
            case ADD_OFFSETS_TO_TXN:
                return new AddOffsetsToTxnResponseData(accessor, apiVersion);
            case END_TXN:
                return new EndTxnResponseData(accessor, apiVersion);
            case WRITE_TXN_MARKERS:
                return new WriteTxnMarkersResponseData(accessor, apiVersion);
            case TXN_OFFSET_COMMIT: // ???
                return new TxnOffsetCommitResponseData(accessor, apiVersion);
            default:
                throw new IllegalArgumentException("Unsupported API key " + apiKey);
        }
    }
}
