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
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaRequestDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LogManager.getLogger(KafkaRequestDecoder.class);


    @Override
    protected Logger log() {
        return LOGGER;
    }

    @Override
    protected KafkaFrame decodeHeaderAndBody(ByteBuf in) {
        // Read the api key and version to determine the header api version
        in.markReaderIndex();
        var apiKey = in.readShort();
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("apiKey: {} {}", apiKey, ApiKeys.forId(apiKey));
        }
        short apiVersion = in.readShort();
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("apiVersion: {}", apiVersion);
        }
        short headerVersion = ApiKeys.forId(apiKey).requestHeaderVersion(apiVersion);
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("headerVersion {}", headerVersion);
        }
        in.resetReaderIndex();

        // TODO Decide whether to decode this API at all

        ByteBufAccessor accessor = new ByteBufAccessor(in);
        var header = readHeader(headerVersion, accessor);
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("header: {}", header);
        }

        ApiMessage body = readBody(apiKey, apiVersion, accessor);
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("body {}", body);
        }
        KafkaFrame kafkaFrame = new KafkaFrame(apiVersion, header, body);
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("frame {}", kafkaFrame);
        }
        return kafkaFrame;
    }

    private RequestHeaderData readHeader(short headerVersion, ByteBufAccessor accessor) {
        return new RequestHeaderData(accessor, headerVersion);
    }

    private ApiMessage readBody(short apiKey, short apiVersion, ByteBufAccessor accessor)  {
        switch (ApiKeys.forId(apiKey)) {
            case PRODUCE:
                return new ProduceRequestData(accessor, apiVersion);
            case FETCH:
                return new FetchRequestData(accessor, apiVersion);
            case LIST_OFFSETS:
                return new ListOffsetsRequestData(accessor, apiVersion);
            case METADATA:
                return new MetadataRequestData(accessor, apiVersion);
            case LEADER_AND_ISR:
                return new LeaderAndIsrRequestData(accessor, apiVersion);
            case STOP_REPLICA:
                return new StopReplicaRequestData(accessor, apiVersion);
            case UPDATE_METADATA:
                return new UpdateMetadataRequestData(accessor, apiVersion);
            case CONTROLLED_SHUTDOWN:
                return new ControlledShutdownRequestData(accessor, apiVersion);
            case OFFSET_COMMIT:
                return new OffsetCommitRequestData(accessor, apiVersion);
            case OFFSET_FETCH:
                return new OffsetFetchRequestData(accessor, apiVersion);
            case FIND_COORDINATOR:
                return new FindCoordinatorRequestData(accessor, apiVersion);
            case JOIN_GROUP:
                return new JoinGroupRequestData(accessor, apiVersion);
            case HEARTBEAT:
                return new HeartbeatRequestData(accessor, apiVersion);
            case LEAVE_GROUP:
                return new LeaveGroupRequestData(accessor, apiVersion);
            case SYNC_GROUP:
                return new SyncGroupRequestData(accessor, apiVersion);
            case DESCRIBE_GROUPS:
                return new DescribeGroupsRequestData(accessor, apiVersion);
            case LIST_GROUPS:
                return new ListGroupsRequestData(accessor, apiVersion);
            case SASL_HANDSHAKE:
                return new SaslHandshakeRequestData(accessor, apiVersion);
            case API_VERSIONS:
                return new ApiVersionsRequestData(accessor, apiVersion);
            case CREATE_TOPICS:
                return new CreateTopicsRequestData(accessor, apiVersion);
            case DELETE_TOPICS:
                return new DeleteTopicsRequestData(accessor, apiVersion);
            case DELETE_RECORDS:
                return new DeleteRecordsRequestData(accessor, apiVersion);
            case INIT_PRODUCER_ID:
                return new InitProducerIdRequestData(accessor, apiVersion);
            case OFFSET_FOR_LEADER_EPOCH:
                return new OffsetForLeaderEpochRequestData(accessor, apiVersion);
            case ADD_PARTITIONS_TO_TXN:
                return new AddPartitionsToTxnRequestData(accessor, apiVersion);
            case ADD_OFFSETS_TO_TXN:
                return new AddOffsetsToTxnRequestData(accessor, apiVersion);
            case END_TXN:
                return new EndTxnRequestData(accessor, apiVersion);
            case WRITE_TXN_MARKERS:
                return new WriteTxnMarkersRequestData(accessor, apiVersion);
            case TXN_OFFSET_COMMIT:
                return new TxnOffsetCommitRequestData(accessor, apiVersion);
            default:
                throw new IllegalArgumentException("Unsupported API key " + apiKey);
        }
    }
}
