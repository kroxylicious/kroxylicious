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

import java.util.List;
import java.util.Map;

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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.strimzi.kproxy.api.filter.KrpcRequestFilter;
import io.strimzi.kproxy.api.filter.KrpcResponseFilter;

public class KafkaRequestDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LogManager.getLogger(KafkaRequestDecoder.class);

    private final List<KrpcRequestFilter> requestFilterHandlers;
    private final List<KrpcResponseFilter> responseFilterHandlers;
    private final Map<Integer, Correlation> correlation;

    public KafkaRequestDecoder(List<KrpcRequestFilter> requestFilterHandlers,
                               List<KrpcResponseFilter> responseFilterHandlers,
                               Map<Integer, Correlation> correlation) {
        super();
        this.requestFilterHandlers = requestFilterHandlers;
        this.responseFilterHandlers = responseFilterHandlers;
        this.correlation = correlation;
    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

    @Override
    protected Frame decodeHeaderAndBody(ChannelHandlerContext ctx, ByteBuf in, final int length) {
        // Read the api key and version to determine the header api version
        final int sof = in.readerIndex();
        var apiId = in.readShort();
        // TODO handle unknown api key
        ApiKeys apiKey = ApiKeys.forId(apiId);
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("{}: apiKey: {} {}", ctx, apiId, apiKey);
        }
        short apiVersion = in.readShort();
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("{}: apiVersion: {}", ctx, apiVersion);
        }

        RequestHeaderData header = null;
        final ByteBufAccessor accessor;
        // build the list of filters and use the emptiness rather than booleans
        var decodeRequest = shouldDecodeRequest(apiKey, apiVersion);
        boolean decodeResponse = shouldDecodeResponse(apiKey, apiVersion);
        log().trace("{}: decodeRequest {}, decodeResponse {}", ctx, decodeRequest, decodeResponse);
        if (decodeRequest || decodeResponse) {
            short headerVersion = apiKey.requestHeaderVersion(apiVersion);
            if (log().isTraceEnabled()) { // avoid boxing
                log().trace("{}: headerVersion {}", ctx, headerVersion);
            }
            in.readerIndex(sof);

            // TODO Decide whether to decode this API at all
            // TODO Can we implement ApiMessage using an opaque wrapper around a bytebuf?

            accessor = new ByteBufAccessor(in);
            header = readHeader(headerVersion, accessor);
            if (log().isTraceEnabled()) {
                log().trace("{}: header: {}", ctx, header);
            }
        }
        else {
            accessor = null;
        }
        final Frame frame;
        if (decodeRequest) {
            ApiMessage body = readBody(apiId, apiVersion, accessor);
            if (log().isTraceEnabled()) {
                log().trace("{}: body {}", ctx, body);
            }
            frame = new DecodedRequestFrame<>(apiVersion, header, body);
            if (log().isTraceEnabled()) {
                log().trace("{}: frame {}", ctx, frame);
            }
        }
        else {
            in.readerIndex(sof);
            frame = opaqueFrame(in, length);
            in.readerIndex(sof + length);
        }
        if (decodeResponse) {
            if (decodeRequest &&
                    apiKey == ApiKeys.PRODUCE &&
                    ((ProduceRequestData) ((DecodedRequestFrame<?>) frame).body).acks() == 0) {
                // If we know it's an acks=0 PRODUCE then we know there will be no response
                // so don't correlate. Shame we can only issue this warning if we decoded the request
                log().warn("{}: Not honouring decode of acks=0 PRODUCE response, because there will be none", ctx);
            }
            else {
                correlation.put(header.correlationId(), new Correlation(apiKey, apiVersion, true));
            }
        }
        return frame;
    }

    private boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
        for (var f : responseFilterHandlers) {
            if (f.shouldDeserializeResponse(apiKey, apiVersion)) {
                return true;
            }
        }
        return false;
    }

    private boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
        for (var f : requestFilterHandlers) {
            if (f.shouldDeserializeRequest(apiKey, apiVersion)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected OpaqueFrame opaqueFrame(ByteBuf in, int length) {
        return new OpaqueRequestFrame(in.readSlice(length).retain(), length);
    }

    private RequestHeaderData readHeader(short headerVersion, ByteBufAccessor accessor) {
        return new RequestHeaderData(accessor, headerVersion);
    }

    private ApiMessage readBody(short apiKey, short apiVersion, ByteBufAccessor accessor) {
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
