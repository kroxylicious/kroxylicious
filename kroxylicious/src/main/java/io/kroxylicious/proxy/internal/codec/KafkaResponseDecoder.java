/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

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
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Readable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.OpaqueFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.internal.InternalResponseFrame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class KafkaResponseDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaResponseDecoder.class);

    private final CorrelationManager correlationManager;

    public KafkaResponseDecoder(CorrelationManager correlationManager) {
        super();
        this.correlationManager = correlationManager;
    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

    @Override
    protected Frame decodeHeaderAndBody(ChannelHandlerContext ctx, ByteBuf in, int length) {
        var wi = in.writerIndex();
        var ri = in.readerIndex();
        var upstreamCorrelationId = in.readInt();
        in.readerIndex(ri);

        CorrelationManager.Correlation correlation = this.correlationManager.getBrokerCorrelation(upstreamCorrelationId);
        if (correlation == null) {
            throw new AssertionError("Missing correlation id " + upstreamCorrelationId);
        }
        else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Recovered correlation {} for upstream correlation id {}", ctx, correlation, upstreamCorrelationId);
        }
        int correlationId = correlation.downstreamCorrelationId();
        in.writerIndex(ri);
        in.writeInt(correlationId);
        in.writerIndex(wi);

        final Frame frame;
        if (correlation.decodeResponse()) {
            ApiKeys apiKey = ApiKeys.forId(correlation.apiKey());
            short apiVersion = correlation.apiVersion();
            var accessor = new ByteBufAccessorImpl(in);
            short headerVersion = apiKey.responseHeaderVersion(apiVersion);
            log().trace("{}: Header version: {}", ctx, headerVersion);
            ResponseHeaderData header = readHeader(headerVersion, accessor);
            log().trace("{}: Header: {}", ctx, header);
            ApiMessage body = readBody(apiKey, apiVersion, accessor);
            log().trace("{}: Body: {}", ctx, body);
            KrpcFilter recipient = correlation.recipient();
            if (recipient == null) {
                frame = new DecodedResponseFrame<>(apiVersion, correlationId, header, body);
            }
            else {
                frame = new InternalResponseFrame<>(recipient, correlation.promise(), apiVersion, correlationId, header, body);
            }
        }
        else {
            frame = opaqueFrame(in, correlationId, length);
        }
        log().trace("{}: Frame: {}", ctx, frame);
        return frame;
    }

    private OpaqueFrame opaqueFrame(ByteBuf in, int correlationId, int length) {
        return new OpaqueResponseFrame(in.readSlice(length).retain(), correlationId, length);
    }

    private ResponseHeaderData readHeader(short headerVersion, Readable accessor) {
        return new ResponseHeaderData(accessor, headerVersion);
    }

    private ApiMessage readBody(ApiKeys apiKey, short apiVersion, Readable accessor) {
        switch (apiKey) {
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
            case SASL_AUTHENTICATE:
                return new SaslAuthenticateRequestData(accessor, apiVersion);
            default:
                throw new IllegalArgumentException("Unsupported API key " + apiKey);
        }
    }
}
