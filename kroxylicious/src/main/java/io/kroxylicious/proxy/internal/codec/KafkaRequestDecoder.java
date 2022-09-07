/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

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
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Readable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class KafkaRequestDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRequestDecoder.class);

    private final DecodePredicate decodePredicate;

    public KafkaRequestDecoder(DecodePredicate decodePredicate) {
        super();
        this.decodePredicate = decodePredicate;
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
        int correlationId = in.readInt();
        LOGGER.debug("{}: {} downstream correlation id: {}", ctx, apiKey, correlationId);

        RequestHeaderData header = null;
        final ByteBufAccessorImpl accessor;
        var decodeRequest = decodePredicate.shouldDecodeRequest(apiKey, apiVersion);
        LOGGER.debug("Decode {}/v{} request? {}, Predicate {} ", apiKey, apiVersion, decodeRequest, decodePredicate);
        boolean decodeResponse = decodePredicate.shouldDecodeResponse(apiKey, apiVersion);
        LOGGER.debug("Decode {}/v{} response? {}, Predicate {}", apiKey, apiVersion, decodeResponse, decodePredicate);
        short headerVersion = apiKey.requestHeaderVersion(apiVersion);
        if (decodeRequest) {
            if (log().isTraceEnabled()) { // avoid boxing
                log().trace("{}: headerVersion {}", ctx, headerVersion);
            }
            in.readerIndex(sof);

            // TODO Decide whether to decode this API at all
            // TODO Can we implement ApiMessage using an opaque wrapper around a bytebuf?

            accessor = new ByteBufAccessorImpl(in);
            header = readHeader(headerVersion, accessor);
            if (log().isTraceEnabled()) {
                log().trace("{}: header: {}", ctx, header);
            }
        }
        else {
            accessor = null;
        }
        final RequestFrame frame;
        if (decodeRequest) {
            ApiMessage body = readBody(apiId, apiVersion, accessor);
            if (log().isTraceEnabled()) {
                log().trace("{}: body {}", ctx, body);
            }

            frame = new DecodedRequestFrame<ApiMessage>(apiVersion, correlationId, decodeResponse, header, body);
            if (log().isTraceEnabled()) {
                log().trace("{}: frame {}", ctx, frame);
            }
        }
        else {
            in.readerIndex(sof);
            frame = opaqueFrame(in, correlationId, decodeResponse, length);
            in.readerIndex(sof + length);
        }

        return frame;
    }

    private OpaqueRequestFrame opaqueFrame(ByteBuf in,
                                           int correlationId,
                                           boolean decodeResponse,
                                           int length) {
        return new OpaqueRequestFrame(
                in.readSlice(length).retain(),
                correlationId,
                decodeResponse,
                length);
    }

    private RequestHeaderData readHeader(short headerVersion, Readable accessor) {
        return new RequestHeaderData(accessor, headerVersion);
    }

    private ApiMessage readBody(short apiKey, short apiVersion, Readable accessor) {
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
            case SASL_AUTHENTICATE:
                return new SaslAuthenticateRequestData(accessor, apiVersion);
            default:
                throw new IllegalArgumentException("Unsupported API key " + apiKey);
        }
    }
}
