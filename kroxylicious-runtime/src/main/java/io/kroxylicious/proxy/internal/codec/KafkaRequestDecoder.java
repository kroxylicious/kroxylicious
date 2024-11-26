/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Readable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.util.Metrics;

import edu.umd.cs.findbugs.annotations.NonNull;

public class KafkaRequestDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRequestDecoder.class);
    public static final String CLIENT_AHEAD_OF_PROXY = "CLIENT_AHEAD_OF_PROXY";

    private final DecodePredicate decodePredicate;

    public KafkaRequestDecoder(DecodePredicate decodePredicate, int socketFrameMaxSize) {
        super(socketFrameMaxSize);
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
        final int startOfMessage = in.readerIndex();
        int correlationId = in.readInt();
        LOGGER.debug("{}: {} downstream correlation id: {}", ctx, apiKey, correlationId);
        RequestHeaderData header = null;
        final ByteBufAccessorImpl accessor;
        Metrics.inboundDownstreamMessagesCounter().increment();
        var decodeRequest = decodePredicate.shouldDecodeRequest(apiKey, apiVersion);
        LOGGER.debug("Decode {}/v{} request? {}, Predicate {} ", apiKey, apiVersion, decodeRequest, decodePredicate);
        boolean decodeResponse = decodePredicate.shouldDecodeResponse(apiKey, apiVersion);
        LOGGER.debug("Decode {}/v{} response? {}, Predicate {}", apiKey, apiVersion, decodeResponse, decodePredicate);
        short headerVersion = apiKey.requestHeaderVersion(apiVersion);
        if (decodeRequest) {
            Metrics.inboundDownstreamDecodedMessagesCounter().increment();
            Metrics.payloadSizeBytesUpstreamSummary(apiKey, apiVersion).record(length);
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
            short highestProxyVersion = apiKey.latestVersion(true);
            boolean clientAheadOfProxy = apiVersion > highestProxyVersion;
            if (clientAheadOfProxy) {
                if (apiKey == ApiKeys.API_VERSIONS) {
                    return createV0ApiVersionRequestFrame(ctx, correlationId);
                }
                else {
                    log().error("{}: apiVersion {} for {} ahead of proxy maximum: {}", ctx, apiVersion, apiKey, highestProxyVersion);
                    throw new IllegalStateException("client apiVersion " + apiVersion + "  ahead of proxy maximum " + highestProxyVersion + " for api key: " + apiKey);
                }
            }
            ApiMessage body = BodyDecoder.decodeRequest(apiKey, apiVersion, accessor);
            if (log().isTraceEnabled()) {
                log().trace("{}: body {}", ctx, body);
            }

            frame = new DecodedRequestFrame<>(apiVersion, correlationId, decodeResponse, header, body);
            if (log().isTraceEnabled()) {
                log().trace("{}: frame {}", ctx, frame);
            }
        }
        else {
            boolean hasResponse = true;
            if (apiKey == ApiKeys.PRODUCE) {
                short acks = readAcks(in, startOfMessage, apiKey.id, apiVersion);
                hasResponse = acks != 0;
            }
            in.readerIndex(sof);
            frame = opaqueFrame(in, correlationId, decodeResponse, length, hasResponse);
            in.readerIndex(sof + length);
        }
        return frame;
    }

    // we make the smallest assumption we can that the message starts with apiKey + apiVersion + correlationId
    private @NonNull DecodedRequestFrame<ApiVersionsRequestData> createV0ApiVersionRequestFrame(ChannelHandlerContext ctx,
                                                                                                int correlationId) {
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("{}: downgrading apiVersion request to v0", ctx);
        }
        RequestHeaderData requestHeaderData = new RequestHeaderData();
        requestHeaderData.setCorrelationId(correlationId);
        requestHeaderData.setRequestApiKey(ApiKeys.API_VERSIONS.id);
        requestHeaderData.setRequestApiVersion((short) 0);
        ApiVersionsRequestData apiVersionsRequestData = new ApiVersionsRequestData();
        DecodedRequestFrame<ApiVersionsRequestData> frame = new DecodedRequestFrame<>(
                (short) 0, correlationId, true, requestHeaderData, apiVersionsRequestData);
        // additional state to correlate the response and downgrade to v0 if required
        frame.requestResponseState().putState(CLIENT_AHEAD_OF_PROXY, true);
        return frame;
    }

    private static void incrementReaderIndex(ByteBuf byteBuf, int increment) {
        byteBuf.readerIndex(byteBuf.readerIndex() + increment);
    }

    static short readAcks(ByteBuf in, int startOfMessage, short apiKey, short apiVersion) {
        // Annoying case: we need to know whether to expect a response so that we know
        // whether to add to the correlation (so that, in turn, we know how to rewrite the correlation
        // id of the client response).
        // Adding ack-less Produce requests to the correlation => OOME.
        // This requires decoding at least the first one or two
        // fields of all Produce requests.
        // Because we want to avoid parsing the produce request using ProduceRequestData
        // just for this we are stuck with hand coding deserialization code...
        in.readerIndex(startOfMessage);
        short headerVersion = ApiKeys.forId(apiKey).requestHeaderVersion(apiVersion);
        incrementReaderIndex(in, 4);
        if (headerVersion >= 1) {
            int clientIdLength = in.readShort();
            incrementReaderIndex(in, clientIdLength);
        }
        if (headerVersion >= 2) {
            int numTaggedFields = ByteBufAccessorImpl.readUnsignedVarint(in);
            for (int i = 0; i < numTaggedFields; i++) {
                ByteBufAccessorImpl.readUnsignedVarint(in);
                int size = ByteBufAccessorImpl.readUnsignedVarint(in);
                incrementReaderIndex(in, size);
            }
        }

        final short acks;
        if (apiVersion >= 3) { // Transactional id comes before acks
            int transactionIdLength;
            if (apiVersion < 9) { // Last non-flexible version
                transactionIdLength = in.readShort();
            }
            else if (apiVersion <= 11) { // Flexible versions
                transactionIdLength = ByteBufAccessorImpl.readUnsignedVarint(in);
            }
            else {
                throw new AssertionError("Unsupported Produce apiVersion: " + apiVersion);
            }
            incrementReaderIndex(in, transactionIdLength);
        }
        acks = in.readShort();
        return acks;
    }

    private OpaqueRequestFrame opaqueFrame(ByteBuf in,
                                           int correlationId,
                                           boolean decodeResponse,
                                           int length,
                                           boolean hasResponse) {
        return new OpaqueRequestFrame(
                in.readSlice(length).retain(),
                correlationId,
                decodeResponse,
                length,
                hasResponse);
    }

    private RequestHeaderData readHeader(short headerVersion, Readable accessor) {
        return new RequestHeaderData(accessor, headerVersion);
    }

}
