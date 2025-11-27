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
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;
import io.kroxylicious.proxy.internal.filter.ApiVersionsDowngradeFilter;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaRequestDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRequestDecoder.class);
    private static final int CURRENT_PRODUCE_REQUEST_VERSION = 13;

    private final DecodePredicate decodePredicate;

    private final ApiVersionsServiceImpl apiVersionsService;

    public KafkaRequestDecoder(DecodePredicate decodePredicate,
                               int socketFrameMaxSize,
                               ApiVersionsServiceImpl apiVersionsService,
                               @Nullable KafkaMessageListener listener) {
        super(socketFrameMaxSize, listener);
        this.decodePredicate = decodePredicate;
        this.apiVersionsService = apiVersionsService;
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
        ApiKeys apiKey = readApiKey(ctx, apiId);
        short apiVersion = readApiVersion(ctx, in);
        final int startOfMessage = in.readerIndex();
        int correlationId = in.readInt();
        LOGGER.debug("{}: {} downstream correlation id: {}", ctx, apiKey, correlationId);

        var decodeRequest = decodePredicate.shouldDecodeRequest(apiKey, apiVersion);

        LOGGER.debug("Decode {}/v{} request? {}, Predicate {} ", apiKey, apiVersion, decodeRequest, decodePredicate);
        boolean decodeResponse = decodePredicate.shouldDecodeResponse(apiKey, apiVersion);
        LOGGER.debug("Decode {}/v{} response? {}, Predicate {}", apiKey, apiVersion, decodeResponse, decodePredicate);
        short headerVersion = apiKey.requestHeaderVersion(apiVersion);

        final RequestFrame frame;
        if (decodeRequest) {
            short highestProxyVersion = apiVersionsService.latestVersion(apiKey);
            boolean clientAheadOfProxy = apiVersion > highestProxyVersion;
            if (clientAheadOfProxy) {
                if (apiKey == ApiKeys.API_VERSIONS) {
                    return createV0ApiVersionRequestFrame(ctx, correlationId);
                }
                else {
                    log().error("{}: apiVersion {} for {} ahead of proxy maximum: {}", ctx, apiVersion, apiKey, highestProxyVersion);
                    throw new IllegalStateException("client apiVersion " + apiVersion + " ahead of proxy maximum " + highestProxyVersion + " for api key: " + apiKey);
                }
            }
            DecodedBufer result = decodeRequest(ctx, in, headerVersion, sof);
            ApiMessage body = BodyDecoder.decodeRequest(apiKey, apiVersion, result.accessor());
            if (log().isTraceEnabled()) {
                log().trace("{}: body {}", ctx, body);
            }

            frame = new DecodedRequestFrame<>(apiVersion, correlationId, decodeResponse, result.header(), body);
            if (log().isTraceEnabled()) {
                log().trace("{}: frame {}", ctx, frame);
            }
        }
        else {
            boolean hasResponse = requiresResponse(in, apiKey, startOfMessage, apiVersion);
            in.readerIndex(sof);
            frame = opaqueFrame(in, apiId, apiVersion, correlationId, decodeResponse, length, hasResponse);
            in.readerIndex(sof + length);
        }
        return frame;
    }

    private static boolean requiresResponse(ByteBuf in, ApiKeys apiKey, int startOfMessage, short apiVersion) {
        boolean hasResponse = true;
        if (apiKey == ApiKeys.PRODUCE) {
            short acks = readAcks(in, startOfMessage, apiKey.id, apiVersion);
            hasResponse = acks != 0;
        }
        return hasResponse;
    }

    private DecodedBufer decodeRequest(ChannelHandlerContext ctx, ByteBuf in, short headerVersion, int sof) {
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("{}: headerVersion {}", ctx, headerVersion);
        }
        in.readerIndex(sof);

        // TODO Decide whether to decode this API at all
        // TODO Can we implement ApiMessage using an opaque wrapper around a bytebuf?

        final ByteBufAccessorImpl accessor = new ByteBufAccessorImpl(in);
        RequestHeaderData header = readHeader(headerVersion, accessor);
        if (log().isTraceEnabled()) {
            log().trace("{}: header: {}", ctx, header);
        }
        return new DecodedBufer(header, accessor);
    }

    private record DecodedBufer(RequestHeaderData header, ByteBufAccessorImpl accessor) {}

    private short readApiVersion(ChannelHandlerContext ctx, ByteBuf in) {
        short apiVersion = in.readShort();
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("{}: apiVersion: {}", ctx, apiVersion);
        }
        return apiVersion;
    }

    private ApiKeys readApiKey(ChannelHandlerContext ctx, short apiId) {
        ApiKeys apiKey = ApiKeys.forId(apiId);
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("{}: apiKey: {} {}", ctx, apiId, apiKey);
        }
        return apiKey;
    }

    private DecodedRequestFrame<ApiVersionsRequestData> createV0ApiVersionRequestFrame(ChannelHandlerContext ctx,
                                                                                       int correlationId) {
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("{}: downgrading apiVersion request to v0", ctx);
        }
        return ApiVersionsDowngradeFilter.downgradeApiVersionsFrame(correlationId);
    }

    private static void incrementReaderIndex(ByteBuf byteBuf, int increment) {
        byteBuf.readerIndex(byteBuf.readerIndex() + increment);
    }

    static short readAcks(ByteBuf in, int startOfMessage, short apiKey, short apiVersion) {
        if (apiVersion > CURRENT_PRODUCE_REQUEST_VERSION) {
            // we must test that we can read acks out of new versions and that all variations of preceding fields are skipped
            throw new AssertionError("Unsupported Produce version: " + apiVersion);
        }
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
            if (clientIdLength > 0) {
                incrementReaderIndex(in, clientIdLength);
            }
        }
        if (headerVersion >= 2) {
            int numTaggedFields = ByteBufAccessorImpl.readUnsignedVarint(in);
            for (int i = 0; i < numTaggedFields; i++) {
                ByteBufAccessorImpl.readUnsignedVarint(in);
                int size = ByteBufAccessorImpl.readUnsignedVarint(in);
                incrementReaderIndex(in, size);
            }
        }
        if (headerVersion >= 3) {
            // we must test that the new header version can be skipped, handling any new fields or other modifications
            throw new AssertionError("Unsupported Produce header version: " + headerVersion);
        }

        final short acks;
        if (apiVersion >= 3) { // Transactional id comes before acks
            int transactionIdLength;
            if (apiVersion < 9) { // Last non-flexible version
                short nullableStringLength = in.readShort();
                transactionIdLength = nullableStringLength == -1 ? 0 : nullableStringLength;
            }
            else { // Flexible versions
                transactionIdLength = ByteBufAccessorImpl.readUnsignedVarint(in);
            }
            incrementReaderIndex(in, transactionIdLength);
        }
        acks = in.readShort();
        return acks;
    }

    private OpaqueRequestFrame opaqueFrame(ByteBuf in,
                                           short apiKey,
                                           short apiVersion,
                                           int correlationId,
                                           boolean decodeResponse,
                                           int length,
                                           boolean hasResponse) {
        return new OpaqueRequestFrame(
                in.readSlice(length).retain(),
                apiKey,
                apiVersion,
                correlationId,
                decodeResponse,
                length,
                hasResponse);
    }

    private RequestHeaderData readHeader(short headerVersion, Readable accessor) {
        return new RequestHeaderData(accessor, headerVersion);
    }

}
