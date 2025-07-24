/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.security.cert.X509Certificate;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsRequestDataJsonConverter;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseDataJsonConverter;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestDataJsonConverter;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseDataJsonConverter;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestDataJsonConverter;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseDataJsonConverter;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestDataJsonConverter;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseDataJsonConverter;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestDataJsonConverter;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseDataJsonConverter;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestDataJsonConverter;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseDataJsonConverter;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateRequestDataJsonConverter;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslAuthenticateResponseDataJsonConverter;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestDataJsonConverter;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseDataJsonConverter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

public class AuditLoggerFilter implements RequestFilter, ResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditLoggerFilter.class);
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final EnumMap<ApiKeys, Boolean> loggableRequests = new EnumMap<>(ApiKeys.class);
    private final EnumMap<ApiKeys, Boolean> loggableResponses = new EnumMap<>(ApiKeys.class);
    {
        for (var key : ApiKeys.values()) {
            loggableRequests.put(key, true);
            loggableResponses.put(key, true);
        }
    }

    final long initMillis;
    final long initNanos;
    final Map<Integer, Short> requestVersions;

    public AuditLoggerFilter() {
        initMillis = System.currentTimeMillis();
        initNanos = System.nanoTime();
        requestVersions = new HashMap<>();
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        Boolean logRequest = loggableRequests.get(apiKey);
        if (logRequest != null) {
            var object = JsonNodeFactory.instance.objectNode();
            long nanosSinceInit = System.nanoTime() - initNanos;
            object.put("nanosSinceInit", nanosSinceInit);
            object.put("currentTimeMillis", currentTimeMillis(nanosSinceInit));
            object.put("channelDescriptor", context.channelDescriptor());
            // TODO ^^ context.channelDescriptor() just contains the info about the connection to server, not the connection from client
            // TODO netty has a ChannelId which we should probably use??
            object.put("clientCertSig", context.clientTlsContext().flatMap(ClientTlsContext::clientCertificate)
                    .map(X509Certificate::getSignature).orElse(null));
            object.put("clientId", header.clientId());
            object.put("correlationId", header.correlationId());
            object.put("apiKey", apiKey.name);
            if (logRequest) {
                // TODO don't do this for SaslAuthenticate, to avoid logging private credentials
                object.set("request", requestJson(apiKey, header.requestApiVersion(), request, false));
            }
            log(object);
        }
        if (loggableResponses.get(apiKey)) {
            requestVersions.put(header.correlationId(), header.requestApiVersion());
        }

        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        Boolean logResponse = loggableResponses.get(apiKey);
        if (logResponse != null) {
            var object = JsonNodeFactory.instance.objectNode();
            long nanosSinceInit = System.nanoTime() - initNanos;
            object.put("nanosSinceInit", nanosSinceInit);
            object.put("currentTimeMillis", currentTimeMillis(nanosSinceInit));
            // object.put("channelDescriptor", context.channelDescriptor());
            object.put("clientCertSig", context.clientTlsContext().flatMap(ClientTlsContext::clientCertificate)
                    .map(X509Certificate::getSignature).orElse(null));
            // TODO object.put("serverPrincipal", serverPrincipal.getName());
            object.put("correlationId", header.correlationId());
            object.put("apiKey", apiKey.name);
            if (logResponse) {
                short requestApiVersion = requestVersions.remove(header.correlationId());
                // TODO don't do this for SaslAuthenticate, to avoid logging private credentials
                object.set("response", responseJson(apiKey, requestApiVersion, response, false));
            }
            log(object);
        }
        return context.forwardResponse(header, response);
    }

    @Nullable
    private static JsonNode requestJson(ApiKeys apiKey, short requestApiVersion, ApiMessage request, boolean serializeRecords) {
        return switch (apiKey) {
            // TODO generate a method to do this using the krpc plugin
            case API_VERSIONS -> ApiVersionsRequestDataJsonConverter.write((ApiVersionsRequestData) request, requestApiVersion, serializeRecords);
            case METADATA -> MetadataRequestDataJsonConverter.write((MetadataRequestData) request, requestApiVersion, serializeRecords);
            case SASL_HANDSHAKE -> SaslHandshakeRequestDataJsonConverter.write((SaslHandshakeRequestData) request, requestApiVersion, serializeRecords);
            case SASL_AUTHENTICATE -> SaslAuthenticateRequestDataJsonConverter.write((SaslAuthenticateRequestData) request, requestApiVersion, serializeRecords);
            case PRODUCE -> ProduceRequestDataJsonConverter.write((ProduceRequestData) request, requestApiVersion, serializeRecords);
            case INIT_PRODUCER_ID -> InitProducerIdRequestDataJsonConverter.write((InitProducerIdRequestData) request, requestApiVersion, serializeRecords);
            case FIND_COORDINATOR -> FindCoordinatorRequestDataJsonConverter.write((FindCoordinatorRequestData) request, requestApiVersion, serializeRecords);
            case FETCH -> FetchRequestDataJsonConverter.write((FetchRequestData) request, requestApiVersion, serializeRecords);
            default -> null;
        };
    }

    @Nullable
    private static JsonNode responseJson(ApiKeys apiKey, short requestApiVersion, ApiMessage response, boolean serializeRecords) {
        return switch (apiKey) {
            // TODO generate a method to do this using the krpc plugin
            case API_VERSIONS -> ApiVersionsResponseDataJsonConverter.write((ApiVersionsResponseData) response, requestApiVersion, serializeRecords);
            case METADATA -> MetadataResponseDataJsonConverter.write((MetadataResponseData) response, requestApiVersion, serializeRecords);
            case SASL_HANDSHAKE -> SaslHandshakeResponseDataJsonConverter.write((SaslHandshakeResponseData) response, requestApiVersion, serializeRecords);
            case SASL_AUTHENTICATE -> SaslAuthenticateResponseDataJsonConverter.write((SaslAuthenticateResponseData) response, requestApiVersion, serializeRecords);
            case PRODUCE -> ProduceResponseDataJsonConverter.write((ProduceResponseData) response, requestApiVersion, serializeRecords);
            case INIT_PRODUCER_ID -> InitProducerIdResponseDataJsonConverter.write((InitProducerIdResponseData) response, requestApiVersion, serializeRecords);
            case FIND_COORDINATOR -> FindCoordinatorResponseDataJsonConverter.write((FindCoordinatorResponseData) response, requestApiVersion, serializeRecords);
            case FETCH -> FetchResponseDataJsonConverter.write((FetchResponseData) response, requestApiVersion, serializeRecords);
            default -> null;
        };
    }

    private long currentTimeMillis(long nanosSinceInit) {
        // Using nanosSinceInit means that on machines where
        // System.nanoTime() ticks more frequently than System.currentTimeMillis()
        // we will report times with a higher resolution than System.currentTimeMillis()
        // even if that time is not strictly accurate
        return initMillis + (nanosSinceInit / 1_000_000);
    }

    private static void log(ObjectNode object) {
        // TODO abstract this, so we can log to a Kafka topic.
        try {
            var json = OBJECT_MAPPER.writeValueAsString(object);
            LOGGER.info(json);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
