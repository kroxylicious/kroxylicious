/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

import javax.net.ssl.SSLContext;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;

import static io.kroxylicious.proxy.filter.TlsTestFilter.Outcome.FAILED_TO_CREATE_SSL_CONTEXT;
import static io.kroxylicious.proxy.filter.TlsTestFilter.Outcome.SEND_EXCEPTION;
import static io.kroxylicious.proxy.filter.TlsTestFilter.Outcome.SUCCESS;
import static io.kroxylicious.proxy.filter.TlsTestFilter.Outcome.UNEXPECTED_RESPONSE;

public class TlsTestFilter implements RequestFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TlsTestFilter.class);
    public static final int INIT_SEND_SUCCESS_TAG = 50;
    public static final int INSTANCE_SEND_SUCCESS_TAG = 51;

    public enum Outcome {
        FAILED_TO_CREATE_SSL_CONTEXT,
        SUCCESS,
        UNEXPECTED_RESPONSE,
        SEND_EXCEPTION
    }

    private final Outcome initResult;
    private final Outcome instanceResult;

    public TlsTestFilter(TlsTestFilterConfig config, FilterFactoryContext initContext, FilterFactoryContext instanceContext) {
        this.initResult = getSslContextAndRequest(initContext, config);
        this.instanceResult = getSslContextAndRequest(instanceContext, config);
    }

    private static Outcome getSslContextAndRequest(FilterFactoryContext context, TlsTestFilterConfig config) {
        SSLContext sslContext;
        try {
            sslContext = context.clientSslContext(config.tls);
        }
        catch (Exception e) {
            LOGGER.warn("exception obtaining ssl context {}", e.getMessage());
            LOGGER.debug("exception obtaining ssl context", e);
            return FAILED_TO_CREATE_SSL_CONTEXT;
        }
        HttpClient client = getHttpClient(sslContext);
        HttpRequest request = HttpRequest.newBuilder().uri(config.endpointUri()).build();
        Outcome result;
        try {
            HttpResponse<String> send = client.send(request, HttpResponse.BodyHandlers.ofString());
            result = send.statusCode() == 200 ? SUCCESS : UNEXPECTED_RESPONSE;
        }
        catch (Exception e) {
            LOGGER.warn("exception cause while sending http request, {}", request);
            LOGGER.debug("exception cause while sending http request, {}", request, e);
            result = SEND_EXCEPTION;
        }
        return result;
    }

    private static HttpClient getHttpClient(SSLContext sslContext) {
        return HttpClient.newBuilder().sslContext(sslContext).connectTimeout(Duration.ofSeconds(2)).build();
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        request.unknownTaggedFields().add(new RawTaggedField(INIT_SEND_SUCCESS_TAG, initResult.name().getBytes(StandardCharsets.UTF_8)));
        request.unknownTaggedFields().add(new RawTaggedField(INSTANCE_SEND_SUCCESS_TAG, instanceResult.name().getBytes(StandardCharsets.UTF_8)));
        return context.forwardRequest(header, request);
    }

    public record TlsTestFilterConfig(String endpoint, Tls tls) {
        @JsonCreator
        public TlsTestFilterConfig(@JsonProperty(value = "endpoint", required = true) String endpoint,
                                   @JsonProperty(value = "tls", required = true) Tls tls) {
            this.endpoint = endpoint;
            this.tls = tls;
        }

        public URI endpointUri() {
            return URI.create(endpoint);
        }
    }

}
