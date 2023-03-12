/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestDataJsonConverter;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseDataJsonConverter;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestDataJsonConverter;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseDataJsonConverter;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestDataJsonConverter;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseDataJsonConverter;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestDataJsonConverter;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseDataJsonConverter;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestDataJsonConverter;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseDataJsonConverter;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestDataJsonConverter;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseDataJsonConverter;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestDataJsonConverter;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseDataJsonConverter;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestDataJsonConverter;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseDataJsonConverter;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestDataJsonConverter;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseDataJsonConverter;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.zjsonpatch.JsonDiff;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ResourceInfo;

import io.kroxylicious.proxy.filter.KrpcFilterContext;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MultiTenantTransformationFilterTest {
    private static final Pattern TEST_RESOURCE_FILTER = Pattern.compile(
            String.format("%s/.*\\.yaml", MultiTenantTransformationFilterTest.class.getPackageName().replace(".", "/")));
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private record ApiMessageTestDef(ApiMessage message, JsonNode expectedPatch) {
    }

    private record RequestResponseTestDef(ApiMessageType apiKey, RequestHeaderData header, ApiMessageTestDef request, ApiMessageTestDef response) {
    }

    private record Converters(BiFunction<JsonNode, Short, ApiMessage> requestReader,
                              BiFunction<JsonNode, Short, ApiMessage> responseReader,
                              BiFunction<ApiMessage, Short, JsonNode> requestWriter,
                              BiFunction<ApiMessage, Short, JsonNode> responseWriters) {
    }

    private static final Map<ApiMessageType, Converters> converters = Map.of(
            ApiMessageType.CREATE_TOPICS, new Converters(
                    CreateTopicsRequestDataJsonConverter::read,
                    CreateTopicsResponseDataJsonConverter::read,
                    (o, ver) -> CreateTopicsRequestDataJsonConverter.write(((CreateTopicsRequestData) o), ver),
                    (o, ver) -> CreateTopicsResponseDataJsonConverter.write(((CreateTopicsResponseData) o), ver)),
            ApiMessageType.DELETE_TOPICS, new Converters(
                    DeleteTopicsRequestDataJsonConverter::read,
                    DeleteTopicsResponseDataJsonConverter::read,
                    (o, ver) -> DeleteTopicsRequestDataJsonConverter.write(((DeleteTopicsRequestData) o), ver),
                    (o, ver) -> DeleteTopicsResponseDataJsonConverter.write(((DeleteTopicsResponseData) o), ver)),
            ApiMessageType.METADATA, new Converters(
                    MetadataRequestDataJsonConverter::read,
                    MetadataResponseDataJsonConverter::read,
                    (o, ver) -> MetadataRequestDataJsonConverter.write(((MetadataRequestData) o), ver),
                    (o, ver) -> MetadataResponseDataJsonConverter.write(((MetadataResponseData) o), ver)),
            ApiMessageType.PRODUCE, new Converters(
                    ProduceRequestDataJsonConverter::read,
                    ProduceResponseDataJsonConverter::read,
                    (o, ver) -> ProduceRequestDataJsonConverter.write(((ProduceRequestData) o), ver),
                    (o, ver) -> ProduceResponseDataJsonConverter.write(((ProduceResponseData) o), ver)),
            ApiMessageType.LIST_OFFSETS, new Converters(
                    ListOffsetsRequestDataJsonConverter::read,
                    ListOffsetsResponseDataJsonConverter::read,
                    (o, ver) -> ListOffsetsRequestDataJsonConverter.write(((ListOffsetsRequestData) o), ver),
                    (o, ver) -> ListOffsetsResponseDataJsonConverter.write(((ListOffsetsResponseData) o), ver)),
            ApiMessageType.OFFSET_FETCH, new Converters(
                    OffsetFetchRequestDataJsonConverter::read,
                    OffsetFetchResponseDataJsonConverter::read,
                    (o, ver) -> OffsetFetchRequestDataJsonConverter.write(((OffsetFetchRequestData) o), ver),
                    (o, ver) -> OffsetFetchResponseDataJsonConverter.write(((OffsetFetchResponseData) o), ver)),
            ApiMessageType.OFFSET_FOR_LEADER_EPOCH, new Converters(
                    OffsetForLeaderEpochRequestDataJsonConverter::read,
                    OffsetForLeaderEpochResponseDataJsonConverter::read,
                    (o, ver) -> OffsetForLeaderEpochRequestDataJsonConverter.write(((OffsetForLeaderEpochRequestData) o), ver),
                    (o, ver) -> OffsetForLeaderEpochResponseDataJsonConverter.write(((OffsetForLeaderEpochResponseData) o), ver)),
            ApiMessageType.OFFSET_COMMIT, new Converters(
                    OffsetCommitRequestDataJsonConverter::read,
                    OffsetCommitResponseDataJsonConverter::read,
                    (o, ver) -> OffsetCommitRequestDataJsonConverter.write(((OffsetCommitRequestData) o), ver),
                    (o, ver) -> OffsetCommitResponseDataJsonConverter.write(((OffsetCommitResponseData) o), ver)),
            ApiMessageType.FETCH, new Converters(
                    FetchRequestDataJsonConverter::read,
                    FetchResponseDataJsonConverter::read,
                    (o, ver) -> FetchRequestDataJsonConverter.write(((FetchRequestData) o), ver),
                    (o, ver) -> FetchResponseDataJsonConverter.write(((FetchResponseData) o), ver)));

    private static Stream<RequestResponseTestDef> requestResponseTestDefinitions() throws Exception {
        return ClassPath.from(MultiTenantTransformationFilterTest.class.getClassLoader()).getResources().stream()
                .filter(ri -> TEST_RESOURCE_FILTER.matcher(ri.getResourceName()).matches())
                .map(MultiTenantTransformationFilterTest::readTestResource)
                .flatMap(Arrays::stream)
                .filter(Predicate.not(td -> td.get("disabled").asBoolean(false)))
                .map(MultiTenantTransformationFilterTest::buildRequestResponseTestDef);
    }

    private static JsonNode[] readTestResource(ResourceInfo resourceInfo) {
        try {
            return MAPPER.reader().readValue(resourceInfo.url(), JsonNode[].class);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static RequestResponseTestDef buildRequestResponseTestDef(JsonNode jsonNode) {
        var apiMessageType = jsonNode.get("apiMessageType");
        var version = jsonNode.get("version");
        assertNotNull(apiMessageType);
        assertNotNull(version);
        var messageType = ApiMessageType.valueOf(apiMessageType.asText());
        var header = new RequestHeaderData().setRequestApiKey(messageType.apiKey()).setRequestApiVersion(version.shortValue());

        var conv = converters.get(messageType);
        var request = buildApiMessageTestDef(header.requestApiVersion(), jsonNode.get("request"), conv.requestReader());
        var response = buildApiMessageTestDef(header.requestApiVersion(), jsonNode.get("response"), conv.responseReader());
        return new RequestResponseTestDef(messageType, header, request, response);
    }

    private static ApiMessageTestDef buildApiMessageTestDef(short apiVersion, JsonNode node, BiFunction<JsonNode, Short, ApiMessage> readerFunc) {
        if (node != null) {
            var request = readerFunc.apply(node.get("payload"), apiVersion);
            var requestDiff = node.get("diff");
            return new ApiMessageTestDef(request, requestDiff);
        }
        return null;
    }

    private final MultiTenantTransformationFilter filter = new MultiTenantTransformationFilter();

    private final KrpcFilterContext context = mock(KrpcFilterContext.class);

    private final ArgumentCaptor<ApiMessage> apiMessageCaptor = ArgumentCaptor.forClass(ApiMessage.class);

    @BeforeEach
    public void beforeEach() {
        when(context.sniHostname()).thenReturn("tenant1.kafka.example.com");
    }

    public static Stream<Arguments> requests() throws Exception {
        return requestResponseTestDefinitions().map(t -> Arguments.of(t.apiKey(), t.header(), t.request()));
    }

    @ParameterizedTest
    @MethodSource
    void requests(ApiMessageType apiMessageType, RequestHeaderData header, ApiMessageTestDef requestTestDef) {
        var request = requestTestDef.message();
        // marshalled the request object back to json, this is used for the comparison later.
        var requestWriter = converters.get(apiMessageType).requestWriter();
        var marshalled = requestWriter.apply(request, header.requestApiVersion());

        filter.onRequest(ApiKeys.forId(apiMessageType.apiKey()), header, request, context);
        verify(context).forwardRequest(apiMessageCaptor.capture());

        var filtered = requestWriter.apply(apiMessageCaptor.getValue(), header.requestApiVersion());
        assertEquals(requestTestDef.expectedPatch(), JsonDiff.asJson(marshalled, filtered));
    }

    public static Stream<Arguments> responses() throws Exception {
        return requestResponseTestDefinitions().map(t -> Arguments.of(t.apiKey(), t.header(), t.response()));
    }

    @ParameterizedTest
    @MethodSource
    void responses(ApiMessageType apiMessageType, RequestHeaderData header, ApiMessageTestDef responseTestDef) {
        var response = responseTestDef.message();
        // marshalled the response object back to json, this is used for comparison later.
        var responseWriter = converters.get(apiMessageType).responseWriters();

        var marshalled = responseWriter.apply(response, header.requestApiVersion());

        filter.onResponse(ApiKeys.forId(apiMessageType.apiKey()), new ResponseHeaderData(), response, context);
        verify(context).forwardResponse(apiMessageCaptor.capture());

        var filtered = responseWriter.apply(apiMessageCaptor.getValue(), header.requestApiVersion());
        assertEquals(responseTestDef.expectedPatch(), JsonDiff.asJson(marshalled, filtered));
    }
}
