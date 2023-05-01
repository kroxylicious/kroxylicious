/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.zjsonpatch.JsonDiff;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ResourceInfo;

import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.FilterInvokers;
import io.kroxylicious.proxy.filter.KrpcFilterContext;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.kroxylicious.proxy.filter.multitenant.KafkaApiMessageConverter.requestConverterFor;
import static io.kroxylicious.proxy.filter.multitenant.KafkaApiMessageConverter.responseConverterFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MultiTenantTransformationFilterTest {
    private static final Pattern TEST_RESOURCE_FILTER = Pattern.compile(
            String.format("%s/.*\\.yaml", MultiTenantTransformationFilterTest.class.getPackageName().replace(".", "/")));
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private record ApiMessageTestDef(ApiMessage message, JsonNode expectedPatch) {
    }

    private record RequestResponseTestDef(String testName, ApiMessageType apiKey, RequestHeaderData header, ApiMessageTestDef request, ApiMessageTestDef response) {
    }

    private static Stream<RequestResponseTestDef> requestResponseTestDefinitions() throws Exception {
        var resourceInfoStream = ClassPath.from(MultiTenantTransformationFilterTest.class.getClassLoader()).getResources().stream()
                .filter(ri -> TEST_RESOURCE_FILTER.matcher(ri.getResourceName()).matches()).toList();

        // https://youtrack.jetbrains.com/issue/IDEA-315462: we've seen issues in IDEA in IntelliJ Workspace Model API mode where test resources
        // don't get added to the Junit runner classpath. You can work around by not using that mode, or by adding src/test/resources to the
        // runner's classpath using 'modify classpath' option in the dialogue.
        checkState(!resourceInfoStream.isEmpty(), "no test resource files found on classpath matching %s", TEST_RESOURCE_FILTER);

        return resourceInfoStream.stream()
                .map(MultiTenantTransformationFilterTest::readTestResource)
                .flatMap(Arrays::stream)
                .filter(Predicate.not(td -> td.get("disabled").asBoolean(false)))
                .map(MultiTenantTransformationFilterTest::buildRequestResponseTestDef);
    }

    private static JsonNode[] readTestResource(ResourceInfo resourceInfo) {
        try {
            var sourceFile = Path.of(resourceInfo.url().getPath()).getFileName().toString();
            var jsonNodes = MAPPER.reader().readValue(resourceInfo.url(), JsonNode[].class);
            for (int i = 0; i < jsonNodes.length; i++) {
                if (jsonNodes[i] instanceof ObjectNode objectNode) {
                    objectNode.put("source", String.format("%s[%d]", sourceFile, i));
                }
            }
            return jsonNodes;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static RequestResponseTestDef buildRequestResponseTestDef(JsonNode jsonNode) {
        var sourceNode = jsonNode.get("source");
        var descriptionNode = jsonNode.get("description");
        var apiMessageTypeNode = jsonNode.get("apiMessageType");
        var versionNode = jsonNode.get("version");
        checkNotNull(sourceNode, "source missing from test definition");
        var source = sourceNode.textValue();
        checkNotNull(apiMessageTypeNode, "apiMessageType missing from test definition: %s", source);
        checkNotNull(versionNode, "version missing from test definition: %s", source);
        var messageType = ApiMessageType.valueOf(apiMessageTypeNode.asText());
        var header = new RequestHeaderData().setRequestApiKey(messageType.apiKey()).setRequestApiVersion(versionNode.shortValue());

        var testName = String.format("%s,%s,v%d[%s]", source, messageType, versionNode.intValue(),
                Optional.ofNullable(descriptionNode).map(JsonNode::asText).orElse("-"));
        var requestConverter = requestConverterFor(messageType);
        var responseConverter = responseConverterFor(messageType);
        var request = buildApiMessageTestDef(header.requestApiVersion(), jsonNode.get("request"), requestConverter.reader());
        var response = buildApiMessageTestDef(header.requestApiVersion(), jsonNode.get("response"), responseConverter.reader());
        return new RequestResponseTestDef(testName, messageType, header, request, response);
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

    private final FilterInvoker invoker = FilterInvokers.from(filter);

    private final KrpcFilterContext context = mock(KrpcFilterContext.class);

    private final ArgumentCaptor<ApiMessage> apiMessageCaptor = ArgumentCaptor.forClass(ApiMessage.class);

    @BeforeEach
    public void beforeEach() {
        when(context.sniHostname()).thenReturn("tenant1.kafka.example.com");
    }

    public static Stream<Arguments> requests() throws Exception {
        return requestResponseTestDefinitions().filter(td -> td.request() != null).map(td -> Arguments.of(td.testName(), td.apiKey(), td.header(), td.request()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "requests")
    void requestsTransformed(@SuppressWarnings("unused") String testName, ApiMessageType apiMessageType, RequestHeaderData header, ApiMessageTestDef requestTestDef) {
        var request = requestTestDef.message();
        // marshalled the request object back to json, this is used for the comparison later.
        var requestWriter = requestConverterFor(apiMessageType).writer();
        var marshalled = requestWriter.apply(request, header.requestApiVersion());

        invoker.onRequest(ApiKeys.forId(apiMessageType.apiKey()), header, request, context);
        verify(context).forwardRequest(apiMessageCaptor.capture());

        var filtered = requestWriter.apply(apiMessageCaptor.getValue(), header.requestApiVersion());
        assertEquals(requestTestDef.expectedPatch(), JsonDiff.asJson(marshalled, filtered));
    }

    public static Stream<Arguments> responses() throws Exception {
        return requestResponseTestDefinitions().filter(td -> td.response() != null).map(td -> Arguments.of(td.testName(), td.apiKey(), td.header(), td.response()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "responses")
    void responseTransformed(@SuppressWarnings("unused") String testName, ApiMessageType apiMessageType, RequestHeaderData header, ApiMessageTestDef responseTestDef) {
        var response = responseTestDef.message();
        // marshalled the response object back to json, this is used for comparison later.
        var responseWriter = responseConverterFor(apiMessageType).writer();

        var marshalled = responseWriter.apply(response, header.requestApiVersion());

        invoker.onResponse(ApiKeys.forId(apiMessageType.apiKey()), apiMessageType.highestSupportedVersion(), new ResponseHeaderData(), response, context);
        verify(context).forwardResponse(apiMessageCaptor.capture());

        var filtered = responseWriter.apply(apiMessageCaptor.getValue(), header.requestApiVersion());
        assertEquals(responseTestDef.expectedPatch(), JsonDiff.asJson(marshalled, filtered));
    }
}
