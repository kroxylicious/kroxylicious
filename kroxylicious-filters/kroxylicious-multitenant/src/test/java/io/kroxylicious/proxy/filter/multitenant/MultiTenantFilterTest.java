/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.flipkart.zjsonpatch.JsonDiff;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ResourceInfo;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.multitenant.config.MultiTenantConfig;
import io.kroxylicious.test.condition.kafka.FetchResponseDataCondition;
import io.kroxylicious.test.requestresponsetestdef.ApiMessageTestDef;
import io.kroxylicious.test.requestresponsetestdef.RequestResponseTestDef;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.kroxylicious.test.condition.kafka.ApiMessageCondition.forApiKey;
import static io.kroxylicious.test.condition.kafka.ProduceRequestDataCondition.produceRequestMatching;
import static io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter.requestConverterFor;
import static io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter.responseConverterFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MultiTenantFilterTest {
    private static final Pattern TEST_RESOURCE_FILTER = Pattern.compile(
            String.format("%s/.*\\.yaml", MultiTenantFilterTest.class.getPackageName().replace(".", "/")));
    private static final String TENANT_1 = "tenant1";

    private static final String VIRTUAL_CLUSTER_NAME = "vc1";
    private static final String TEST_TOPIC = "testTopic";
    @Mock
    private FilterContext filterContext;

    private static List<ResourceInfo> getTestResources() throws IOException {
        var resources = ClassPath.from(MultiTenantFilterTest.class.getClassLoader()).getResources().stream()
                .filter(ri -> TEST_RESOURCE_FILTER.matcher(ri.getResourceName()).matches()).toList();

        // https://youtrack.jetbrains.com/issue/IDEA-315462: we've seen issues in IDEA in IntelliJ Workspace Model API mode where test resources
        // don't get added to the Junit runner classpath. You can work around by not using that mode, or by adding src/test/resources to the
        // runner's classpath using 'modify classpath' option in the dialogue.
        checkState(!resources.isEmpty(), "no test resource files found on classpath matching %s", TEST_RESOURCE_FILTER);

        return resources;
    }

    private final MultiTenantFilter filter = new MultiTenantFilter(new MultiTenantConfig(null));

    private final FilterInvoker invoker = getOnlyElement(FilterAndInvoker.build(filter)).invoker();

    @Mock(strictness = LENIENT)
    private FilterContext context;

    @Captor
    private ArgumentCaptor<RequestHeaderData> requestHeaderDataCaptor;

    @Mock(strictness = LENIENT)
    private RequestFilterResult requestFilterResult;
    @Captor
    private ArgumentCaptor<ResponseHeaderData> responseHeaderDataArgumentCaptor;
    @Mock(strictness = LENIENT)
    private ResponseFilterResult responseFilterResult;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;

    @BeforeEach
    public void beforeEach() {
        when(context.getVirtualClusterName()).thenReturn(TENANT_1);
    }

    public static Stream<Arguments> requests() throws Exception {
        return RequestResponseTestDef.requestResponseTestDefinitions(getTestResources()).filter(td -> td.request() != null)
                .map(td -> Arguments.of(td.testName(), td.apiKey(), td.header(), td.request()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "requests")
    void requestsTransformed(@SuppressWarnings("unused") String testName, ApiMessageType apiMessageType, RequestHeaderData header, ApiMessageTestDef requestTestDef)
            throws Exception {
        var request = requestTestDef.message();
        // marshalled the request object back to json, this is used for the comparison later.
        var requestWriter = requestConverterFor(apiMessageType).writer();
        var marshalled = requestWriter.apply(request, header.requestApiVersion());

        when(requestFilterResult.message()).thenAnswer(invocation -> apiMessageCaptor.getValue());
        when(requestFilterResult.header()).thenAnswer(invocation -> requestHeaderDataCaptor.getValue());
        when(context.forwardRequest(requestHeaderDataCaptor.capture(), apiMessageCaptor.capture())).thenAnswer(
                invocation -> CompletableFuture.completedStage(requestFilterResult));

        var stage = invoker.onRequest(ApiKeys.forId(apiMessageType.apiKey()), header.requestApiVersion(), header, request, context);
        assertThat(stage).isCompleted();
        var forward = stage.toCompletableFuture().get().message();

        var filtered = requestWriter.apply(forward, header.requestApiVersion());
        assertEquals(requestTestDef.expectedPatch(), JsonDiff.asJson(marshalled, filtered));
    }

    public static Stream<Arguments> responses() throws Exception {
        return RequestResponseTestDef.requestResponseTestDefinitions(getTestResources()).filter(td -> td.response() != null)
                .map(td -> Arguments.of(td.testName(), td.apiKey(), td.header(), td.response()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "responses")
    void responseTransformed(@SuppressWarnings("unused") String testName, ApiMessageType apiMessageType, RequestHeaderData header, ApiMessageTestDef responseTestDef)
            throws Exception {
        var response = responseTestDef.message();
        // marshalled the response object back to json, this is used for comparison later.
        var responseWriter = responseConverterFor(apiMessageType).writer();

        var marshalled = responseWriter.apply(response, header.requestApiVersion());

        when(responseFilterResult.message()).thenAnswer(invocation -> apiMessageCaptor.getValue());
        when(responseFilterResult.header()).thenAnswer(invocation -> responseHeaderDataArgumentCaptor.getValue());
        when(context.forwardResponse(responseHeaderDataArgumentCaptor.capture(), apiMessageCaptor.capture())).thenAnswer(
                invocation -> CompletableFuture.completedStage(responseFilterResult));

        ResponseHeaderData headerData = new ResponseHeaderData();
        var stage = invoker.onResponse(ApiKeys.forId(apiMessageType.apiKey()), header.requestApiVersion(), headerData, response, context);
        assertThat(stage).isCompleted();
        var forward = stage.toCompletableFuture().get().message();

        var filtered = responseWriter.apply(forward, header.requestApiVersion());
        assertEquals(responseTestDef.expectedPatch(), JsonDiff.asJson(marshalled, filtered));
    }

    @Test
    void shouldReWriteTopic() {
        var multiTenantTransformationFilter = new MultiTenantFilter(new MultiTenantConfig(null));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn(VIRTUAL_CLUSTER_NAME);
        var request = createProduceRequest(TEST_TOPIC);
        // When
        multiTenantTransformationFilter.onProduceRequest(
                ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), request, filterContext);

        // Then
        verify(filterContext).forwardRequest(any(RequestHeaderData.class), assertArg(
                apiMessage -> assertThat(apiMessage)
                        .is(forApiKey(ApiKeys.PRODUCE))
                        .is(produceRequestMatching(produceRequestData -> produceRequestData.topicData()
                                .stream()
                                .allMatch(topicProduceData -> topicProduceData.name().equals("vc1-testTopic"))))));

    }

    @ParameterizedTest
    @ValueSource(strings = { ".", "_", "", "-.-" })
    @NullSource
    void produceWithCustomSeparator(String separator) {
        var expectedServerTopicName = "%s%s%s".formatted(VIRTUAL_CLUSTER_NAME, Objects.requireNonNullElse(separator, MultiTenantConfig.DEFAULT_SEPARATOR), TEST_TOPIC);
        var multiTenantTransformationFilter = new MultiTenantFilter(new MultiTenantConfig(separator));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn(VIRTUAL_CLUSTER_NAME);
        var request = createProduceRequest(TEST_TOPIC);
        // When
        multiTenantTransformationFilter.onProduceRequest(
                ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), request, filterContext);

        // Then
        verify(filterContext).forwardRequest(any(RequestHeaderData.class), assertArg(
                apiMessage -> assertThat(apiMessage)
                        .is(forApiKey(ApiKeys.PRODUCE))
                        .is(produceRequestMatching(produceRequestData -> produceRequestData.topicData()
                                .stream()
                                .allMatch(topicProduceData -> topicProduceData.name().equals(expectedServerTopicName))))));
    }

    @ParameterizedTest
    @ValueSource(strings = { ".", "_", "", "-.-" })
    @NullSource
    void fetchWithCustomSeparator(String separator) {
        var serverTopic = "%s%s%s".formatted(VIRTUAL_CLUSTER_NAME, Objects.requireNonNullElse(separator, MultiTenantConfig.DEFAULT_SEPARATOR), TEST_TOPIC);
        var multiTenantTransformationFilter = new MultiTenantFilter(new MultiTenantConfig(separator));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn(VIRTUAL_CLUSTER_NAME);
        var response = createFetchResponseData(serverTopic);
        // When
        multiTenantTransformationFilter.onFetchResponse(
                FetchResponseData.HIGHEST_SUPPORTED_VERSION, new ResponseHeaderData(), response, filterContext);

        // Then
        verify(filterContext).forwardResponse(any(ResponseHeaderData.class), assertArg(
                apiMessage -> assertThat(apiMessage)
                        .is(forApiKey(ApiKeys.FETCH))
                        .is(FetchResponseDataCondition.fetchResponseMatching(fetchResponseData -> fetchResponseData.responses()
                                .stream()
                                .allMatch(topicFetchData -> topicFetchData.topic().equals(TEST_TOPIC))))));
    }

    @Test
    void illegalKafkaResourceCharsInVirtualClusterNameDetected() {
        var multiTenantTransformationFilter = new MultiTenantFilter(new MultiTenantConfig(MultiTenantConfig.DEFAULT_SEPARATOR));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn("$badvcn$");
        var request = createProduceRequest(TEST_TOPIC);
        var header = new RequestHeaderData();
        // When
        assertThatThrownBy(() -> {
            multiTenantTransformationFilter.onProduceRequest(
                    ProduceRequestData.HIGHEST_SUPPORTED_VERSION, header, request, filterContext);
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void illegalKafkaResourceCharsInSeparatorDetected() {
        var multiTenantTransformationFilter = new MultiTenantFilter(new MultiTenantConfig("$bad$"));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn(VIRTUAL_CLUSTER_NAME);
        var request = createProduceRequest(TEST_TOPIC);
        var header = new RequestHeaderData();
        // When
        assertThatThrownBy(() -> {
            multiTenantTransformationFilter.onProduceRequest(
                    ProduceRequestData.HIGHEST_SUPPORTED_VERSION, header, request, filterContext);
        }).isInstanceOf(IllegalStateException.class);
    }

    private ProduceRequestData createProduceRequest(String topic) {
        final ProduceRequestData request = new ProduceRequestData();
        final ProduceRequestData.TopicProduceData topicData = new ProduceRequestData.TopicProduceData();
        topicData.setName(topic);
        request.topicData().add(topicData);
        return request;
    }

    private FetchResponseData createFetchResponseData(String topic) {
        var response = new FetchResponseData();
        var topicResponse = new FetchResponseData.FetchableTopicResponse();
        topicResponse.setTopic(topic);
        response.responses().add(topicResponse);
        return response;
    }

}
