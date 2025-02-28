/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.flipkart.zjsonpatch.JsonDiff;
import com.google.common.reflect.ClassPath;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.model.VirtualClusterModel.VirtualClusterGatewayModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.requestresponsetestdef.ApiMessageTestDef;
import io.kroxylicious.test.requestresponsetestdef.RequestResponseTestDef;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter.responseConverterFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BrokerAddressFilterTest {

    private static final Pattern TEST_RESOURCE_FILTER = Pattern.compile(
            String.format("%s/.*\\.yaml", BrokerAddressFilterTest.class.getPackageName().replace(".", "/")));

    private static List<ClassPath.ResourceInfo> getTestResources() throws IOException {
        var resources = ClassPath.from(BrokerAddressFilterTest.class.getClassLoader()).getResources().stream()
                .filter(ri -> TEST_RESOURCE_FILTER.matcher(ri.getResourceName()).matches()).toList();

        // https://youtrack.jetbrains.com/issue/IDEA-315462: we've seen issues in IDEA in IntelliJ Workspace Model API mode where test resources
        // don't get added to the Junit runner classpath. You can work around by not using that mode, or by adding src/test/resources to the
        // runner's classpath using 'modify classpath' option in the dialogue.
        checkState(!resources.isEmpty(), "no test resource files found on classpath matching %s", TEST_RESOURCE_FILTER);

        return resources;
    }

    @Mock
    private VirtualClusterGatewayModel virtualClusterListenerModel;

    @Mock
    private EndpointReconciler endpointReconciler;

    @Mock
    private FilterContext context;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;

    @Captor
    private ArgumentCaptor<ResponseHeaderData> responseHeaderDataCaptor;

    private BrokerAddressFilter filter;

    private FilterInvoker invoker;

    public static Stream<Arguments> nodeInfoCarryingResponses() throws Exception {
        return responses(td -> td.response() != null);
    }

    public static Stream<Arguments> completeClusterInfoCarryingResponses() throws Exception {
        return responses(td -> td.response() != null && Set.of(ApiMessageType.METADATA, ApiMessageType.DESCRIBE_CLUSTER).contains(td.apiKey()));
    }

    private static Stream<Arguments> responses(Predicate<RequestResponseTestDef> requestResponseTestDefPredicate) throws Exception {
        return RequestResponseTestDef.requestResponseTestDefinitions(getTestResources()).filter(requestResponseTestDefPredicate)
                .map(td -> Arguments.of(td.testName(), td.apiKey(), td.header(), td.response()));
    }

    @BeforeEach
    public void beforeEach() {
        filter = new BrokerAddressFilter(virtualClusterListenerModel, endpointReconciler);
        invoker = getOnlyElement(FilterAndInvoker.build(filter)).invoker();
        lenient().when(virtualClusterListenerModel.getBrokerAddress(0)).thenReturn(HostPort.parse("downstream:19199"));
        lenient().when(virtualClusterListenerModel.getAdvertisedBrokerAddress(0)).thenReturn(HostPort.parse("downstream:19200"));

        var nodeMap = Map.of(0, HostPort.parse("upstream:9199"));
        lenient().when(endpointReconciler.reconcile(Mockito.eq(virtualClusterListenerModel), Mockito.eq(nodeMap)))
                .thenReturn(CompletableFuture.completedStage(null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "nodeInfoCarryingResponses")
    void nodeInfoCarryingResponsesTransformed(@SuppressWarnings("unused") String testName, ApiMessageType apiMessageType, RequestHeaderData header,
                                              ApiMessageTestDef responseTestDef)
            throws Exception {
        filterResponseAndVerify(apiMessageType, header, responseTestDef);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "completeClusterInfoCarryingResponses")
    void reconcileCachesUpstreamAddress(@SuppressWarnings("unused") String testName, ApiMessageType apiMessageType, RequestHeaderData header,
                                        ApiMessageTestDef responseTestDef)
            throws Exception {

        filterResponseAndVerify(apiMessageType, header, responseTestDef);
        verify(endpointReconciler, times(1)).reconcile(Mockito.eq(virtualClusterListenerModel), Mockito.anyMap());
    }

    private void filterResponseAndVerify(ApiMessageType apiMessageType, RequestHeaderData header, ApiMessageTestDef responseTestDef) throws Exception {
        var response = responseTestDef.message();
        // marshalled the response object back to json, this is used for comparison later.
        var responseWriter = responseConverterFor(apiMessageType).writer();

        var marshalled = responseWriter.apply(response, header.requestApiVersion());

        configureContextResponseStubbing();

        ResponseHeaderData headerData = new ResponseHeaderData();
        var stage = invoker.onResponse(ApiKeys.forId(apiMessageType.apiKey()), header.requestApiVersion(), headerData, response, context);
        assertThat(stage).isCompleted();
        var value = stage.toCompletableFuture().get().message();

        var filtered = responseWriter.apply(value, header.requestApiVersion());
        assertThat(JsonDiff.asJson(marshalled, filtered)).isEqualTo(responseTestDef.expectedPatch());
    }

    private void configureContextResponseStubbing() {
        lenient().when(context.responseFilterResultBuilder()).thenReturn(new ResponseFilterResultBuilderImpl());
        lenient().when(context.forwardResponse(responseHeaderDataCaptor.capture(), apiMessageCaptor.capture()))
                .thenAnswer((x) -> new ResponseFilterResultBuilderImpl().forward(responseHeaderDataCaptor.getValue(), apiMessageCaptor.getValue()).completed());
    }
}
