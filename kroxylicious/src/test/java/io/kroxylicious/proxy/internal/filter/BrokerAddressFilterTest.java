/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.io.IOException;
import java.util.List;
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
import org.mockito.junit.jupiter.MockitoExtension;

import com.flipkart.zjsonpatch.JsonDiff;
import com.google.common.reflect.ClassPath;

import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.FilterInvokers;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.requestresponsetestdef.ApiMessageTestDef;
import io.kroxylicious.test.requestresponsetestdef.RequestResponseTestDef;

import static com.google.common.base.Preconditions.checkState;
import static io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter.responseConverterFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    private VirtualCluster virtualCluster;

    @Mock
    private KrpcFilterContext context;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;

    private BrokerAddressFilter filter;

    private FilterInvoker invoker;

    public static Stream<Arguments> responses() throws Exception {
        return RequestResponseTestDef.requestResponseTestDefinitions(getTestResources()).filter(td -> td.response() != null)
                .map(td -> Arguments.of(td.testName(), td.apiKey(), td.header(), td.response()));
    }

    @BeforeEach
    public void beforeEach() {
        filter = new BrokerAddressFilter(virtualCluster);
        invoker = FilterInvokers.from(filter);
        when(virtualCluster.getBrokerAddress(0)).thenReturn(HostPort.parse("downstream:19199"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "responses")
    void responseTransformed(@SuppressWarnings("unused") String testName, ApiMessageType apiMessageType, RequestHeaderData header, ApiMessageTestDef responseTestDef) {
        var response = responseTestDef.message();
        // marshalled the response object back to json, this is used for comparison later.
        var responseWriter = responseConverterFor(apiMessageType).writer();

        var marshalled = responseWriter.apply(response, header.requestApiVersion());

        ResponseHeaderData headerData = new ResponseHeaderData();
        invoker.onResponse(ApiKeys.forId(apiMessageType.apiKey()), header.requestApiVersion(), headerData, response, context);
        verify(context).forwardResponse(any(), apiMessageCaptor.capture());

        var filtered = responseWriter.apply(apiMessageCaptor.getValue(), header.requestApiVersion());
        assertEquals(responseTestDef.expectedPatch(), JsonDiff.asJson(marshalled, filtered));
    }

}
