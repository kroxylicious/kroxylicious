/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.channel.local.LocalChannel;

import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.KafkaSession;
import io.kroxylicious.proxy.internal.KafkaSessionState;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.subject.DefaultSubjectBuilder;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.router.Response;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.NodeIdentificationStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RouteFilterPipelineTest {

    private static final short API_VERSION = 12;
    private static final String ROUTE_NAME = "test-route";
    private static final int CORRELATION_ID = -500;

    private DefaultEventLoopGroup eventLoopGroup;
    private EventLoop eventLoop;
    private LocalChannel inboundChannel;
    private ClientConnectionStateMachine ccsm;
    private RouteFilterPipeline pipeline;

    @BeforeEach
    void setUp() throws Exception {
        eventLoopGroup = new DefaultEventLoopGroup(1);
        eventLoop = eventLoopGroup.next();
        inboundChannel = new LocalChannel();
        eventLoop.register(inboundChannel).sync();
        ccsm = createCcsm();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (pipeline != null) {
            eventLoop.submit(() -> pipeline.close()).sync();
        }
        inboundChannel.close().sync();
        eventLoopGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).sync();
    }

    @Test
    void requestShouldPassThroughFilterAndReachForwarder() throws Exception {
        var filter = new ClientIdStampingFilter("stamped-client");
        pipeline = createPipeline(filter);

        var future = new CompletableFuture<Response>();
        var forwarded = new AtomicReference<Object>();
        var frame = fetchRequestFrame(CORRELATION_ID);

        eventLoop.submit(() -> pipeline.writeRequest(frame, future, forwarded::set)).sync();

        assertThat(forwarded.get()).isInstanceOf(DecodedRequestFrame.class);
        var forwardedFrame = (DecodedRequestFrame<?>) forwarded.get();
        assertThat(forwardedFrame.header().clientId()).isEqualTo("stamped-client");
    }

    @Test
    void responseShouldPassThroughFilterAndCompleteFuture() throws Exception {
        var filter = new ErrorCodeStampingFilter((short) 42);
        pipeline = createPipeline(filter);

        var future = new CompletableFuture<Response>();
        var frame = fetchRequestFrame(CORRELATION_ID);

        eventLoop.submit(() -> {
            pipeline.writeRequest(frame, future, forwarded -> {
                pipeline.writeResponse(
                        new ResponseHeaderData().setCorrelationId(CORRELATION_ID),
                        new FetchResponseData(),
                        CORRELATION_ID,
                        API_VERSION);
            });
        }).sync();

        assertThat(future).succeedsWithin(Duration.ofSeconds(5));
        Response response = future.get();
        assertThat(response.body()).isInstanceOf(FetchResponseData.class);
        assertThat(((FetchResponseData) response.body()).errorCode()).isEqualTo((short) 42);
    }

    @Test
    void shortCircuitShouldCompleteFutureWithoutForwarding() throws Exception {
        var filter = new ShortCircuitFilter();
        pipeline = createPipeline(filter);

        var future = new CompletableFuture<Response>();
        var forwarded = new AtomicReference<Object>();
        var frame = fetchRequestFrame(CORRELATION_ID);

        eventLoop.submit(() -> pipeline.writeRequest(frame, future, forwarded::set)).sync();

        assertThat(forwarded.get())
                .as("Short-circuit should not invoke forwarder")
                .isNull();
        assertThat(future).succeedsWithin(Duration.ofSeconds(5));
        Response response = future.get();
        assertThat(response.body()).isInstanceOf(FetchResponseData.class);
    }

    @Test
    void multipleFiltersShouldExecuteInOrder() throws Exception {
        var filter1 = new ClientIdStampingFilter("first");
        var filter2 = new ClientIdStampingFilter("second");
        pipeline = createPipeline(filter1, filter2);

        var future = new CompletableFuture<Response>();
        var forwarded = new AtomicReference<Object>();
        var frame = fetchRequestFrame(CORRELATION_ID);

        eventLoop.submit(() -> pipeline.writeRequest(frame, future, forwarded::set)).sync();

        var forwardedFrame = (DecodedRequestFrame<?>) forwarded.get();
        assertThat(forwardedFrame.header().clientId())
                .as("Last filter in request order wins")
                .isEqualTo("second");
    }

    private RouteFilterPipeline createPipeline(Object... filters) throws Exception {
        var filterAndInvokers = new java.util.ArrayList<FilterAndInvoker>();
        for (Object filter : filters) {
            filterAndInvokers.addAll(FilterAndInvoker.build(
                    "test-filter", (io.kroxylicious.proxy.filter.Filter) filter));
        }
        var nodeIdMapping = new IdentityNodeIdMapping(ROUTE_NAME);
        RouterDispatchHandler.MetadataAddressCacher metadataAddressCacher = body -> {
        };
        return RouteFilterPipeline.create(
                eventLoop,
                filterAndInvokers,
                inboundChannel,
                null,
                ccsm,
                ROUTE_NAME,
                new AtomicInteger(Integer.MIN_VALUE)::getAndIncrement,
                new AtomicInteger(),
                nodeIdMapping,
                metadataAddressCacher).toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private ClientConnectionStateMachine createCcsm() {
        var targetCluster = mock(TargetCluster.class);
        when(targetCluster.bootstrapServersList()).thenReturn(
                List.of(HostPort.parse("localhost:9092")));
        var vc = new VirtualClusterModel("TestVC", targetCluster, false, false,
                List.of(), CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10));
        vc.addGateway("default", mock(NodeIdentificationStrategy.class), Optional.empty());
        var endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.nodeId()).thenReturn(0);
        var gw = mock(EndpointGateway.class);
        when(gw.virtualCluster()).thenReturn(vc);
        when(endpointBinding.endpointGateway()).thenReturn(gw);
        var session = new KafkaSession(KafkaSessionState.ESTABLISHING);
        return new ClientConnectionStateMachine(
                endpointBinding, new DefaultSubjectBuilder(List.of()), session);
    }

    private static DecodedRequestFrame<FetchRequestData> fetchRequestFrame(int correlationId) {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion(API_VERSION)
                .setClientId("original-client");
        return new DecodedRequestFrame<>(
                API_VERSION, correlationId, true, header, new FetchRequestData());
    }

    /**
     * A filter that stamps its clientId on every FETCH request.
     */
    static class ClientIdStampingFilter implements FetchRequestFilter {
        private final String clientId;

        ClientIdStampingFilter(String clientId) {
            this.clientId = clientId;
        }

        @Override
        public CompletionStage<RequestFilterResult> onFetchRequest(
                                                                   short apiVersion,
                                                                   RequestHeaderData header,
                                                                   FetchRequestData request,
                                                                   FilterContext context) {
            header.setClientId(clientId);
            return context.forwardRequest(header, request);
        }
    }

    /**
     * A filter that stamps an error code on every FETCH response.
     */
    static class ErrorCodeStampingFilter implements FetchResponseFilter {
        private final short errorCode;

        ErrorCodeStampingFilter(short errorCode) {
            this.errorCode = errorCode;
        }

        @Override
        public CompletionStage<ResponseFilterResult> onFetchResponse(
                                                                     short apiVersion,
                                                                     ResponseHeaderData header,
                                                                     FetchResponseData response,
                                                                     FilterContext context) {
            response.setErrorCode(errorCode);
            return context.forwardResponse(header, response);
        }
    }

    /**
     * A filter that short-circuits every FETCH request with a response.
     */
    static class ShortCircuitFilter implements FetchRequestFilter {
        @Override
        public CompletionStage<RequestFilterResult> onFetchRequest(
                                                                   short apiVersion,
                                                                   RequestHeaderData header,
                                                                   FetchRequestData request,
                                                                   FilterContext context) {
            return context.requestFilterResultBuilder()
                    .shortCircuitResponse(new ResponseHeaderData(), new FetchResponseData())
                    .completed();
        }
    }
}
