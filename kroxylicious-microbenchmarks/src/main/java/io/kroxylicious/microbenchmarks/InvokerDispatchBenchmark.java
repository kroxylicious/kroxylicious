/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.microbenchmarks;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.kroxylicious.filters.FourInterfaceFilter0;
import io.kroxylicious.filters.FourInterfaceFilter1;
import io.kroxylicious.filters.FourInterfaceFilter2;
import io.kroxylicious.filters.FourInterfaceFilter3;
import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.filter.ArrayFilterInvoker;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.FilterInvokers;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResultBuilder;
import io.kroxylicious.proxy.filter.SpecificFilterInvoker;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.tls.ClientTlsContext;

// try hard to make shouldHandleXYZ to observe different receivers concrete types, saving unrolling to bias a specific call-site to a specific concrete type
@Fork(value = 2, jvmArgsAppend = "-XX:LoopUnrollLimit=1")
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class InvokerDispatchBenchmark {

    // A low and constant number of tokens allows us to balance the amount of work done in the benchmarks with still being able to observe the effects of the different dispatch mechanisms.
    public static final int CONSUME_TOKENS = 5;

    public enum Invoker {
        array {
            @Override
            FilterInvoker invokerWith(Filter filter) {
                return new ArrayFilterInvoker(filter);
            }
        },
        specific {
            @Override
            FilterInvoker invokerWith(Filter filter) {
                return new SpecificFilterInvoker(filter);
            }
        },
        switching {
            @Override
            FilterInvoker invokerWith(Filter filter) {
                return FilterInvokers.arrayInvoker(filter);
            }
        };

        abstract FilterInvoker invokerWith(Filter filter);
    }

    @State(Scope.Benchmark)
    public static class BenchState {
        FilterInvoker[] invokers;

        ApiKeys[] keys;

        @Param({ "array", "specific", "switching" })
        String invoker;

        private RequestHeaderData requestHeaders;
        private FilterContext filterContext;
        private Map.Entry<ApiKeys, ApiMessage>[] apiMessages;

        @SuppressWarnings("unchecked")
        @Setup
        public void init() {
            Invoker invokerType = Invoker.valueOf(invoker);
            invokers = new FilterInvoker[]{
                    invokerType.invokerWith(new FourInterfaceFilter0()),
                    invokerType.invokerWith(new FourInterfaceFilter1()),
                    invokerType.invokerWith(new FourInterfaceFilter2()),
                    invokerType.invokerWith(new FourInterfaceFilter3()),
                    invokerType.invokerWith(new FourInterfaceFilter0()),
                    invokerType.invokerWith(new FourInterfaceFilter1()),
                    invokerType.invokerWith(new FourInterfaceFilter2()),
                    invokerType.invokerWith(new FourInterfaceFilter3())
            };
            final Map<ApiKeys, ApiMessage> messages = Map.of(ApiKeys.PRODUCE, new ProduceRequestData(), ApiKeys.API_VERSIONS, new ApiVersionsRequestData(), ApiKeys.FETCH,
                    new FetchRequestData());
            apiMessages = messages.entrySet().toArray(new Map.Entry[0]); // Avoids iterator.next showing up in the benchmarks
            requestHeaders = new RequestHeaderData();
            filterContext = new StubFilterContext();
            keys = messages.keySet().toArray(new ApiKeys[0]);
        }
    }

    @Benchmark
    public void testDispatchToShouldHandle(BenchState state, Blackhole blackhole) {
        invokeShouldHandle(blackhole, state.invokers, state.keys);
    }

    @Benchmark
    public void testDispatchToHandleRequest(BenchState state) {
        invokeHandleRequest(state.invokers, state.apiMessages, state.requestHeaders, state.filterContext);
    }

    @Benchmark
    @Threads(4)
    public void test4ThreadsDispatchToShouldHandle(BenchState state, Blackhole blackhole) {
        invokeShouldHandle(blackhole, state.invokers, state.keys);
    }

    @Benchmark
    @Threads(4)
    public void test4ThreadsDispatchToHandleRequest(BenchState state) {
        invokeHandleRequest(state.invokers, state.apiMessages, state.requestHeaders, state.filterContext);
    }

    private static void invokeShouldHandle(Blackhole blackhole, FilterInvoker[] filters, ApiKeys[] apiKeys) {
        for (ApiKeys apiKey : apiKeys) {
            final short apiVersion = apiKey.latestVersion();
            for (FilterInvoker invoker : filters) {
                blackhole.consume(invoker.shouldHandleRequest(apiKey, apiVersion));
                blackhole.consume(invoker.shouldHandleResponse(apiKey, apiVersion));
            }
        }
    }

    private static void invokeHandleRequest(FilterInvoker[] filters, Map.Entry<ApiKeys, ApiMessage>[] apiMessages, RequestHeaderData requestHeaders,
                                            FilterContext filterContext) {
        for (Map.Entry<ApiKeys, ApiMessage> entry : apiMessages) {
            final ApiKeys apiKey = entry.getKey();
            final short apiVersion = apiKey.latestVersion();
            for (FilterInvoker invoker : filters) {
                if (invoker.shouldHandleRequest(apiKey, apiVersion)) {
                    invoker.onRequest(apiKey, apiVersion, requestHeaders, entry.getValue(), filterContext);
                }
            }
        }
    }

    private static class StubFilterContext implements FilterContext {
        @Override
        public String channelDescriptor() {
            return "";
        }

        @Override
        public String sessionId() {
            return "";
        }

        @Override
        public ByteBufferOutputStream createByteBufferOutputStream(int initialCapacity) {
            return null;
        }

        @Override
        public String sniHostname() {
            return null;
        }

        @Override
        public String getVirtualClusterName() {
            return null;
        }

        @Override
        public Optional<ClientTlsContext> clientTlsContext() {
            return Optional.empty();
        }

        @Override
        public void clientSaslAuthenticationSuccess(String mechanism, String authorizedId) {

        }

        @Override
        public void clientSaslAuthenticationFailure(String mechanism, String authorizedId, Exception exception) {

        }

        @Override
        public Optional<ClientSaslContext> clientSaslContext() {
            return Optional.empty();
        }

        @Override
        public <M extends ApiMessage> CompletionStage<M> sendRequest(RequestHeaderData header, ApiMessage request) {
            return null;
        }

        @Override
        public CompletionStage<TopicNameMapping> topicNames(Collection<Uuid> topicIds) {
            return null;
        }

        @Override
        public CompletionStage<ResponseFilterResult> forwardResponse(ResponseHeaderData header, ApiMessage response) {
            return null;
        }

        @Override
        public RequestFilterResultBuilder requestFilterResultBuilder() {
            return null;
        }

        @Override
        public CompletionStage<RequestFilterResult> forwardRequest(RequestHeaderData header, ApiMessage request) {
            return null;
        }

        @Override
        public ResponseFilterResultBuilder responseFilterResultBuilder() {
            return null;
        }

    }
}
