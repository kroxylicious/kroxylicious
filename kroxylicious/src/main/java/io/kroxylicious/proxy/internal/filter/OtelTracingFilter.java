package io.kroxylicious.proxy.internal.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.internal.util.NettyMemoryRecords;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OtelTracingFilter implements ProduceRequestFilter, ProduceResponseFilter {

    private final TextMapPropagator textMapPropagator;
    //TODO inject? Configure?
    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("kroxylicious", "0.0.1");

    public OtelTracingFilter() {
        textMapPropagator = W3CTraceContextPropagator.getInstance();
    }

    @Override
    public void onProduceRequest(ProduceRequestData request, KrpcFilterContext filterContext) {
        //TODO not convinced creating a span here
        try (Scope ignored = configureSpanDefaults("ProduceRequest", filterContext)
                .startSpan()
                .makeCurrent()) {
            //TODO only take action if we are tracing the request
            if (Span.current().isRecording()) {
                request.topicData().stream()
                        .flatMap(topicProduceData -> topicProduceData.partitionData().stream())
                        .forEach(partitionProduceData -> {
                            MemoryRecords records = (MemoryRecords) partitionProduceData.records();
                            try(MemoryRecordsBuilder newRecords = NettyMemoryRecords.builder(filterContext.allocate((records).sizeInBytes()),
                                    CompressionType.NONE,
                                    TimestampType.CREATE_TIME,
                                    0)) {
                                records.batches().forEach(batchRecord -> batchRecord.forEach(record -> {
                                    final Scope recordScope = configureSpanDefaults("Record", filterContext)
                                            .setParent(Context.current())
                                            .startSpan()
                                            .makeCurrent();
                                    final List<Header> headers = new ArrayList<>(Arrays.asList(record.headers().clone()));
                                    textMapPropagator.inject(Context.current(),
                                            record,
                                            (carrier, key, value) -> headers.add(new RecordHeader("kroxy_" + key, value.getBytes(StandardCharsets.UTF_8))));
                                    newRecords.append(record.timestamp(), record.key(), record.value(), headers.toArray(new Header[0]));
                                    recordScope.close();
                                }));
                                partitionProduceData.setRecords(newRecords.build());
                            }
                        });
            }
        }
    }

    private static SpanBuilder configureSpanDefaults(String spanName, KrpcFilterContext filterContext) {
        return tracer.spanBuilder(spanName)
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("KroxyliciousCluster", "testing")
                .setAttribute("Channel", filterContext.channelDescriptor());
    }

    @Override
    public void onProduceResponse(ProduceResponseData response, KrpcFilterContext context) {

    }

    public static class OtelTracingFilterConfig extends FilterConfig {
        @JsonCreator
        public OtelTracingFilterConfig() {
        }
    }

}
