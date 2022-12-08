package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;

public class OtelTracingFilter implements ProduceRequestFilter, ProduceResponseFilter {
    @Override
    public void onProduceRequest(ProduceRequestData request, KrpcFilterContext context) {
        ;
    }

    @Override
    public void onProduceResponse(ProduceResponseData response, KrpcFilterContext context) {

    }
}
