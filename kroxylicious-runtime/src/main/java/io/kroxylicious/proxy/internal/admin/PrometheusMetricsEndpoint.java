/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin;

import java.util.function.Function;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;

import io.kroxylicious.proxy.internal.MeterRegistries;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class PrometheusMetricsEndpoint implements Function<HttpRequest, HttpResponse> {

    public static String PATH = "/metrics";

    private final PrometheusMeterRegistry registry;

    public PrometheusMetricsEndpoint(MeterRegistries registries) {
        this.registry = registries.maybePrometheusMeterRegistry()
                                  .orElseThrow(() -> new IllegalStateException("Attempting to configure a prometheus endpoint but no Prometheus registry available"));
    }

    @Override
    public HttpResponse apply(HttpRequest httpRequest) {
        return RoutingHttpServer.responseWithBody(httpRequest, OK, registry.scrape());
    }
}
