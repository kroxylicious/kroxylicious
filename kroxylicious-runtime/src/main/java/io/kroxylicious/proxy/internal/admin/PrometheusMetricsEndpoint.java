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

/**
 * Management HTTP endpoint that serves metrics in Prometheus exposition format, produced by
 * scraping the Prometheus meter registry.
 */
public class PrometheusMetricsEndpoint implements Function<HttpRequest, HttpResponse> {

    /** HTTP path at which the metrics endpoint is exposed. */
    public static final String PATH = "/metrics";

    private final PrometheusMeterRegistry registry;

    /**
     * Creates a Prometheus metrics endpoint.
     *
     * @param registries meter registries from which the Prometheus meter registry is obtained
     * @throws IllegalStateException if no Prometheus meter registry is available
     */
    public PrometheusMetricsEndpoint(MeterRegistries registries) {
        this.registry = registries.maybePrometheusMeterRegistry()
                .orElseThrow(() -> new IllegalStateException("Attempting to configure a prometheus endpoint but no Prometheus registry available"));
    }

    @Override
    public HttpResponse apply(HttpRequest httpRequest) {
        return RoutingHttpServer.responseWithBody(httpRequest, OK, registry.scrape());
    }
}
