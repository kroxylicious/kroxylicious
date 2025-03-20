/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.function.IntSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.monitoring.micrometer.MicrometerMetrics;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.exporter.httpserver.MetricsHandler;

import io.kroxylicious.kubernetes.operator.management.UnsupportedHttpMethodFilter;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The {@code main} method entrypoint for the operator
 */
public class OperatorMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorMain.class);
    private static final String BIND_ADDRESS_VAR_NAME = "BIND_ADDRESS";
    private static final int DEFAULT_MANAGEMENT_PORT = 8080;
    static final String HTTP_PATH_LIVEZ = "/livez";
    static final String HTTP_PATH_METRICS = "/metrics";
    private final Operator operator;
    private final HttpServer managementServer;

    public OperatorMain() throws IOException {
        this(null, createHttpServer());
    }

    @VisibleForTesting
    OperatorMain(@Nullable KubernetesClient kubeClient, @NonNull HttpServer managementServer) {
        configurePrometheusMetrics(managementServer);
        // o.withMetrics is invoked multiple times so can cause issues with enabling metrics.
        operator = new Operator(o -> {
            o.withMetrics(enablePrometheusMetrics());
            if (kubeClient != null) {
                o.withKubernetesClient(kubeClient);
            }
        });
        this.managementServer = managementServer;
    }

    public static void main(String[] args) {
        try {
            new OperatorMain().start();
        }
        catch (Exception e) {
            LOGGER.error("Operator has thrown exception during startup. Will now exit.", e);
            System.exit(1);
        }
    }

    /**
     * Starts the operator instance and returns once that has completed successfully.
     */
    void start() {
        operator.installShutdownHook(Duration.ofSeconds(10));
        operator.register(new ProxyReconciler());
        addHttpGetHandler("/", () -> 404);
        managementServer.start();
        operator.start();
        addHttpGetHandler(HTTP_PATH_LIVEZ, this::livezStatusCode);
        LOGGER.info("Operator started");
    }

    private void addHttpGetHandler(
                                   String path,
                                   IntSupplier statusCodeSupplier) {
        managementServer.createContext(path, exchange -> {
            try (exchange) {
                // note while the JDK docs advise exchange.getRequestBody().transferTo(OutputStream.nullOutputStream()); we explicitly don't do that!
                // As a denial-of-service protection we don't expect anything other than GET requests so there should be no input to read.
                exchange.sendResponseHeaders(statusCodeSupplier.getAsInt(), -1);
            }
        }).getFilters().add(UnsupportedHttpMethodFilter.INSTANCE);
    }

    private int livezStatusCode() {
        int sc;
        try {
            sc = operator.getRuntimeInfo().allEventSourcesAreHealthy() ? 200 : 400;
        }
        catch (Exception e) {
            sc = 400;
            LOGGER.error("Ignoring exception caught while getting operator health info", e);
        }
        LOGGER.trace("Responding {} to GET {}", sc, HTTP_PATH_LIVEZ);
        return sc;
    }

    void stop() {
        operator.stop();
        managementServer.stop(0); // TODO maybe this should be configurable
        LOGGER.info("Operator stopped.");
    }

    private MicrometerMetrics enablePrometheusMetrics() {
        return MicrometerMetrics.newPerResourceCollectingMicrometerMetricsBuilder(Metrics.globalRegistry)
                .withCleanUpDelayInSeconds(35)
                .withCleaningThreadNumber(1)
                .build();
    }

    private void configurePrometheusMetrics(HttpServer managementServer) {
        final PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        final HttpContext metricsContext = managementServer.createContext(HTTP_PATH_METRICS,
                new MetricsHandler(prometheusMeterRegistry.getPrometheusRegistry()));
        metricsContext.getFilters().add(UnsupportedHttpMethodFilter.INSTANCE);
        Metrics.globalRegistry.add(prometheusMeterRegistry);
    }

    @VisibleForTesting
    static HttpServer createHttpServer() throws IOException {
        final Properties systemProps = System.getProperties();
        if (!systemProps.containsKey("sun.net.httpserver.maxReqTime")) {
            System.setProperty("sun.net.httpserver.maxReqTime", "60");
        }

        if (!systemProps.containsKey("sun.net.httpserver.maxRspTime")) {
            System.setProperty("sun.net.httpserver.maxRspTime", "120");
        }

        return HttpServer.create(getBindAddress(), 0);
    }

    @NonNull
    @VisibleForTesting
    static InetSocketAddress getBindAddress() {
        final Map<String, String> envVars = System.getenv();
        final String bindAddress = envVars.getOrDefault(BIND_ADDRESS_VAR_NAME, "0.0.0.0:" + DEFAULT_MANAGEMENT_PORT);
        String bindToInterface;
        int bindToPort;
        if (bindAddress.contains(":")) {
            final HostPort parse = HostPort.parse(bindAddress);
            bindToInterface = parse.host();
            bindToPort = parse.port();
        }
        else if (!bindAddress.isEmpty()) {
            LOGGER.warn("{} env var is set but does not contain `:` assuming hostname only and binding to default port ({})",
                    BIND_ADDRESS_VAR_NAME,
                    DEFAULT_MANAGEMENT_PORT);
            bindToInterface = bindAddress;
            bindToPort = DEFAULT_MANAGEMENT_PORT;
        }
        else {
            bindToInterface = "0.0.0.0";
            bindToPort = DEFAULT_MANAGEMENT_PORT;
        }

        LOGGER.info("Starting management server on: {}:{}", bindToInterface, bindToPort);
        return new InetSocketAddress(bindToInterface, bindToPort);
    }

}
