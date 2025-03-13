/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.monitoring.micrometer.MicrometerMetrics;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.exporter.httpserver.MetricsHandler;

import io.kroxylicious.kubernetes.operator.config.FilterApiDecl;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The {@code main} method entrypoint for the operator
 */
public class OperatorMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorMain.class);
    private final Operator operator;
    private final HttpServer managementServer;

    public OperatorMain() throws IOException {
        this(null, HttpServer.create(new InetSocketAddress("0.0.0.0", 8080), 10));
    }

    public OperatorMain(@Nullable KubernetesClient kubeClient, @NonNull HttpServer managementServer) {
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
        var registeredController = operator.register(new ProxyReconciler(runtimeDecl()));
        // TODO couple the health of the registeredController to the operator's HTTP healthchecks
        managementServer.start();
        operator.start();
        LOGGER.info("Operator started.");
    }

    void stop() {
        operator.stop();
        managementServer.stop(0); // TODO maybe this should be configurable
        LOGGER.info("Operator stopped.");
    }

    @NonNull
    static RuntimeDecl runtimeDecl() {
        // TODO read these from some configuration CR
        return new RuntimeDecl(List.of(new FilterApiDecl("filter.kroxylicious.io", "v1alpha1", "KafkaProtocolFilter")));
    }

    private MicrometerMetrics enablePrometheusMetrics() {
        return MicrometerMetrics.newPerResourceCollectingMicrometerMetricsBuilder(Metrics.globalRegistry)
                .withCleanUpDelayInSeconds(35)
                .withCleaningThreadNumber(1)
                .build();
    }

    private void configurePrometheusMetrics(HttpServer managementServer) {
        final PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        managementServer.createContext("/metrics", new MetricsHandler(prometheusMeterRegistry.getPrometheusRegistry()));
        Metrics.globalRegistry.add(prometheusMeterRegistry);
    }
}
