/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.monitoring.micrometer.MicrometerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;

import io.kroxylicious.kubernetes.operator.config.FilterApiDecl;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The {@code main} method entrypoint for the operator
 */
public class OperatorMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorMain.class);
    private MeterRegistry registry;
    private final Operator operator;
    private HTTPServer metricsServer;

    public OperatorMain() {
        this(null);
    }

    public OperatorMain(@Nullable KubernetesClient kubeClient) {
        final MicrometerMetrics metrics = enablePrometheusMetrics();
        // o.withMetrics is invoked multiple times so can cause issues with enabling metrics.
        operator = new Operator(o -> {
            o.withMetrics(metrics);
            if (kubeClient != null) {
                o.withKubernetesClient(kubeClient);
            }
        });
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
        try {
            metricsServer = HTTPServer.builder().port(8080).buildAndStart();
            var registeredController = operator.register(new ProxyReconciler(runtimeDecl()));
            // TODO couple the health of the registeredController to the operator's HTTP healthchecks
            operator.start();
            LOGGER.info("Operator started.");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void stop() {
        operator.stop();
        metricsServer.stop();
        LOGGER.info("Operator stopped.");
    }

    @NonNull
    static RuntimeDecl runtimeDecl() {
        // TODO read these from some configuration CR
        return new RuntimeDecl(List.of(new FilterApiDecl("filter.kroxylicious.io", "v1alpha1", "KafkaProtocolFilter")));
    }

    private MicrometerMetrics enablePrometheusMetrics() {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.globalRegistry.add(registry);
        return MicrometerMetrics.newPerResourceCollectingMicrometerMetricsBuilder(Metrics.globalRegistry).withCleanUpDelayInSeconds(35).withCleaningThreadNumber(1)
                .build();
    }
}
