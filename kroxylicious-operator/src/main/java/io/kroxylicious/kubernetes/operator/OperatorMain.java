/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.monitoring.micrometer.MicrometerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import io.kroxylicious.kubernetes.operator.config.FilterApiDecl;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The {@code main} method entrypoint for the operator
 */
public class OperatorMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorMain.class);
    private MeterRegistry registry;
    private final Operator operator;

    public OperatorMain() {
        operator = new Operator(o -> o.withMetrics(enablePrometheusMetrics()));
    }

    public static void main(String[] args) {
        try {
            new OperatorMain().run();
        }
        catch (Exception e) {
            LOGGER.error("Operator has thrown exception during startup. Will now exit.", e);
            System.exit(1);
        }
    }

    void run() {
        operator.installShutdownHook(Duration.ofSeconds(10));
        var registeredController = operator.register(new ProxyReconciler(runtimeDecl()));
        // TODO couple the health of the registeredController to the operator's HTTP healthchecks
        operator.start();
        LOGGER.info("Operator started.");
    }

    public void close() {
        // remove the meters we contributed to the global registry.
        var copy = List.copyOf(registry.getMeters());
        copy.forEach(Metrics.globalRegistry::remove);
        Metrics.removeRegistry(registry);
        registry.close();
        LOGGER.info("Operator closed.");
        registry = null;
    }

    @VisibleForTesting
    MeterRegistry getRegistry() {
        return registry;
    }

    @NonNull
    static RuntimeDecl runtimeDecl() {
        // TODO read these from some configuration CR
        return new RuntimeDecl(List.of(
                new FilterApiDecl("filter.kroxylicious.io", "v1alpha1", "KafkaProtocolFilter")));
    }

    private MicrometerMetrics enablePrometheusMetrics() {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.addRegistry(registry);
        return MicrometerMetrics.newPerResourceCollectingMicrometerMetricsBuilder(registry)
                .withCleanUpDelayInSeconds(35)
                .withCleaningThreadNumber(1)
                .build();
    }
}
