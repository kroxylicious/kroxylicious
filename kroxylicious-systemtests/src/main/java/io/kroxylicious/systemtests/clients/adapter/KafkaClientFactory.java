/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.adapter;

import io.kroxylicious.systemtests.api.client.ClientDriver;
import io.kroxylicious.systemtests.api.client.ClientExecutionEnvironment;
import io.kroxylicious.systemtests.api.client.ClientSelection;
import io.kroxylicious.systemtests.api.client.KafkaClient;
import io.kroxylicious.systemtests.clients.KafClient;
import io.kroxylicious.systemtests.clients.KcatClient;
import io.kroxylicious.systemtests.clients.PythonTestClient;
import io.kroxylicious.systemtests.clients.StrimziTestClient;
import io.kroxylicious.systemtests.steps.KafkaSteps;

/**
 * Builds the intent-based {@link KafkaClient} for a {@link ClientSelection}.
 * <p>
 * Execution-environment concerns (namespace, container image, image preloading) stay here and in
 * the legacy clients; none of them leak into the {@link KafkaClient} contract a test sees.
 */
public final class KafkaClientFactory {

    private KafkaClientFactory() {
    }

    /**
     * Builds the client selected by {@link ClientSelection#fromEnvironment()}.
     *
     * @param namespace namespace on-cluster clients are deployed into; ignored for in-process clients
     * @return the client
     * @throws UnsupportedOperationException if the selected combination is not yet implemented
     */
    public static KafkaClient fromEnvironment(String namespace) {
        return create(ClientSelection.fromEnvironment(), namespace);
    }

    /**
     * Builds the client for the given selection.
     *
     * @param selection driver and execution environment
     * @param namespace namespace on-cluster clients are deployed into; ignored for in-process clients
     * @return the client
     * @throws UnsupportedOperationException if the selected combination is not yet implemented
     */
    public static KafkaClient create(ClientSelection selection, String namespace) {
        return switch (selection.executionEnvironment()) {
            case ON_CLUSTER -> onCluster(selection.driver(), namespace);
            case IN_PROCESS -> inProcess(selection.driver());
        };
    }

    private static KafkaClient onCluster(ClientDriver driver, String namespace) {
        io.kroxylicious.systemtests.clients.KafkaClient legacy = switch (driver) {
            case JAVA -> new StrimziTestClient();
            case LIBRDKAFKA -> new PythonTestClient();
            case SARAMA -> new KafClient();
            case KCAT -> new KcatClient();
        };
        return new OnClusterKafkaClient(driver, legacy.inNamespace(namespace),
                (topic, bootstrap, partitions, replicationFactor) -> KafkaSteps.createTopic(namespace, topic, bootstrap, partitions, replicationFactor));
    }

    private static KafkaClient inProcess(ClientDriver driver) {
        if (driver == ClientDriver.JAVA) {
            return new InProcessJavaKafkaClient();
        }
        throw new UnsupportedOperationException("The " + driver.envValue() + " driver cannot yet run " + ClientExecutionEnvironment.IN_PROCESS.envValue());
    }
}
