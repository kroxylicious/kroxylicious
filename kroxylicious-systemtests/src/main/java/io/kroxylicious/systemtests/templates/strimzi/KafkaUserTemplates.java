/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.strimzi;

import io.strimzi.api.kafka.model.user.KafkaUserBuilder;

import io.kroxylicious.systemtests.Constants;

/**
 * The type Kafka user templates.
 */
public class KafkaUserTemplates {

    /**
     * Default user kafka user builder.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param name the name
     * @return the kafka user builder
     */
    public static KafkaUserBuilder defaultUser(String namespaceName, String clusterName, String name) {
        return new KafkaUserBuilder()
                                     .withNewMetadata()
                                     .withName(name)
                                     .withNamespace(namespaceName)
                                     .addToLabels(Constants.STRIMZI_CLUSTER_LABEL, clusterName)
                                     .endMetadata();
    }
}
