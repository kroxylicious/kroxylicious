/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.templates.strimzi;

import io.strimzi.api.kafka.model.KafkaUserBuilder;

import io.kroxylicious.Constants;

public class KafkaUserTemplates {

    public static KafkaUserBuilder defaultUser(String namespaceName, String clusterName, String name) {
        return new KafkaUserBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespaceName)
                .addToLabels(Constants.STRIMZI_CLUSTER_LABEL, clusterName)
                .endMetadata();
    }
}
