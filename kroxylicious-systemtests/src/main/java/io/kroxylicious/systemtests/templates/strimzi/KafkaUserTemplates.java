/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.strimzi;

import io.fabric8.kubernetes.api.model.SecretKeySelector;
import io.strimzi.api.kafka.model.common.PasswordBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthenticationBuilder;

import io.kroxylicious.systemtests.Constants;

/**
 * The type Kafka user templates.
 */
public class KafkaUserTemplates {

    private static KafkaUserBuilder baseUser(String namespaceName, String clusterName, String name) {
        return new KafkaUserBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespaceName)
                .addToLabels(Constants.STRIMZI_CLUSTER_LABEL, clusterName)
                .endMetadata();
    }

    /**
     * Kafka user with secret builder.
     *
     * @param namespace the namespace
     * @param clusterName the cluster name
     * @param name the name
     * @param secretName the secret name
     * @return  the kafka user builder
     */
    public static KafkaUserBuilder kafkaUserWithSecret(String namespace, String clusterName, String name, String secretName) {
        // @formatter:off
        return baseUser(namespace, clusterName, name)
                .editSpec()
                    .withAuthentication(new KafkaUserScramSha512ClientAuthenticationBuilder()
                        .withPassword(new PasswordBuilder()
                            .withNewValueFrom()
                                .withSecretKeyRef(new SecretKeySelector("password", secretName, false))
                            .endValueFrom()
                            .build())
                        .build())
                .endSpec();
        // @formatter:on
    }
}
