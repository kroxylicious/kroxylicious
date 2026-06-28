/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.util.Optional;
import java.util.Set;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpec;

class KafkaServicePrimaryToStrimziCaCertificateSecondaryMapper implements PrimaryToSecondaryMapper<KafkaService> {
    @Override
    public Set<ResourceID> toSecondaryResourceIDs(KafkaService service) {
        return Optional.ofNullable(service.getSpec())
                .map(KafkaServiceSpec::getStrimziKafkaRef)
                .map(strimziKafkaRef -> Set.of(new ResourceID(strimziKafkaRef.getRef().getName() + "-cluster-ca-cert", service.getMetadata().getNamespace())))
                .orElse(Set.of());
    }
}
