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
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class KafkaServicePrimaryToTlsCertSecretSecondaryMapper implements PrimaryToSecondaryMapper<KafkaService> {
    @Override
    public Set<ResourceID> toSecondaryResourceIDs(KafkaService cluster) {
        return Optional.ofNullable(cluster.getSpec())
                .map(KafkaServiceSpec::getTls)
                .map(Tls::getCertificateRef)
                .map(cr -> ResourcesUtil.localRefAsResourceId(cluster, cr)).orElse(Set.of());
    }
}
