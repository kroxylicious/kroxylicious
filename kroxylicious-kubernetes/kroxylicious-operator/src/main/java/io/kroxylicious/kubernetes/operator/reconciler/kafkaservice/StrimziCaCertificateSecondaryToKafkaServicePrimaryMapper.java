/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Secret;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.common.StrimziKafkaRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpec;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.STRIMZI_CLUSTER_CA_CERT_SECRET_SUFFIX;

class StrimziCaCertificateSecondaryToKafkaServicePrimaryMapper implements SecondaryToPrimaryMapper<Secret> {
    private static final String STRIMZI_CA_CERTIFICATE_REF_INDEX = "strimziCaCertificateRef";

    private final EventSourceContext<KafkaService> context;

    StrimziCaCertificateSecondaryToKafkaServicePrimaryMapper(EventSourceContext<KafkaService> context) {
        this.context = context;
        context.getPrimaryCache().addIndexer(STRIMZI_CA_CERTIFICATE_REF_INDEX, service -> Optional.ofNullable(service.getSpec())
                .map(KafkaServiceSpec::getStrimziKafkaRef)
                .filter(StrimziKafkaRef::getTrustStrimziCaCertificate)
                .map(strimziKafkaRef -> ResourcesUtil.namespacedName(ResourcesUtil.strimziKafkaNamespace(service, strimziKafkaRef),
                        strimziKafkaRef.getRef().getName() + STRIMZI_CLUSTER_CA_CERT_SECRET_SUFFIX))
                .stream()
                .toList());
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(Secret secret) {
        return context.getPrimaryCache()
                .byIndex(STRIMZI_CA_CERTIFICATE_REF_INDEX, ResourcesUtil.namespacedName(ResourcesUtil.namespace(secret), ResourcesUtil.name(secret)))
                .stream()
                .map(ResourceID::fromResource)
                .collect(Collectors.toSet());
    }
}
