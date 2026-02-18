/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.util.Optional;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Secret;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class SecretSecondaryJoinedOnTlsCertificateRefMapperToKafkaServicePrimaryMapper implements SecondaryToPrimaryMapper<Secret> {
    private final EventSourceContext<KafkaService> context;

    SecretSecondaryJoinedOnTlsCertificateRefMapperToKafkaServicePrimaryMapper(EventSourceContext<KafkaService> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(Secret secret) {
        return ResourcesUtil.findReferrers(context,
                secret,
                KafkaService.class,
                service -> Optional.ofNullable(service.getSpec())
                        .map(KafkaServiceSpec::getTls)
                        .map(Tls::getCertificateRef));
    }
}
