/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyNetworkPolicyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxynetworkpolicystatus.KafkaProxyIngresses;

/**
 * Assertions on a {@link KafkaProxyNetworkPolicyStatus}.
 */
public class KafkaProxyNetworkPolicyStatusAssert extends AbstractStatusAssert<KafkaProxyNetworkPolicyStatus, KafkaProxyNetworkPolicyStatusAssert> {
    /**
     * Creates an assertion on the given status.
     *
     * @param o the status to assert on
     */
    protected KafkaProxyNetworkPolicyStatusAssert(
                                                  KafkaProxyNetworkPolicyStatus o) {
        super(o, KafkaProxyNetworkPolicyStatusAssert.class,
                KafkaProxyNetworkPolicyStatus::getObservedGeneration,
                KafkaProxyNetworkPolicyStatus::getConditions);
    }

    /**
     * Creates an assertion on the given status.
     *
     * @param actual the status to assert on
     * @return a new assertion
     */
    public static KafkaProxyNetworkPolicyStatusAssert assertThat(KafkaProxyNetworkPolicyStatus actual) {
        return new KafkaProxyNetworkPolicyStatusAssert(actual);
    }

    @Override
    public AbstractLongAssert<?> observedGeneration() {
        Assertions.assertThat(actual).isNotNull();
        return Assertions.assertThat(actual.getObservedGeneration());
    }

    @Override
    public ListAssert<Condition.Status> conditions() {
        return Assertions.assertThat(actual.getConditions())
                .asInstanceOf(InstanceOfAssertFactories.list(Condition.Status.class));
    }

    @Override
    public ConditionAssert singleCondition() {
        return conditions().singleElement(AssertFactory.condition());
    }

    /**
     * Returns a list assertion on the status's per-entry {@code kafkaProxyIngresses}.
     *
     * @return a list assertion on the kafkaProxyIngresses entries
     */
    public ListAssert<KafkaProxyIngresses> kafkaProxyIngresses() {
        return Assertions.assertThat(actual.getKafkaProxyIngresses()).asInstanceOf(InstanceOfAssertFactories.list(KafkaProxyIngresses.class));
    }

}
