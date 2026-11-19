/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.IngressRef;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxynetworkpolicystatus.KafkaProxyIngresses;

/**
 * Assertions on a {@code status.kafkaProxyIngresses[]} entry ({@link KafkaProxyIngresses}) within
 * a {@code KafkaProxyNetworkPolicy} status.
 */
public class KafkaProxyNetworkPolicyIngressAssert extends AbstractObjectAssert<KafkaProxyNetworkPolicyIngressAssert, KafkaProxyIngresses> {
    /**
     * Creates an assertion on the given status entry.
     *
     * @param o the status entry to assert on
     */
    protected KafkaProxyNetworkPolicyIngressAssert(
                                                   KafkaProxyIngresses o) {
        super(o, KafkaProxyNetworkPolicyIngressAssert.class);
    }

    /**
     * Creates an assertion on the given status entry.
     *
     * @param actual the status entry to assert on
     * @return a new assertion
     */
    public static KafkaProxyNetworkPolicyIngressAssert assertThat(KafkaProxyIngresses actual) {
        return new KafkaProxyNetworkPolicyIngressAssert(actual);
    }

    /**
     * Returns an assertion on the entry's {@code ingressRef.name}.
     *
     * @return an assertion on the ingressRef name
     */
    public AbstractStringAssert<?> ingressRefName() {
        return Assertions.assertThat(actual.getIngressRef()).extracting(IngressRef::getName)
                .asInstanceOf(InstanceOfAssertFactories.STRING);
    }

    /**
     * Asserts that the entry's {@code ingressRef.name} equals the given name.
     *
     * @param name the expected name
     * @return this assertion
     */
    public KafkaProxyNetworkPolicyIngressAssert hasIngressRefName(String name) {
        ingressRefName().isEqualTo(name);
        return this;
    }

    /**
     * Returns a list assertion on the entry's conditions.
     *
     * @return a list assertion on the conditions
     */
    public ListAssert<Condition> conditions() {
        return Assertions.assertThat(actual.getConditions()).asInstanceOf(InstanceOfAssertFactories.list(Condition.class));
    }

    /**
     * Returns an assertion on the entry's condition list.
     *
     * @return an assertion on the condition list
     */
    public ConditionListAssert conditionList() {
        return ConditionListAssert.assertThat(actual.getConditions());
    }

}
