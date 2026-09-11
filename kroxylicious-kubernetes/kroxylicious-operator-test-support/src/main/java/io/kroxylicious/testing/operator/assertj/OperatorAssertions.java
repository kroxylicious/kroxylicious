/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import org.assertj.core.api.InstanceOfAssertFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;
import io.kroxylicious.proxy.config.Configuration;

/**
 * Entry point for the custom assertions in this package, following the AssertJ
 * {@code assertThat} naming convention.
 */
public class OperatorAssertions {

    // Static factory should not be instantiated.
    private OperatorAssertions() {
    }

    /**
     * Factory producing {@link ProxyConfigAssert}, for use with AssertJ's {@code asInstanceOf}.
     */
    public static final InstanceOfAssertFactory<?, ? extends ProxyConfigAssert> CONFIGURATION = new InstanceOfAssertFactory<>(Configuration.class,
            OperatorAssertions::assertThat);

    /**
     * Creates an assertion on the given {@code KafkaProxy} status.
     *
     * @param actual the status to assert on
     * @return a new assertion
     */
    public static KafkaProxyStatusAssert assertThat(KafkaProxyStatus actual) {
        return KafkaProxyStatusAssert.assertThat(actual);
    }

    /**
     * Creates an assertion on the given {@code KafkaService} status.
     *
     * @param actual the status to assert on
     * @return a new assertion
     */
    public static KafkaServiceStatusAssert assertThat(KafkaServiceStatus actual) {
        return KafkaServiceStatusAssert.assertThat(actual);
    }

    /**
     * Creates an assertion on the given cluster entry of a {@code KafkaProxy} status.
     *
     * @param actual the cluster to assert on
     * @return a new assertion
     */
    public static ClusterAssert assertThat(Clusters actual) {
        return ClusterAssert.assertThat(actual);
    }

    /**
     * Creates an assertion on the given condition.
     *
     * @param actual the condition to assert on
     * @return a new assertion
     */
    public static ConditionAssert assertThat(Condition actual) {
        return ConditionAssert.assertThat(actual);
    }

    /**
     * Creates an assertion on the given proxy configuration.
     *
     * @param actual the configuration to assert on
     * @return a new assertion
     */
    public static ProxyConfigAssert assertThat(Configuration actual) {
        return ProxyConfigAssert.assertThat(actual);
    }

    /**
     * Creates an assertion on the metadata of the given resource.
     *
     * @param <T> the resource type
     * @param actual the resource to assert on
     * @return a new assertion
     */
    public static <T extends HasMetadata> MetadataAssert<T> assertThat(T actual) {
        return MetadataAssert.assertThat(actual);
    }

    /**
     * Creates an assertion on the given metadata.
     *
     * @param actual the metadata to assert on
     * @return a new assertion
     */
    public static ObjectMetaAssert assertThat(ObjectMeta actual) {
        return ObjectMetaAssert.assertThat(actual);
    }
}
