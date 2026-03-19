/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalKroxyliciousOperatorExtensionBuilderTest {

    @Test
    void buildWithNoReconcilerShouldThrow() {
        var builder = LocalKroxyliciousOperatorExtension.builder();
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("at least one reconciler");
    }

    @Test
    void buildWithOneReconcilerShouldSucceed() {
        assertThatCode(() -> LocalKroxyliciousOperatorExtension.builder()
                .withReconciler((KafkaProxy resource, Context<KafkaProxy> context) -> UpdateControl.noUpdate())
                .build()).doesNotThrowAnyException();
    }

    @Test
    void withReconcilerAccumulatesMultipleReconcilers() {
        assertThatCode(() -> LocalKroxyliciousOperatorExtension.builder()
                .withReconciler((KafkaProxy resource, Context<KafkaProxy> context) -> UpdateControl.noUpdate())
                .withReconciler((KafkaProxy resource, Context<KafkaProxy> context) -> UpdateControl.noUpdate())
                .build()).doesNotThrowAnyException();
    }
}