/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.InOrder;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class LocalKroxyliciousOperatorExtensionLifecycleTest {

    private final LocallyRunningOperatorRbacHandler rbacHandler = mock(LocallyRunningOperatorRbacHandler.class);
    private final LocallyRunOperatorExtension locallyRunOperatorExtension = mock(LocallyRunOperatorExtension.class);
    private final ExtensionContext context = mock(ExtensionContext.class);

    private final LocalKroxyliciousOperatorExtension extension = new LocalKroxyliciousOperatorExtension(
            LocalKroxyliciousOperatorExtension.builder()
                    .withReconciler((KafkaProxy resource, Context<KafkaProxy> ctx) -> UpdateControl.noUpdate()),
            () -> rbacHandler,
            handler -> locallyRunOperatorExtension,
            () -> {
            });

    @Test
    void beforeAllSetsUpRbacThenStartsOperator() throws Exception {
        extension.beforeAll(context);

        InOrder order = inOrder(rbacHandler, locallyRunOperatorExtension);
        order.verify(rbacHandler).beforeEach(context);
        order.verify(locallyRunOperatorExtension).beforeAll(context);
    }

    @Test
    void afterAllStopsOperatorThenCleansUpRbac() throws Exception {
        extension.beforeAll(context);

        extension.afterAll(context);

        InOrder order = inOrder(locallyRunOperatorExtension, rbacHandler);
        order.verify(locallyRunOperatorExtension).afterAll(context);
        order.verify(rbacHandler).afterEach(context);
        order.verify(rbacHandler).afterAll(context);
    }

    @Test
    void afterAllIsIdempotentIfBeforeAllWasNeverCalled() throws Exception {
        extension.afterAll(context);

        verify(locallyRunOperatorExtension, never()).afterAll(context);
        verify(rbacHandler, never()).afterEach(context);
    }

    @Test
    void afterEachIsSafeIfBeforeAllWasNeverCalled() throws Exception {
        extension.afterEach(context);
        // testActor is null — nothing to verify, just must not throw
    }
}
