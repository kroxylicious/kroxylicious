/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.operator.StatusFactory;

/**
 * Checks whether a {@link KafkaProxy} resource has a {@code null} spec.
 * <p>
 * If true, appends a {@link io.kroxylicious.kubernetes.api.common.Condition.Type#DeprecationWarning} condition.
 *
 * @see DeprecationCheckContext
 */
public class AbsentSpecDeprecationChecker implements DeprecationChecker<KafkaProxySpec, KafkaProxyStatus, KafkaProxy, StatusFactory<KafkaProxy>> {

    private static final String MESSAGE = "No spec, please add an empty one. Support for spec-less KafkaProxy resources is deprecated and will be removed in a future release.";

    public void check(DeprecationCheckContext<KafkaProxySpec, KafkaProxyStatus, KafkaProxy, StatusFactory<KafkaProxy>> context) {
        var proxy = context.resource();

        if (proxy.getSpec() == null) {
            context.addCondition(Condition.Type.DeprecationWarning, MESSAGE);
        }
    }
}
