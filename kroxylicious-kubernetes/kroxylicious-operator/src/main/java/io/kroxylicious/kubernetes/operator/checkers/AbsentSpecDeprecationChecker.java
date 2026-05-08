/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.operator.OperatorLoggingKeys;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.KafkaProxyStatusFactory;

/**
 * Checks whether a {@link KafkaProxy} resource has a {@code null} spec.
 *
 * <p>A missing spec is deprecated: future releases will require at least an empty spec object.
 * When an absent spec is detected this checker:
 * <ul>
 *   <li>Logs a one-time warning per resource UID, suppressed on subsequent reconcile cycles for
 *       up to one hour to avoid log spam.</li>
 *   <li>Appends a {@link io.kroxylicious.kubernetes.api.common.Condition.Type#DeprecationWarning}
 *       condition to the context's condition list.</li>
 * </ul>
 * Once a resource gains a spec the cached entry is invalidated so that if the spec is later
 * removed again the warning is re-emitted.
 */
public class AbsentSpecDeprecationChecker extends DeprecationChecker<KafkaProxySpec, KafkaProxyStatus, KafkaProxy, KafkaProxyStatusFactory> {
    public void check(DeprecationCheckContext<KafkaProxySpec, KafkaProxyStatus, KafkaProxy, KafkaProxyStatusFactory> context) {
        var proxy = context.resource();

        var resourceUid = Optional.ofNullable(proxy).map(HasMetadata::getMetadata).map(ObjectMeta::getUid);
        resourceUid.ifPresent(uid -> {
            if (proxy.getSpec() == null) {
                if (getLogCache().asMap().putIfAbsent(getCacheKey(uid), true) == null) {
                    context.logger().atWarn()
                            .addKeyValue(OperatorLoggingKeys.KIND, ResourcesUtil.kind(proxy))
                            .addKeyValue(OperatorLoggingKeys.NAME, ResourcesUtil.name(proxy))
                            .addKeyValue(OperatorLoggingKeys.NAMESPACE, ResourcesUtil.namespace(proxy))
                            .log("No spec, please add an empty one. "
                                    + " Support for spec-less KafkaProxy resources is deprecated and will be removed in a future release.");

                }

                context.conditions().add(context.statusFactory().newTrueCondition(proxy, Condition.Type.DeprecationWarning,
                        "Support for spec-less KafkaProxy resources is deprecated and will be removed in a future release."));
            }
            else {
                getLogCache().invalidate(getCacheKey(uid));
            }
        });
    }

    private String getCacheKey(String uid) {
        return "absent-spec/" + uid;
    }
}
