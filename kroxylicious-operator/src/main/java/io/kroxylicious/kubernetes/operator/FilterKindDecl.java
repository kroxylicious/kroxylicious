/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import io.javaoperatorsdk.operator.processing.GroupVersionKind;

/**
 * Information about an API kind (as it exists in Kube) and the corresponding implementation class
 * @param group
 * @param version
 * @param kind
 * @param klass
 */
public record FilterKindDecl(String group, String version, String kind, String klass) {

    GroupVersionKind groupVersionKind() {
        return new GroupVersionKind(group, version, kind);
    }
}
