/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.config;

import io.javaoperatorsdk.operator.processing.GroupVersionKind;

/**
 * Information about a Kubernetes API and the corresponding filter implementation class
 * @param group The API group
 * @param version The API version
 * @param kind The API kind
 */
public record FilterApiDecl(String group, String version, String kind) {

    public static FilterApiDecl GENERIC_FILTER = new FilterApiDecl("filter.kroxylicious.io", "v1alpha1", "KafkaProtocolFilter");

    public GroupVersionKind groupVersionKind() {
        return new GroupVersionKind(group, version, kind);
    }
}
