/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.javaoperatorsdk.operator.processing.GroupVersionKind;

/**
 * Information about a Kubernetes API and the corresponding filter implementation class
 * @param group The API group
 * @param version The API version
 * @param kind The API kind
 * @param className The implementation class
 */
public record FilterApiDecl(String group, String version, String kind, String className) {

    @JsonCreator
    public static FilterApiDecl fromApiVersion(@JsonProperty("apiVersion") String apiVersion,
                                               @JsonProperty("kind") String kind,
                                               @JsonProperty("class") String className) {
        var index = apiVersion.indexOf("/");
        if (index == -1 || index == apiVersion.length() - 1) {
            throw new IllegalArgumentException("Invalid apiVersion; should look like '<group>/<version>'");
        }
        var group = apiVersion.substring(0, index);
        var version = apiVersion.substring(index + 1);
        return new FilterApiDecl(group, version, kind, className);
    }

    public GroupVersionKind groupVersionKind() {
        return new GroupVersionKind(group, version, kind);
    }
}
