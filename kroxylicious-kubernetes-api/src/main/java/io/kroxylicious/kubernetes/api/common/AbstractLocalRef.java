/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;

import edu.umd.cs.findbugs.annotations.Nullable;

abstract class AbstractLocalRef extends LocalRef<HasMetadata> implements KubernetesResource {
    @com.fasterxml.jackson.annotation.JsonProperty("group")
    @io.fabric8.generator.annotation.Pattern("^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private String group;
    @com.fasterxml.jackson.annotation.JsonProperty("kind")
    @io.fabric8.generator.annotation.Pattern("^[a-zA-Z]([-a-zA-Z0-9]*[a-zA-Z0-9])?$")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private String kind;
    @com.fasterxml.jackson.annotation.JsonProperty("name")
    @io.fabric8.generator.annotation.Required()
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private String name;

    @Override
    @Nullable
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Override
    @Nullable
    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String toString() {
        return this.getClass() + "(group=" + this.getGroup() + ", kind=" + this.getKind() + ", name=" + this.getName() + ")";
    }
}
