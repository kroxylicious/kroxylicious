/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common.tls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * TLS cipher suite controls.
 */
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "allow", "deby" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
@javax.annotation.processing.Generated("io.fabric8.java.generator.CRGeneratorRunner")
@lombok.ToString()
@lombok.EqualsAndHashCode()
@io.sundr.builder.annotations.Buildable(editableEnabled = false, validationEnabled = false, generateBuilderPackage = false, builderPackage = "io.fabric8.kubernetes.api.builder", refs = {
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.ObjectMeta.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.ObjectReference.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.LabelSelector.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.Container.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.EnvVar.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.ContainerPort.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.Volume.class),
        @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.VolumeMount.class)
})
public class CipherSuites implements io.fabric8.kubernetes.api.builder.Editable<CipherSuitesBuilder>, io.fabric8.kubernetes.api.model.KubernetesResource {

    @Override
    public CipherSuitesBuilder edit() {
        return new CipherSuitesBuilder(this);
    }

    /**
     * The cipher suites to allow.
     * If not specified a default list of ciphers, which depends on the runtime and enabled protocols, will be used.
     */
    @JsonProperty("allow")
    @JsonPropertyDescription("The cipher suites to allow.\nIf not specified a default list of ciphers, which depends on the runtime and enabled protocols, will be used.\n")
    @JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<String> allow;

    /**
     * List of cipher suites to allow.
     *
     * @return list of cipher suite names.
     */
    public java.util.List<String> getAllow() {
        return allow;
    }

    /**
     * Sets the list of cipher suites to allow.
     *
     * @param allow list of cipher suites.
     */
    public void setAllow(java.util.List<String> allow) {
        this.allow = allow;
    }

    /**
     * The list of cipher suites to deny.
     * If not specified this will default to the empty list.
     */
    @com.fasterxml.jackson.annotation.JsonProperty("deny")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("The cipher suites to deny.\nIf not specified this will default to the empty list.\n")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private List<String> deny = io.fabric8.kubernetes.client.utils.Serialization.unmarshal("[]", java.util.List.class);

    /**
     *  List of cipher suites to deny.
     *
     * @return list of cipher suites.
     */
    public List<String> getDeny() {
        return deny;
    }

    /**
     * Sets the list of cipher suites to deny
     *
     * @param deny list of cipher suites.
     */
    public void setDeny(List<String> deny) {
        this.deny = deny;
    }
}
