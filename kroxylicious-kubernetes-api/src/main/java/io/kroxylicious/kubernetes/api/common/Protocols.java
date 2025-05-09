/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import java.util.List;

/**
 * TLS protocol controls.
 */
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "allow", "deny" })
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
public class Protocols implements io.fabric8.kubernetes.api.builder.Editable<ProtocolsBuilder>, io.fabric8.kubernetes.api.model.KubernetesResource {

    @Override
    public ProtocolsBuilder edit() {
        return new ProtocolsBuilder(this);
    }

    /**
     * The TLS protocols to allow, ordered by preference.
     * If not specified this defaults to the TLS protocols of the proxy JVM's default SSL context.
     */
    @com.fasterxml.jackson.annotation.JsonProperty("allow")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("The TLS protocols to allow, ordered by preference.\nIf not specified this defaults to the TLS protocols of the proxy JVM's default SSL context.\n")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<String> allow;

    /**
     * Ordered list of protocols to allow.
     *
     * @return list of protocols.
     */
    public List<String> getAllow() {
        return allow;
    }

    /**
     * Sets the ordered list of protocols to allow.
     *
     * @param allow list of protocols.
     */
    public void setAllow(List<String> allow) {
        this.allow = allow;
    }

    /**
     * The TLS protocols to deny.
     * If not specified this defaults to the empty list.
     */
    @com.fasterxml.jackson.annotation.JsonProperty("deny")
    @com.fasterxml.jackson.annotation.JsonPropertyDescription("The TLS protocols to deny.\nIf not specified this defaults to the empty list.\n")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private List<String> deny = io.fabric8.kubernetes.client.utils.Serialization.unmarshal("[]", java.util.List.class);

    /**
     *  List of protocols to deny.
     *
     * @return list of protocols.
     */
    public List<String> getDeny() {
        return deny;
    }

    /**
     * Sets the list of protocols to deny.
     *
     * @param deny list of protocols.
     */
    public void setDeny(List<String> deny) {
        this.deny = deny;
    }
}
