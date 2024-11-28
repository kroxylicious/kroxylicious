/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package trickynaming;

/**
 * API being defined
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "group", "scope", "names", "versions" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class TrickySpec {

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.lang.String group;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.lang.String scope;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private trickynaming.TrickySpecNames names;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.util.List<trickynaming.TrickySpecVersion> versions;

    /**
     * Required properties constructor.
     */
    public TrickySpec() {
    }

    /**
     * All properties constructor.
     * @param group The value of the {@code group} property. This is an optional property.
     * @param scope The value of the {@code scope} property. This is an optional property.
     * @param names The value of the {@code names} property. This is an optional property.
     * @param versions The value of the {@code versions} property. This is an optional property.
     */
    @com.fasterxml.jackson.annotation.JsonCreator()
    public TrickySpec(@com.fasterxml.jackson.annotation.JsonProperty(value = "group") @edu.umd.cs.findbugs.annotations.Nullable java.lang.String group, @com.fasterxml.jackson.annotation.JsonProperty(value = "scope") @edu.umd.cs.findbugs.annotations.Nullable java.lang.String scope, @com.fasterxml.jackson.annotation.JsonProperty(value = "names") @edu.umd.cs.findbugs.annotations.Nullable trickynaming.TrickySpecNames names, @com.fasterxml.jackson.annotation.JsonProperty(value = "versions") @edu.umd.cs.findbugs.annotations.Nullable java.util.List<trickynaming.TrickySpecVersion> versions) {
        this.group = group;
        this.scope = scope;
        this.names = names;
        this.versions = versions;
    }

    /**
     * The group of the API being defined
     * @return The value of this object's group.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "group")
    public java.lang.String group() {
        return this.group;
    }

    /**
     * The group of the API being defined
     *  @param group The new value for this object's group.
     */
    public void group(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String group) {
        this.group = group;
    }

    /**
     * The scope of the API being defined
     * @return The value of this object's scope.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "scope")
    public java.lang.String scope() {
        return this.scope;
    }

    /**
     * The scope of the API being defined
     *  @param scope The new value for this object's scope.
     */
    public void scope(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String scope) {
        this.scope = scope;
    }

    /**
     * The names of the API being defined
     * @return The value of this object's names.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "names")
    public trickynaming.TrickySpecNames names() {
        return this.names;
    }

    /**
     * The names of the API being defined
     *  @param names The new value for this object's names.
     */
    public void names(@edu.umd.cs.findbugs.annotations.Nullable() trickynaming.TrickySpecNames names) {
        this.names = names;
    }

    /**
     * The versions of the API being defined
     * @return The value of this object's versions.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "versions")
    public java.util.List<trickynaming.TrickySpecVersion> versions() {
        return this.versions;
    }

    /**
     * The versions of the API being defined
     *  @param versions The new value for this object's versions.
     */
    public void versions(@edu.umd.cs.findbugs.annotations.Nullable() java.util.List<trickynaming.TrickySpecVersion> versions) {
        this.versions = versions;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "TrickySpec[" + "group: " + this.group + ", scope: " + this.scope + ", names: " + this.names + ", versions: " + this.versions + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.group, this.scope, this.names, this.versions);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof trickynaming.TrickySpec otherTrickySpec)
            return java.util.Objects.equals(this.group, otherTrickySpec.group) && java.util.Objects.equals(this.scope, otherTrickySpec.scope) && java.util.Objects.equals(this.names, otherTrickySpec.names) && java.util.Objects.equals(this.versions, otherTrickySpec.versions);
        else
            return false;
    }
}
