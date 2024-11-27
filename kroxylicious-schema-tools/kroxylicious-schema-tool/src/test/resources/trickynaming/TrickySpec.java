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

    @com.fasterxml.jackson.annotation.JsonProperty(value = "group")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String group;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "scope")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String scope;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "names")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private trickynaming.TrickySpecNames names;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "versions")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<trickynaming.TrickySpecVersion> versions;

    /**
     * The group of the API being defined
     * @return The value of this object's group.
     */
    public java.lang.String getGroup() {
        return this.group;
    }

    /**
     * The group of the API being defined
     *  @param group The new value for this object's group.
     */
    public void setGroup(java.lang.String group) {
        this.group = group;
    }

    /**
     * The scope of the API being defined
     * @return The value of this object's scope.
     */
    public java.lang.String getScope() {
        return this.scope;
    }

    /**
     * The scope of the API being defined
     *  @param scope The new value for this object's scope.
     */
    public void setScope(java.lang.String scope) {
        this.scope = scope;
    }

    /**
     * The names of the API being defined
     * @return The value of this object's names.
     */
    public trickynaming.TrickySpecNames getNames() {
        return this.names;
    }

    /**
     * The names of the API being defined
     *  @param names The new value for this object's names.
     */
    public void setNames(trickynaming.TrickySpecNames names) {
        this.names = names;
    }

    /**
     * The versions of the API being defined
     * @return The value of this object's versions.
     */
    public java.util.List<trickynaming.TrickySpecVersion> getVersions() {
        return this.versions;
    }

    /**
     * The versions of the API being defined
     *  @param versions The new value for this object's versions.
     */
    public void setVersions(java.util.List<trickynaming.TrickySpecVersion> versions) {
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