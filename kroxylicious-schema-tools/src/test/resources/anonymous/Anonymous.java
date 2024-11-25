/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package anonymous;

/**
 * An object with anonymous typed properties
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "obj", "weasels", "ref" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class Anonymous {

    @com.fasterxml.jackson.annotation.JsonProperty("obj")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private anonymous.AnonymousObj obj;

    @com.fasterxml.jackson.annotation.JsonProperty("weasels")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<anonymous.AnonymousWeasel> weasels;

    @com.fasterxml.jackson.annotation.JsonProperty("ref")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private anonymous.ViaRef ref;

    public anonymous.AnonymousObj getObj() {
        return this.obj;
    }

    public void setObj(anonymous.AnonymousObj obj) {
        this.obj = obj;
    }

    public java.util.List<anonymous.AnonymousWeasel> getWeasels() {
        return this.weasels;
    }

    public void setWeasels(java.util.List<anonymous.AnonymousWeasel> weasels) {
        this.weasels = weasels;
    }

    public anonymous.ViaRef getRef() {
        return this.ref;
    }

    public void setRef(anonymous.ViaRef ref) {
        this.ref = ref;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "Anonymous[" + "obj: " + this.obj + ", weasels: " + this.weasels + ", ref: " + this.ref + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.obj, this.weasels, this.ref);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof anonymous.Anonymous otherAnonymous)
            return java.util.Objects.equals(this.obj, otherAnonymous.obj) && java.util.Objects.equals(this.weasels, otherAnonymous.weasels) && java.util.Objects.equals(this.ref, otherAnonymous.ref);
        else
            return false;
    }
}