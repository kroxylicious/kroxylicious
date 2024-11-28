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

    @edu.umd.cs.findbugs.annotations.Nullable()
    private anonymous.AnonymousObj obj;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.util.List<anonymous.AnonymousWeasel> weasels;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private anonymous.ViaRef ref;

    /**
     * Required properties constructor.
     */
    public Anonymous() {
    }

    /**
     * All properties constructor.
     * @param obj The value of the {@code obj} property. This is an optional property.
     * @param weasels The value of the {@code weasels} property. This is an optional property.
     * @param ref The value of the {@code ref} property. This is an optional property.
     */
    public Anonymous(@edu.umd.cs.findbugs.annotations.Nullable() anonymous.AnonymousObj obj, @edu.umd.cs.findbugs.annotations.Nullable() java.util.List<anonymous.AnonymousWeasel> weasels, @edu.umd.cs.findbugs.annotations.Nullable() anonymous.ViaRef ref) {
        this.obj = obj;
        this.weasels = weasels;
        this.ref = ref;
    }

    /**
     * Return the obj.
     *
     * @return The value of this object's obj.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "obj")
    public anonymous.AnonymousObj obj() {
        return this.obj;
    }

    /**
     * Set the obj.
     *
     *  @param obj The new value for this object's obj.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "obj")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void obj(@edu.umd.cs.findbugs.annotations.Nullable() anonymous.AnonymousObj obj) {
        this.obj = obj;
    }

    /**
     * Return the weasels.
     *
     * @return The value of this object's weasels.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "weasels")
    public java.util.List<anonymous.AnonymousWeasel> weasels() {
        return this.weasels;
    }

    /**
     * Set the weasels.
     *
     *  @param weasels The new value for this object's weasels.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "weasels")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void weasels(@edu.umd.cs.findbugs.annotations.Nullable() java.util.List<anonymous.AnonymousWeasel> weasels) {
        this.weasels = weasels;
    }

    /**
     * Return the ref.
     *
     * @return The value of this object's ref.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "ref")
    public anonymous.ViaRef ref() {
        return this.ref;
    }

    /**
     * Set the ref.
     *
     *  @param ref The new value for this object's ref.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "ref")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void ref(@edu.umd.cs.findbugs.annotations.Nullable() anonymous.ViaRef ref) {
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
