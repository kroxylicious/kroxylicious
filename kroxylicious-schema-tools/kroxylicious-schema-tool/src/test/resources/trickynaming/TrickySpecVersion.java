/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package trickynaming;

/**
 * An API Version
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "name", "served", "storage", "schema" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class TrickySpecVersion {

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.lang.String name;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.lang.Boolean served;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.lang.Boolean storage;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private trickynaming.CrdSchema schema;

    /**
     * Required properties constructor.
     */
    public TrickySpecVersion() {
    }

    /**
     * All properties constructor.
     * @param name The value of the {@code name} property. This is an optional property.
     * @param served The value of the {@code served} property. This is an optional property.
     * @param storage The value of the {@code storage} property. This is an optional property.
     * @param schema The value of the {@code schema} property. This is an optional property.
     */
    public TrickySpecVersion(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String name, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.Boolean served, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.Boolean storage, @edu.umd.cs.findbugs.annotations.Nullable() trickynaming.CrdSchema schema) {
        this.name = name;
        this.served = served;
        this.storage = storage;
        this.schema = schema;
    }

    /**
     * The version name. E.g. `v1alpha1`
     * @return The value of this object's name.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "name")
    public java.lang.String name() {
        return this.name;
    }

    /**
     * The version name. E.g. `v1alpha1`
     *  @param name The new value for this object's name.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "name")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void name(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String name) {
        this.name = name;
    }

    /**
     * Whether the API Server should serve this API
     * @return The value of this object's served.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "served")
    public java.lang.Boolean served() {
        return this.served;
    }

    /**
     * Whether the API Server should serve this API
     *  @param served The new value for this object's served.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "served")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void served(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.Boolean served) {
        this.served = served;
    }

    /**
     * Whether this version should be used for storage (in etcd).
     * Only a single version may have `storage: true`.
     *
     * @return The value of this object's storage.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "storage")
    public java.lang.Boolean storage() {
        return this.storage;
    }

    /**
     * Whether this version should be used for storage (in etcd).
     * Only a single version may have `storage: true`.
     *
     *  @param storage The new value for this object's storage.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "storage")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void storage(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.Boolean storage) {
        this.storage = storage;
    }

    /**
     * The schema of this version
     * @return The value of this object's schema.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "schema")
    public trickynaming.CrdSchema schema() {
        return this.schema;
    }

    /**
     * The schema of this version
     *  @param schema The new value for this object's schema.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "schema")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void schema(@edu.umd.cs.findbugs.annotations.Nullable() trickynaming.CrdSchema schema) {
        this.schema = schema;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "TrickySpecVersion[" + "name: " + this.name + ", served: " + this.served + ", storage: " + this.storage + ", schema: " + this.schema + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.name, this.served, this.storage, this.schema);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof trickynaming.TrickySpecVersion otherTrickySpecVersion)
            return java.util.Objects.equals(this.name, otherTrickySpecVersion.name) && java.util.Objects.equals(this.served, otherTrickySpecVersion.served) && java.util.Objects.equals(this.storage, otherTrickySpecVersion.storage) && java.util.Objects.equals(this.schema, otherTrickySpecVersion.schema);
        else
            return false;
    }
}
