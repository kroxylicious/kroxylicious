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

    @com.fasterxml.jackson.annotation.JsonProperty(value = "name")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String name;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "served")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean served;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "storage")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean storage;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "schema")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private trickynaming.CrdSchema schema;

    /**
     * The version name. E.g. `v1alpha1`
     * @return The value of this object's name.
     */
    public java.lang.String getName() {
        return this.name;
    }

    /**
     * The version name. E.g. `v1alpha1`
     *  @param name The new value for this object's name.
     */
    public void setName(java.lang.String name) {
        this.name = name;
    }

    /**
     * Whether the API Server should serve this API
     * @return The value of this object's served.
     */
    public java.lang.Boolean getServed() {
        return this.served;
    }

    /**
     * Whether the API Server should serve this API
     *  @param served The new value for this object's served.
     */
    public void setServed(java.lang.Boolean served) {
        this.served = served;
    }

    /**
     * Whether this version should be used for storage (in etcd).
     * Only a single version may have `storage: true`.
     *
     * @return The value of this object's storage.
     */
    public java.lang.Boolean getStorage() {
        return this.storage;
    }

    /**
     * Whether this version should be used for storage (in etcd).
     * Only a single version may have `storage: true`.
     *
     *  @param storage The new value for this object's storage.
     */
    public void setStorage(java.lang.Boolean storage) {
        this.storage = storage;
    }

    /**
     * The schema of this version
     * @return The value of this object's schema.
     */
    public trickynaming.CrdSchema getSchema() {
        return this.schema;
    }

    /**
     * The schema of this version
     *  @param schema The new value for this object's schema.
     */
    public void setSchema(trickynaming.CrdSchema schema) {
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