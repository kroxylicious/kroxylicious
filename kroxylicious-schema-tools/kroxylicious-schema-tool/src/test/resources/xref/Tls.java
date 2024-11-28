/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package xref;

/**
 * Auto-generated class representing the schema at /definitions/Tls.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "key", "trust" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class Tls {

    @com.fasterxml.jackson.annotation.JsonProperty(value = "key")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private xref.ConfigKey key;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "trust")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private xref.ConfigTrust trust;

    /**
     * Return the key.
     *
     * @return The value of this object's key.
     */
    public xref.ConfigKey getKey() {
        return this.key;
    }

    /**
     * Set the key.
     *
     *  @param key The new value for this object's key.
     */
    public void setKey(xref.ConfigKey key) {
        this.key = key;
    }

    /**
     * Return the trust.
     *
     * @return The value of this object's trust.
     */
    public xref.ConfigTrust getTrust() {
        return this.trust;
    }

    /**
     * Set the trust.
     *
     *  @param trust The new value for this object's trust.
     */
    public void setTrust(xref.ConfigTrust trust) {
        this.trust = trust;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "Tls[" + "key: " + this.key + ", trust: " + this.trust + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.key, this.trust);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof xref.Tls otherTls)
            return java.util.Objects.equals(this.key, otherTls.key) && java.util.Objects.equals(this.trust, otherTls.trust);
        else
            return false;
    }
}