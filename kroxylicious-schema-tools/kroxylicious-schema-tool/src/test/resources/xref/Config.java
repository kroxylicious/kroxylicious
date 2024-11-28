/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package xref;

/**
 * The AWS KMS configuration schema
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "endpointUrl", "accessKey", "secretKey", "region", "tls" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class Config {

    @com.fasterxml.jackson.annotation.JsonProperty(value = "endpointUrl", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String endpointUrl;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "accessKey")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private xref.PasswordProvider accessKey;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "secretKey")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private xref.PasswordProvider secretKey;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "region")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String region;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "tls")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private xref.Tls tls;

    /**
     * Return the endpointUrl.
     *
     * @return The value of this object's endpointUrl.
     */
    public java.lang.String getEndpointUrl() {
        return this.endpointUrl;
    }

    /**
     * Set the endpointUrl.
     *
     *  @param endpointUrl The new value for this object's endpointUrl.
     */
    public void setEndpointUrl(java.lang.String endpointUrl) {
        this.endpointUrl = endpointUrl;
    }

    /**
     * Specification of a password
     * @return The value of this object's accessKey.
     */
    public xref.PasswordProvider getAccessKey() {
        return this.accessKey;
    }

    /**
     * Specification of a password
     *  @param accessKey The new value for this object's accessKey.
     */
    public void setAccessKey(xref.PasswordProvider accessKey) {
        this.accessKey = accessKey;
    }

    /**
     * Specification of a password
     * @return The value of this object's secretKey.
     */
    public xref.PasswordProvider getSecretKey() {
        return this.secretKey;
    }

    /**
     * Specification of a password
     *  @param secretKey The new value for this object's secretKey.
     */
    public void setSecretKey(xref.PasswordProvider secretKey) {
        this.secretKey = secretKey;
    }

    /**
     * AWS region
     * @return The value of this object's region.
     */
    public java.lang.String getRegion() {
        return this.region;
    }

    /**
     * AWS region
     *  @param region The new value for this object's region.
     */
    public void setRegion(java.lang.String region) {
        this.region = region;
    }

    /**
     * Return the tls.
     *
     * @return The value of this object's tls.
     */
    public xref.Tls getTls() {
        return this.tls;
    }

    /**
     * Set the tls.
     *
     *  @param tls The new value for this object's tls.
     */
    public void setTls(xref.Tls tls) {
        this.tls = tls;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "Config[" + "endpointUrl: " + this.endpointUrl + ", accessKey: " + this.accessKey + ", secretKey: " + this.secretKey + ", region: " + this.region + ", tls: " + this.tls + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.endpointUrl, this.accessKey, this.secretKey, this.region, this.tls);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof xref.Config otherConfig)
            return java.util.Objects.equals(this.endpointUrl, otherConfig.endpointUrl) && java.util.Objects.equals(this.accessKey, otherConfig.accessKey) && java.util.Objects.equals(this.secretKey, otherConfig.secretKey) && java.util.Objects.equals(this.region, otherConfig.region) && java.util.Objects.equals(this.tls, otherConfig.tls);
        else
            return false;
    }
}