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

    @edu.umd.cs.findbugs.annotations.NonNull
    private java.lang.String endpointUrl;

    @edu.umd.cs.findbugs.annotations.Nullable
    private xref.PasswordProvider accessKey;

    @edu.umd.cs.findbugs.annotations.Nullable
    private xref.PasswordProvider secretKey;

    @edu.umd.cs.findbugs.annotations.Nullable
    private java.lang.String region;

    @edu.umd.cs.findbugs.annotations.Nullable
    private xref.Tls tls;

    /**
     * All properties constructor.
     * @param endpointUrl The value of the {@code endpointUrl} property. This is a required property.
     * @param accessKey The value of the {@code accessKey} property. This is an optional property.
     * @param secretKey The value of the {@code secretKey} property. This is an optional property.
     * @param region The value of the {@code region} property. This is an optional property.
     * @param tls The value of the {@code tls} property. This is an optional property.
     */
    @com.fasterxml.jackson.annotation.JsonCreator
    public Config(@com.fasterxml.jackson.annotation.JsonProperty(value = "endpointUrl", required = true) @edu.umd.cs.findbugs.annotations.NonNull java.lang.String endpointUrl, @com.fasterxml.jackson.annotation.JsonProperty(value = "accessKey") @edu.umd.cs.findbugs.annotations.Nullable xref.PasswordProvider accessKey, @com.fasterxml.jackson.annotation.JsonProperty(value = "secretKey") @edu.umd.cs.findbugs.annotations.Nullable xref.PasswordProvider secretKey, @com.fasterxml.jackson.annotation.JsonProperty(value = "region") @edu.umd.cs.findbugs.annotations.Nullable java.lang.String region, @com.fasterxml.jackson.annotation.JsonProperty(value = "tls") @edu.umd.cs.findbugs.annotations.Nullable xref.Tls tls) {
        this.endpointUrl = java.util.Objects.requireNonNull(endpointUrl);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.tls = tls;
    }

    /**
     * Return the endpointUrl.
     *
     * @return The value of this object's endpointUrl.
     */
    @edu.umd.cs.findbugs.annotations.NonNull
    @com.fasterxml.jackson.annotation.JsonProperty(value = "endpointUrl", required = true)
    public java.lang.String endpointUrl() {
        return this.endpointUrl;
    }

    /**
     * Set the endpointUrl.
     *
     *  @param endpointUrl The new value for this object's endpointUrl.
     */
    public void endpointUrl(@edu.umd.cs.findbugs.annotations.NonNull java.lang.String endpointUrl) {
        this.endpointUrl = java.util.Objects.requireNonNull(endpointUrl);
    }

    /**
     * Specification of a password
     * @return The value of this object's accessKey.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "accessKey")
    public xref.PasswordProvider accessKey() {
        return this.accessKey;
    }

    /**
     * Specification of a password
     *  @param accessKey The new value for this object's accessKey.
     */
    public void accessKey(@edu.umd.cs.findbugs.annotations.Nullable xref.PasswordProvider accessKey) {
        this.accessKey = accessKey;
    }

    /**
     * Specification of a password
     * @return The value of this object's secretKey.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "secretKey")
    public xref.PasswordProvider secretKey() {
        return this.secretKey;
    }

    /**
     * Specification of a password
     *  @param secretKey The new value for this object's secretKey.
     */
    public void secretKey(@edu.umd.cs.findbugs.annotations.Nullable xref.PasswordProvider secretKey) {
        this.secretKey = secretKey;
    }

    /**
     * AWS region
     * @return The value of this object's region.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "region")
    public java.lang.String region() {
        return this.region;
    }

    /**
     * AWS region
     *  @param region The new value for this object's region.
     */
    public void region(@edu.umd.cs.findbugs.annotations.Nullable java.lang.String region) {
        this.region = region;
    }

    /**
     * Return the tls.
     *
     * @return The value of this object's tls.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "tls")
    public xref.Tls tls() {
        return this.tls;
    }

    /**
     * Set the tls.
     *
     *  @param tls The new value for this object's tls.
     */
    public void tls(@edu.umd.cs.findbugs.annotations.Nullable xref.Tls tls) {
        this.tls = tls;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "Config[" + "endpointUrl: " + this.endpointUrl + ", accessKey: " + this.accessKey + ", secretKey: " + this.secretKey + ", region: " + this.region + ", tls: " + this.tls + "]";
    }

    @java.lang.Override
    public int hashCode() {
        return java.util.Objects.hash(this.endpointUrl, this.accessKey, this.secretKey, this.region, this.tls);
    }

    @java.lang.Override
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof xref.Config otherConfig)
            return java.util.Objects.equals(this.endpointUrl, otherConfig.endpointUrl) && java.util.Objects.equals(this.accessKey, otherConfig.accessKey) && java.util.Objects.equals(this.secretKey, otherConfig.secretKey) && java.util.Objects.equals(this.region, otherConfig.region) && java.util.Objects.equals(this.tls, otherConfig.tls);
        else
            return false;
    }
}
