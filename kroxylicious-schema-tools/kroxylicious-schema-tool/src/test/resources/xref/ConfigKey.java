/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package xref;

/**
 * Auto-generated class representing the schema at /definitions/Tls/properties/key.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "privateKeyFile", "certificateFile", "storeFile", "storePassword", "keyPassword", "storeType" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class ConfigKey {

    @com.fasterxml.jackson.annotation.JsonProperty(value = "privateKeyFile")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String privateKeyFile;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "certificateFile")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String certificateFile;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "storeFile")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String storeFile;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "storePassword")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private xref.PasswordProvider storePassword;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "keyPassword")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private xref.PasswordProvider keyPassword;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "storeType")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String storeType;

    /**
     * location of a file containing the private key.
     * @return The value of this object's privateKeyFile.
     */
    public java.lang.String getPrivateKeyFile() {
        return this.privateKeyFile;
    }

    /**
     * location of a file containing the private key.
     *  @param privateKeyFile The new value for this object's privateKeyFile.
     */
    public void setPrivateKeyFile(java.lang.String privateKeyFile) {
        this.privateKeyFile = privateKeyFile;
    }

    /**
     * location of a file containing the certificate and intermediates.
     * @return The value of this object's certificateFile.
     */
    public java.lang.String getCertificateFile() {
        return this.certificateFile;
    }

    /**
     * location of a file containing the certificate and intermediates.
     *  @param certificateFile The new value for this object's certificateFile.
     */
    public void setCertificateFile(java.lang.String certificateFile) {
        this.certificateFile = certificateFile;
    }

    /**
     * location of a key store, or reference to a PEM file containing both private-key and certificate/intermediates.
     * @return The value of this object's storeFile.
     */
    public java.lang.String getStoreFile() {
        return this.storeFile;
    }

    /**
     * location of a key store, or reference to a PEM file containing both private-key and certificate/intermediates.
     *  @param storeFile The new value for this object's storeFile.
     */
    public void setStoreFile(java.lang.String storeFile) {
        this.storeFile = storeFile;
    }

    /**
     * Specification of a password
     * @return The value of this object's storePassword.
     */
    public xref.PasswordProvider getStorePassword() {
        return this.storePassword;
    }

    /**
     * Specification of a password
     *  @param storePassword The new value for this object's storePassword.
     */
    public void setStorePassword(xref.PasswordProvider storePassword) {
        this.storePassword = storePassword;
    }

    /**
     * Specification of a password
     * @return The value of this object's keyPassword.
     */
    public xref.PasswordProvider getKeyPassword() {
        return this.keyPassword;
    }

    /**
     * Specification of a password
     *  @param keyPassword The new value for this object's keyPassword.
     */
    public void setKeyPassword(xref.PasswordProvider keyPassword) {
        this.keyPassword = keyPassword;
    }

    /**
     * specifies the server key type.
     * Legal values are those types supported by the platform {@link java.security.KeyStore},
     * and PEM (for X-509 certificates express in PEM format).
     *
     * @return The value of this object's storeType.
     */
    public java.lang.String getStoreType() {
        return this.storeType;
    }

    /**
     * specifies the server key type.
     * Legal values are those types supported by the platform {@link java.security.KeyStore},
     * and PEM (for X-509 certificates express in PEM format).
     *
     *  @param storeType The new value for this object's storeType.
     */
    public void setStoreType(java.lang.String storeType) {
        this.storeType = storeType;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "ConfigKey[" + "privateKeyFile: " + this.privateKeyFile + ", certificateFile: " + this.certificateFile + ", storeFile: " + this.storeFile + ", storePassword: " + this.storePassword + ", keyPassword: " + this.keyPassword + ", storeType: " + this.storeType + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.privateKeyFile, this.certificateFile, this.storeFile, this.storePassword, this.keyPassword, this.storeType);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof xref.ConfigKey otherConfigKey)
            return java.util.Objects.equals(this.privateKeyFile, otherConfigKey.privateKeyFile) && java.util.Objects.equals(this.certificateFile, otherConfigKey.certificateFile) && java.util.Objects.equals(this.storeFile, otherConfigKey.storeFile) && java.util.Objects.equals(this.storePassword, otherConfigKey.storePassword) && java.util.Objects.equals(this.keyPassword, otherConfigKey.keyPassword) && java.util.Objects.equals(this.storeType, otherConfigKey.storeType);
        else
            return false;
    }
}