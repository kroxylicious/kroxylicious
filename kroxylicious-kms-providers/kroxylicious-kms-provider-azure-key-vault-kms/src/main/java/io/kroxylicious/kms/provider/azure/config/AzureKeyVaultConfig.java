/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config;

import java.net.URI;
import java.util.Objects;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.kroxylicious.kms.provider.azure.config.auth.ManagedIdentityCredentialsConfig;
import io.kroxylicious.kms.provider.azure.config.auth.Oauth2ClientCredentialsConfig;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * @param oauth2ClientCredentials optional credentials for authenticating with Entra via OAuth2. Exactly one of either {@code oauth2ClientCredentials} or {@code managedIdentityCredentials} must be specified.
 * @param managedIdentityCredentials optional service details for authenticating with Entra via Azure Managed Identity. Exactly one of either {@code oauth2ClientCredentials} or {@code managedIdentityCredentials} must be specified.
 * @param keyVaultName required name of the key vault to use for encryption, e.g. my-key-vault
 * @param keyVaultHost required host of key vault (without key vault name) e.g. vault.azure.net
 * @param keyVaultScheme optional scheme for making HTTP requests to key vault, default value is 'https'
 * @param keyVaultPort optional port for key vault (typically would only be used for testing), defaults to null implying no port will be included in requests
 * @param tls optional TLS configuration for key vault requests
 */
@JsonPropertyOrder({ "oauth2ClientCredentials", "managedIdentityCredentials", "keyVaultScheme", "keyVaultName", "keyVaultHost", "keyVaultPort", "omitVaultNameFromHost",
        "tls" })
public record AzureKeyVaultConfig(@JsonInclude(NON_NULL) @Nullable @JsonProperty Oauth2ClientCredentialsConfig oauth2ClientCredentials,
                                  @JsonInclude(NON_NULL) @Nullable @JsonProperty ManagedIdentityCredentialsConfig managedIdentityCredentials,
                                  @JsonProperty(required = true) String keyVaultName,
                                  @JsonProperty(required = true) String keyVaultHost,
                                  @JsonInclude(NON_NULL) @Nullable @JsonProperty String keyVaultScheme,
                                  @JsonInclude(NON_NULL) @Nullable @JsonProperty Integer keyVaultPort,
                                  @JsonInclude(NON_NULL) @JsonProperty(value = "tls") @Nullable Tls tls) {

    private static final Logger LOG = LoggerFactory.getLogger(AzureKeyVaultConfig.class);
    private static final Pattern VAULT_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-]{3,24}$");

    public AzureKeyVaultConfig {
        Objects.requireNonNull(keyVaultName);
        Objects.requireNonNull(keyVaultHost);
        if (oauth2ClientCredentials != null && managedIdentityCredentials != null) {
            throw new IllegalArgumentException(
                    "more than one authentication method specified, must configure exactly one of either oauth2ClientCredentials or managedIdentityCredentials");
        }
        else if (oauth2ClientCredentials == null && managedIdentityCredentials == null) {
            throw new IllegalArgumentException("no authentication specified, must configure exactly one of either oauth2ClientCredentials or managedIdentityCredentials");
        }
        String host = URI.create("https://" + keyVaultHost).getHost();
        if (!Objects.equals(host, keyVaultHost)) {
            throw new IllegalArgumentException("keyVaultHost is not a valid host");
        }
        if (keyVaultPort != null && (keyVaultPort < 1 || keyVaultPort > 65535)) {
            throw new IllegalArgumentException("keyVaultPort must be in the range (1, 65535) inclusive");
        }
        if (keyVaultScheme != null && keyVaultScheme.endsWith("://")) {
            throw new IllegalArgumentException("keyVaultScheme must not end with ://");
        }
        validateKeyVaultName(keyVaultName);
        if (keyVaultHost.isBlank()) {
            throw new IllegalArgumentException("keyVaultHost is blank");
        }
        if (!kvScheme().equals("https")) {
            LOG.warn("keyVaultScheme {} is not https, production installations should use a secure endpoint", keyVaultScheme());
        }
    }

    private static void validateKeyVaultName(String keyVaultName) {
        if (!VAULT_NAME_PATTERN.matcher(keyVaultName).matches()) {
            throw new IllegalArgumentException("keyVaultName does not match pattern " + VAULT_NAME_PATTERN.pattern());
        }
        if (keyVaultName.startsWith("-")) {
            throw new IllegalArgumentException("keyVaultName must not start with '-'");
        }
        if (keyVaultName.endsWith("-")) {
            throw new IllegalArgumentException("keyVaultName must not end with '-'");
        }
        if (keyVaultName.contains("--")) {
            throw new IllegalArgumentException("keyVaultName must not contain '--'");
        }
    }

    @VisibleForTesting
    String kvScheme() {
        return keyVaultScheme == null ? "https" : keyVaultScheme;
    }

    public String keyVaultUrl(String vaultName) {
        String host = (vaultName + ".") + keyVaultHost;
        String portSpec = keyVaultPort() == null ? "" : ":" + keyVaultPort();
        return kvScheme() + "://" + host + portSpec;
    }
}
