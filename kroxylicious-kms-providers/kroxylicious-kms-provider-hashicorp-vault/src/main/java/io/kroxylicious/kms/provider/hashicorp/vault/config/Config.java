/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.net.URI;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the Vault KMS service.
 *
 * <p>Use the {@code credentials} node to specify the authentication method:
 * <ul>
 *   <li>{@code credentials.vaultToken} — static Vault token (replaces the deprecated top-level {@code vaultToken}).</li>
 *   <li>{@code credentials.kubernetes} — Kubernetes ServiceAccount JWT token auth.</li>
 * </ul>
 *
 * @param vaultUrl              URL of the Vault server e.g. {@code https://myhashicorpvault:8200}
 * @param vaultNamespace        the Vault Enterprise namespace path, if required
 * @param transitEnginePath     the Vault transit engine path, defaults to {@code transit}
 * @param vaultTransitEngineUrl URL of the Vault Transit Engine e.g. {@code https://myhashicorpvault:8200/v1/transit}
 * @param vaultToken            the password provider that will provide the Vault token. Deprecated: use {@code credentials.vaultToken} instead.
 * @param credentials           grouped credential provider configuration.
 * @param tls                   TLS configuration used when connecting to Vault, or {@code null} if platform defaults are to be used.
 */
public record Config(
                     @JsonProperty(value = "vaultUrl", required = false) @Nullable URI vaultUrl,
                     @JsonProperty(value = "vaultNamespace", required = false) @Nullable String vaultNamespace,
                     @JsonProperty(value = "transitEnginePath", required = false) @Nullable String transitEnginePath,
                     @Deprecated(since = "0.25.0", forRemoval = true) @JsonProperty(value = "vaultTransitEngineUrl", required = false, access = Access.WRITE_ONLY) @Nullable URI vaultTransitEngineUrl,
                     @Deprecated(since = "0.25.0", forRemoval = true) @JsonProperty(value = "vaultToken", required = false, access = Access.WRITE_ONLY) @Nullable PasswordProvider vaultToken,
                     @JsonProperty(value = "credentials", required = false) @Nullable VaultCredentialsConfig credentials,
                     @JsonProperty(value = "tls", required = false) @Nullable Tls tls) {

    /**
     * Validates and normalizes the configuration components.
     */
    public Config {
        boolean usesLegacyUrl = vaultTransitEngineUrl != null;
        boolean usesModernUrl = vaultUrl != null;
        boolean usesLegacyCredentials = vaultToken != null;
        boolean usesModernCredentials = credentials != null;

        if (usesLegacyUrl && usesModernCredentials) {
            throw new IllegalArgumentException("Cannot mix deprecated 'vaultTransitEngineUrl' with modern 'credentials' - use 'vaultUrl' instead");
        }
        if (usesModernUrl && usesLegacyCredentials) {
            throw new IllegalArgumentException("Cannot mix modern 'vaultUrl' with deprecated 'vaultToken' - use 'credentials.vaultToken' instead");
        }
        if (usesModernUrl && usesLegacyUrl) {
            throw new IllegalArgumentException("Cannot specify both 'vaultUrl' and deprecated 'vaultTransitEngineUrl'");
        }
        if (!usesModernUrl && !usesLegacyUrl) {
            throw new IllegalArgumentException("Either 'vaultUrl' or deprecated 'vaultTransitEngineUrl' must be provided");
        }
        if (usesLegacyCredentials && usesModernCredentials) {
            throw new IllegalArgumentException("Cannot specify both deprecated 'vaultToken' and 'credentials' - use 'credentials.vaultToken' instead");
        }
        if (!usesLegacyCredentials && !usesModernCredentials) {
            throw new IllegalArgumentException("Either 'credentials' or deprecated 'vaultToken' must be provided");
        }

        if (credentials == null) {
            credentials = new VaultCredentialsConfig(new TokenCredentialsConfig(vaultToken), null);
        }
        if (vaultTransitEngineUrl != null) {
            vaultUrl = extractVaultUrl(vaultTransitEngineUrl);
            if (transitEnginePath == null) {
                transitEnginePath = extractTransitPath(vaultTransitEngineUrl);
            }
        }
        else if (transitEnginePath == null) {
            transitEnginePath = "transit";
        }
    }

    private static URI extractVaultUrl(URI vaultTransitEngineUrl) {
        String scheme = vaultTransitEngineUrl.getScheme();
        String authority = vaultTransitEngineUrl.getAuthority();
        if (scheme == null || authority == null) {
            throw new IllegalArgumentException("vaultTransitEngineUrl must include scheme and authority");
        }
        return URI.create(scheme + "://" + authority);
    }

    private static String extractTransitPath(URI vaultTransitEngineUrl) {
        String path = vaultTransitEngineUrl.getPath();
        if (path == null) {
            return "transit";
        }
        if (path.startsWith("/v1/")) {
            path = path.substring(4);
        }
        else if (path.startsWith("v1/")) {
            path = path.substring(3);
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path.isEmpty() ? "transit" : path;
    }

    /**
     * Convenience constructor for configuration using {@link VaultCredentialsConfig}.
     *
     * @param vaultUrl    URL of the Vault server.
     * @param credentials grouped credential provider configuration.
     * @param tls         TLS configuration.
     */
    public Config(URI vaultUrl, VaultCredentialsConfig credentials, @Nullable Tls tls) {
        this(vaultUrl, null, null, null, null, credentials, tls);
    }

    /**
     * Convenience constructor for configuration using deprecated {@link PasswordProvider}.
     *
     * @param vaultTransitEngineUrl URL of the Vault Transit Engine.
     * @param vaultToken            the password provider that will provide the Vault token.
     * @param tls                   TLS configuration.
     */
    @Deprecated(since = "0.25.0", forRemoval = true)
    public Config(URI vaultTransitEngineUrl, PasswordProvider vaultToken, @Nullable Tls tls) {
        this(null, null, null, vaultTransitEngineUrl, vaultToken, null, tls);
    }
}
