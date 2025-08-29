/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config.auth;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

@JsonPropertyOrder({ "oauthEndpoint", "tenantId", "clientId", "clientSecret", "scope", "tls" })
public record EntraIdentityConfig(@JsonInclude(JsonInclude.Include.NON_NULL) @Nullable String oauthEndpoint,
                                  @JsonProperty(required = true) String tenantId,
                                  @JsonProperty(required = true) PasswordProvider clientId,
                                  @JsonProperty(required = true) PasswordProvider clientSecret,
                                  @JsonInclude(JsonInclude.Include.NON_NULL) @Nullable String scope,
                                  @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty(value = "tls") @Nullable Tls tls) {

    private static final Logger LOG = LoggerFactory.getLogger(EntraIdentityConfig.class);
    public EntraIdentityConfig {
        Objects.requireNonNull(tenantId, "tenantId cannot be null");
        Objects.requireNonNull(clientId, "clientId cannot be null");
        Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
        if (oauthEndpoint != null && !oauthEndpoint.startsWith("https://")) {
            LOG.warn("oauthEndpoint {} does not begin with https://, production installations should use a secure endpoint", oauthEndpoint);
        }
        // check that getting password doesn't throw
        clientSecret.getProvidedPassword();
        clientId.getProvidedPassword();
    }

    @JsonIgnore
    public String getOauthEndpointOrDefault() {
        return oauthEndpoint == null ? "https://login.microsoftonline.com" : oauthEndpoint;
    }

    @JsonIgnore
    public String getAuthScope() {
        return scope == null ? "https://vault.azure.net/.default" : scope;
    }
}
