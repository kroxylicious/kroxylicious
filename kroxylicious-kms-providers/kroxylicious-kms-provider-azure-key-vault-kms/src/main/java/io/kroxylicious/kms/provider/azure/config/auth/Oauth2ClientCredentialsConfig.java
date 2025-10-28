/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.config.auth;

import java.net.URI;
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
public record Oauth2ClientCredentialsConfig(@JsonInclude(JsonInclude.Include.NON_NULL) @Nullable URI oauthEndpoint,
                                            @JsonProperty(required = true) String tenantId,
                                            @JsonProperty(required = true) PasswordProvider clientId,
                                            @JsonProperty(required = true) PasswordProvider clientSecret,
                                            @JsonProperty(required = true) URI scope,
                                            @JsonInclude(JsonInclude.Include.NON_NULL) @JsonProperty(value = "tls") @Nullable Tls tls) {

    private static final Logger LOG = LoggerFactory.getLogger(Oauth2ClientCredentialsConfig.class);
    public static final URI GLOBAL_ENTRA_ENDPOINT = URI.create("https://login.microsoftonline.com");

    public Oauth2ClientCredentialsConfig {
        Objects.requireNonNull(tenantId, "tenantId cannot be null");
        Objects.requireNonNull(clientId, "clientId cannot be null");
        Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
        Objects.requireNonNull(scope, "scope cannot be null");
        if (oauthEndpoint != null && !oauthEndpoint.getScheme().equalsIgnoreCase("https")) {
            LOG.warn("oauthEndpoint {} does not begin with https://, production installations should use a secure endpoint", oauthEndpoint);
        }
        // check that getting password doesn't throw
        clientSecret.getProvidedPassword();
        clientId.getProvidedPassword();
    }

    @JsonIgnore
    public URI getOauthEndpointOrDefault() {
        return oauthEndpoint == null ? GLOBAL_ENTRA_ENDPOINT : oauthEndpoint;
    }
}
