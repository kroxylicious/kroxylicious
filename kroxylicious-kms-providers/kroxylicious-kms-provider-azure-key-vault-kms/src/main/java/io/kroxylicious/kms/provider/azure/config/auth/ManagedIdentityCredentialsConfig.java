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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import edu.umd.cs.findbugs.annotations.Nullable;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * @param targetResource required App ID URI of the target resource
 * @param identityServiceEndpoint optional base URI for Managed Identity Service endpoint (should only be configured for testing), defaults to "http://169.254.169.254" which is the Azure Instance Metadata Service (IMDS)
 */
@JsonPropertyOrder({ "targetResource", "identityServiceEndpoint" })
public record ManagedIdentityCredentialsConfig(@JsonProperty(required = true) String targetResource,
                                               @JsonInclude(NON_NULL) @Nullable @JsonProperty URI identityServiceEndpoint) {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedIdentityCredentialsConfig.class);

    /**
     * The IMDS host IP recommended by Microsoft for use with retrieving Managed Identity tokens (see
     * <a href="https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http">
     *     How to use managed identities for Azure resources on an Azure VM to acquire an access token - Get a token using HTTP
     * </a>). This IP should be used in all cases outside of test scenarios (where a mock host may be configured instead).
     */
    @SuppressWarnings("java:S1313") // Suppress warning about hard-coded IP addresses posing a security risk, it's what Microsoft say to use so there's no way around it here.
    public static final URI DEFAULT_IMDS_HOST = URI.create("http://169.254.169.254");

    public ManagedIdentityCredentialsConfig {
        Objects.requireNonNull(targetResource, "targetResource cannot be null");
        if (identityServiceEndpoint != null) {
            if (!identityServiceEndpoint.toString().startsWith("http://")) {
                LOG.warn(
                        "identityServiceEndpoint {} does not begin with http://, production installations should not use HTTPS as Azure Instance Metadata Service (IMDS) endpoint is not TLS enabled",
                        identityServiceEndpoint);
            }
            LOG.warn("identityServiceEndpoint {} has been configured, this property should not be used in production", identityServiceEndpoint);
        }
    }

    public URI identityServiceURL() {
        return identityServiceEndpoint == null ? DEFAULT_IMDS_HOST : identityServiceEndpoint;
    }
}
