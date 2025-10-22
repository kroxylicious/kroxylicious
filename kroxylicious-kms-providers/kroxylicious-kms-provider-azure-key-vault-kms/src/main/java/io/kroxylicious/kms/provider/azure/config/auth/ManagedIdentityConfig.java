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
 * @param identityServiceHost optional host address for Managed Identity Service (should only be configured for testing), defaults to "169.254.169.254" which is the Azure Instance Metadata Service (IMDS)
 * @param identityServicePort optional port for Managed Identity Service (should only be configured for testing), defaults to null implying no port will be included in requests
 */
@JsonPropertyOrder({ "targetResource", "identityServiceHost", "identityServicePort" })
public record ManagedIdentityConfig(@JsonProperty(required = true) String targetResource,
                                    @JsonInclude(NON_NULL) @Nullable @JsonProperty String identityServiceHost,
                                    @JsonInclude(NON_NULL) @Nullable @JsonProperty Integer identityServicePort) {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedIdentityConfig.class);

    /**
     * The IMDS host IP recommended by Microsoft for use with retrieving Managed Identity tokens (see
     * <a href="https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http">
     *     How to use managed identities for Azure resources on an Azure VM to acquire an access token - Get a token using HTTP
     * </a>). This IP should be used in all cases outside of test scenarios (where a mock host may be configured instead).
     */
    @SuppressWarnings("java:S1313") // Suppress warning about hard-coded IP addresses posing a security risk, it's what Microsoft say to use so there's no way around it here.
    public static final String DEFAULT_IMDS_HOST = "169.254.169.254";

    public ManagedIdentityConfig {
        Objects.requireNonNull(targetResource, "targetResource cannot be null");
        if (identityServicePort != null && (identityServicePort < 1 || identityServicePort > 65535)) {
            throw new IllegalArgumentException("identityServicePort must be in the range (1, 65535) inclusive");
        }
        if (identityServiceHost != null) {
            String host = URI.create("http://" + identityServiceHost).getHost();
            if (!Objects.equals(host, identityServiceHost)) {
                throw new IllegalArgumentException("identityServiceHost '" + identityServiceHost + "' is not a valid host (host is '" + host + "')");
            }
            LOG.warn("identityServiceHost {} has been configured, this property should not be used in production", identityServiceHost);
        }
        if (identityServicePort != null) {
            LOG.warn("identityServicePort {} has been configured, this property should not be used in production", identityServicePort);
        }
    }

    public String identityServiceURL() {
        String host = identityServiceHost == null ? DEFAULT_IMDS_HOST : identityServiceHost;
        String portSpec = identityServicePort == null ? "" : ":" + identityServicePort;
        return "http://" + host + portSpec;
    }
}
