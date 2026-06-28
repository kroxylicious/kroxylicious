/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.net.URI;
import java.nio.file.Path;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.provider.aws.kms.credentials.Credentials;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the provider that obtains {@link Credentials} from the
 * <a href="https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html">EKS Pod Identity</a>
 * Agent.  This is the AWS-recommended successor to IRSA: a local agent (typically reachable on the
 * link-local address {@code http://169.254.170.23/v1/credentials}) returns temporary credentials
 * after authenticating the request with a projected service-account token.
 * <p>
 * All fields are optional in the YAML.  Fields left {@code null} fall back to the standard
 * environment variables that the EKS pod-identity webhook injects into the pod:
 * </p>
 * <ul>
 *   <li>{@code credentialsFullUri} &rarr; {@code AWS_CONTAINER_CREDENTIALS_FULL_URI}</li>
 *   <li>{@code authorizationTokenFile} &rarr; {@code AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE}</li>
 * </ul>
 *
 * @param credentialsFullUri URL of the Pod Identity Agent credentials endpoint.
 * @param authorizationTokenFile path to the projected token file used as the bearer credential.
 * @param credentialLifetimeFactor the factor applied to determine how long until a credential is preemptively refreshed.
 */
public record PodIdentityCredentialsProviderConfig(@JsonProperty(value = "credentialsFullUri", required = false) @Nullable URI credentialsFullUri,
                                                   @JsonProperty(value = "authorizationTokenFile", required = false) @Nullable Path authorizationTokenFile,
                                                   @JsonProperty(value = "credentialLifetimeFactor", required = false) @Nullable Double credentialLifetimeFactor) {

}
