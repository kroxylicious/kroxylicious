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
 * Configuration for the provider that obtains {@link Credentials} from AWS STS by exchanging a
 * Kubernetes service-account OIDC token via the
 * <a href="https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html">AssumeRoleWithWebIdentity</a>
 * action.  This is the IRSA (IAM Roles for Service Accounts) flow used on Amazon EKS.
 * <p>
 * All fields are optional in the YAML.  Fields left {@code null} fall back to the standard EKS
 * environment variables that the pod-identity webhook injects:
 * </p>
 * <ul>
 *   <li>{@code roleArn} &rarr; {@code AWS_ROLE_ARN}</li>
 *   <li>{@code webIdentityTokenFile} &rarr; {@code AWS_WEB_IDENTITY_TOKEN_FILE}</li>
 *   <li>{@code roleSessionName} &rarr; {@code AWS_ROLE_SESSION_NAME} (or generated if both unset)</li>
 *   <li>{@code stsRegion} &rarr; {@code AWS_REGION} or, failing that, the surrounding {@link Config#region()}</li>
 *   <li>{@code stsEndpointUrl} &rarr; {@code https://sts.<stsRegion>.amazonaws.com}</li>
 * </ul>
 *
 * @param roleArn IAM role to assume.
 * @param webIdentityTokenFile path to the projected service-account OIDC token file.
 * @param roleSessionName session name reported to AWS CloudTrail (must match {@code [\w+=,.@-]*}, max 64 chars).
 * @param stsEndpointUrl override of the STS endpoint URL.  Defaults to {@code https://sts.<Config.region>.amazonaws.com}.
 *                        Override for non-standard partitions (e.g. China: {@code sts.<region>.amazonaws.com.cn}).
 * @param durationSeconds requested duration of the assumed-role session (900–43200s).  When {@code null}, STS picks the role's max session duration.
 * @param credentialLifetimeFactor the factor applied to determine how long until a credential is preemptively refreshed.
 */
public record WebIdentityCredentialsProviderConfig(@JsonProperty(value = "roleArn", required = false) @Nullable String roleArn,
                                                   @JsonProperty(value = "webIdentityTokenFile", required = false) @Nullable Path webIdentityTokenFile,
                                                   @JsonProperty(value = "roleSessionName", required = false) @Nullable String roleSessionName,
                                                   @JsonProperty(value = "stsEndpointUrl", required = false) @Nullable URI stsEndpointUrl,
                                                   @JsonProperty(value = "durationSeconds", required = false) @Nullable Integer durationSeconds,
                                                   @JsonProperty(value = "credentialLifetimeFactor", required = false) @Nullable Double credentialLifetimeFactor) {

}
