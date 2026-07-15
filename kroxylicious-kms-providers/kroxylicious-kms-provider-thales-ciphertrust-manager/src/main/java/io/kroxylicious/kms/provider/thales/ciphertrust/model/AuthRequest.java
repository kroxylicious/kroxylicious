/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Request model for CipherTrust Manager authentication.
 * Supports:
 * <ul>
 *   <li>User authentication with username/password (grant_type=password)</li>
 *   <li>Token refresh (grant_type=refresh_token)</li>
 *   <li>Client certificate authentication (grant_type=client_credential)</li>
 * </ul>
 *
 * @param username username for initial authentication
 * @param password password for initial authentication
 * @param refreshToken refresh token for token refresh
 * @param grantType optional grant type
 * @param refreshTokenLifetime optional refresh token lifetime
 * @param refreshTokenRevokeUnusedIn optional refresh token revoke unused duration
 * @param renewRefreshToken optional flag to renew refresh token
 * @param authDomain optional auth domain
 * @param clientId client ID for client certificate authentication
 * @param connection optional connection
 * @param cookies optional cookies flag
 * @param domain optional domain
 * @param labels optional labels
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record AuthRequest(
                          @JsonProperty("username") @Nullable String username,
                          @JsonProperty("password") @Nullable String password,
                          @JsonProperty("refresh_token") @Nullable String refreshToken,
                          @JsonProperty("grant_type") @Nullable String grantType,
                          @JsonProperty("refresh_token_lifetime") @Nullable Integer refreshTokenLifetime,
                          @JsonProperty("refresh_token_revoke_unused_in") @Nullable Integer refreshTokenRevokeUnusedIn,
                          @JsonProperty("renew_refresh_token") @Nullable Boolean renewRefreshToken,
                          @JsonProperty("auth_domain") @Nullable String authDomain,
                          @JsonProperty("client_id") @Nullable String clientId,
                          @JsonProperty("connection") @Nullable String connection,
                          @JsonProperty("cookies") @Nullable Boolean cookies,
                          @JsonProperty("domain") @Nullable String domain,
                          @JsonProperty("labels") @Nullable List<String> labels) {

    /**
     * Create an auth request for initial authentication with username and password.
     *
     * @param username the username
     * @param password the password
     * @param domain   optional domain name; when non-null the token is scoped to that domain
     * @return auth request
     */
    public static AuthRequest withPassword(String username, String password, @Nullable String domain) {
        return new AuthRequest(username, password, null, "password", null, null, null, null, null, null, null, domain, null);
    }

    /**
     * Create an auth request for token refresh.
     *
     * @param refreshToken the refresh token
     * @return auth request
     */
    public static AuthRequest withRefreshToken(String refreshToken) {
        return new AuthRequest(null, null, refreshToken, "refresh_token", null, null, null, null, null, null, null, null, null);
    }

    /**
     * Create an auth request for client certificate authentication.
     * <p>
     * The client certificate must be presented during the TLS handshake.
     * The grant_type is set to "client_credential" (singular, as required by CTM API).
     * </p>
     *
     * @param clientId the client ID obtained during client registration
     * @return auth request
     */
    public static AuthRequest withClientCredential(String clientId) {
        return new AuthRequest(null, null, null, "client_credential", null, null, null, null, clientId, null, null, null, null);
    }

    @Override
    @SuppressWarnings("java:S2068")
    public String toString() {
        return "AuthRequest{" +
                "username='" + username + '\'' +
                ", password='********'" +
                ", refreshToken='********'" +
                ", grantType='" + grantType + '\'' +
                ", refreshTokenLifetime=" + refreshTokenLifetime +
                ", refreshTokenRevokeUnusedIn=" + refreshTokenRevokeUnusedIn +
                ", renewRefreshToken=" + renewRefreshToken +
                ", authDomain='" + authDomain + '\'' +
                ", clientId='" + clientId + '\'' +
                ", connection='" + connection + '\'' +
                ", cookies=" + cookies +
                ", domain='" + domain + '\'' +
                ", labels=" + labels +
                '}';
    }
}
