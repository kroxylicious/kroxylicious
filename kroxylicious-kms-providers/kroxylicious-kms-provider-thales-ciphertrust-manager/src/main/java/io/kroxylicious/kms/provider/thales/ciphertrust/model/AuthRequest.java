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
 * Supports both initial authentication (username/password) and token refresh (refresh_token).
 *
 * @param username username for initial authentication
 * @param password password for initial authentication
 * @param refreshToken refresh token for token refresh
 * @param grantType optional grant type
 * @param refreshTokenLifetime optional refresh token lifetime
 * @param refreshTokenRevokeUnusedIn optional refresh token revoke unused duration
 * @param renewRefreshToken optional flag to renew refresh token
 * @param authDomain optional auth domain
 * @param clientId optional client ID
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
     * @return auth request
     */
    public static AuthRequest withPassword(String username, String password) {
        return new AuthRequest(username, password, null, "password", null, null, null, null, null, null, null, null, null);
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

    @Override
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
