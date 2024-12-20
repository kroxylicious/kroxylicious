/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.provider.fortanix.dsm.model.DeleteAliasRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.ErrorResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.SecurityObjectRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.SecurityObjectResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.InfoRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.InfoResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.KeyResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.RotateKeyRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.ScheduleKeyDeletionRequest;
import io.kroxylicious.kms.provider.fortanix.dsm.model.ScheduleKeyDeletionResponse;
import io.kroxylicious.kms.provider.fortanix.dsm.model.SessionAuthResponse;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractFortanixDsmKmsTestKmsFacade implements TestKmsFacade<Config, String, FortanixDsmKmsEdek> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFortanixDsmKmsTestKmsFacade.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int MINIMUM_ALLOWED_EXPIRY_DAYS = 7;

    private static final TypeReference<SessionAuthResponse> SESSION_AUTH_RESPONSE = new TypeReference<>() {
    };

    private static final TypeReference<List<KeyResponse>> KEY_LIST_RESPONSE_RESPONSE = new TypeReference<>() {
    };
    private static final TypeReference<SecurityObjectResponse> GENERATE_SECURITY_OBJECT_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<InfoResponse> DESCRIBE_KEY_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<ScheduleKeyDeletionResponse> SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<ErrorResponse> ERROR_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    public static final String KEY_GROUP = "eae3d454-825f-40e6-abd4-1b7e978e1687";
    private final HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();

    private SessionAuthResponse session;

    protected AbstractFortanixDsmKmsTestKmsFacade() {
    }

    protected abstract void startKms();

    protected abstract void stopKms();

    @Override
    public final void start() {
        startKms();
    }

    @NonNull
    protected abstract URI getEndpointUrl();

    @Override
    public Config getKmsServiceConfig() {
        return new Config(getEndpointUrl(), new InlinePassword(getApiKey()), null);
    }

    protected abstract String getApiKey();

    @Override
    public final Class<FortanixDsmKmsService> getKmsServiceClass() {
        return FortanixDsmKmsService.class;
    }

    @Override
    public final void stop() {

        deleteAllkeys();

        stopKms();
    }

    private void deleteAllkeys() {

        var sessionResponse = getSessionAuthResponse();

        var keyListRequest = HttpRequest.newBuilder()
                .uri(getEndpointUrl().resolve("/crypto/v1/keys?group_id=" + KEY_GROUP))
                .header(FortanixDsmKms.AUTHORIZATION_HEADER, sessionResponse.tokenType() + " " + sessionResponse.accessToken())
                .GET()
                .build();

        var keys = sendRequest("", keyListRequest, KEY_LIST_RESPONSE_RESPONSE);
        keys.forEach(k -> {

            var keyDeleteRequest = HttpRequest.newBuilder()
                    .uri(getEndpointUrl().resolve("/crypto/v1/keys/" + k.kid()))
                    .header(FortanixDsmKms.AUTHORIZATION_HEADER, sessionResponse.tokenType() + " " + sessionResponse.accessToken())
                    .DELETE()
                    .build();

            sendRequestExpectingNoResponse(keyDeleteRequest);
        });

    }

    @Override
    public final TestKekManager getTestKekManager() {
        return new FortanixDsmKmsTestKekManager();
    }

    class FortanixDsmKmsTestKekManager implements TestKekManager {

        @Override
        public void generateKek(String alias) {
            var sessionResponse = getSessionAuthResponse();

            var generateRequest = new SecurityObjectRequest(alias, 256, "AES", false, List.of("ENCRYPT", "DECRYPT", "APPMANAGEABLE"), KEY_GROUP);
            var request = createRequest("/crypto/v1/keys", generateRequest, sessionResponse);
            var response = sendRequest(alias, request, GENERATE_SECURITY_OBJECT_RESPONSE_TYPE_REF);
            LOGGER.trace("generateKek {} -> {}", alias, response);

        }

        @Override
        public InfoResponse read(String alias) {
            final InfoRequest describeKey = new InfoRequest(FortanixDsmKms.ALIAS_PREFIX + alias);
            var request = createRequest(null, describeKey, null);
            return sendRequest(alias, request, DESCRIBE_KEY_RESPONSE_TYPE_REF);
        }

        @Override
        public void deleteKek(String alias) {
            var key = read(alias);
            var keyId = key.kid();
            final ScheduleKeyDeletionRequest request = new ScheduleKeyDeletionRequest(keyId, MINIMUM_ALLOWED_EXPIRY_DAYS);
            var scheduleDeleteRequest = createRequest(null, request, null);

            sendRequest(keyId, scheduleDeleteRequest, SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF);

            final DeleteAliasRequest deleteAlias = new DeleteAliasRequest(FortanixDsmKms.ALIAS_PREFIX + alias);
            var deleteAliasRequest = createRequest(null, deleteAlias, null);
            sendRequestExpectingNoResponse(deleteAliasRequest);
        }

        @Override
        public void rotateKek(String alias) {
            var key = read(alias);
            final RotateKeyRequest rotateKey = new RotateKeyRequest(key.kid());
            var rotateKeyRequest = createRequest(null, rotateKey, null);
            try {
                sendRequestExpectingNoResponse(rotateKeyRequest);
            }
            catch (AwsNotImplementException e) {
                pseudoRotate(alias);
            }
        }

        private void pseudoRotate(String alias) {
            // RotateKeyOnDemand is not implemented in localstack.
            // https://docs.localstack.cloud/references/coverage/coverage_kms/#:~:text=Show%20Tests-,RotateKeyOnDemand,-ScheduleKeyDeletion
            // https://github.com/localstack/localstack/issues/10723

            // mimic rotate by creating a new key and repoint the alias at it, leaving the original key in place.
            // final GenerateSecurityObjectRequest request = new GenerateSecurityObjectRequest(alias,);
            // var keyRequest = createRequest(request, null);
            // var createKeyResponse = sendRequest(alias, keyRequest, CREATE_KEY_RESPONSE_TYPE_REF);
            //
            // final UpdateAliasRequest update = new UpdateAliasRequest(createKeyResponse.kid(), FortanixDsmKms.ALIAS_PREFIX + alias);
            // var aliasRequest = createRequest(update, null);
            // sendRequestExpectingNoResponse(aliasRequest);
        }
    }

    private HttpRequest createRequest(String path, Object request, SessionAuthResponse sessionResponse) {
        var body = encodeJson(request).getBytes(UTF_8);

        return HttpRequest.newBuilder()
                .uri(getEndpointUrl().resolve(path))
                .header(FortanixDsmKms.AUTHORIZATION_HEADER, sessionResponse.tokenType() + " " + sessionResponse.accessToken())
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
    }

    private SessionAuthResponse getSessionAuthResponse() {
        if (this.session == null) {
            var sessionRequest = createSessionRequest();
            this.session = sendRequest("session", sessionRequest, SESSION_AUTH_RESPONSE);
        }
        return this.session;
    }

    private HttpRequest createSessionRequest() {

        return HttpRequest.newBuilder()
                .uri(getEndpointUrl().resolve("/sys/v1/session/auth"))
                .header(FortanixDsmKms.AUTHORIZATION_HEADER, "Basic " + getApiKey())
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
    }

    private <R> R sendRequest(String key, HttpRequest request, TypeReference<R> valueTypeRef) {
        try {
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            checkForError(key, request.uri(), response.statusCode(), response);
            return decodeJson(valueTypeRef, response.body());
        }
        catch (IOException e) {
            if (e.getCause() instanceof KmsException ke) {
                throw ke;
            }
            throw new UncheckedIOException("Request to %s failed".formatted(request), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during REST API call: %s".formatted(request.uri()), e);
        }
    }

    private void checkForError(String key, URI uri, int statusCode, HttpResponse<byte[]> response) {
        // AWS API states that only the 200 response is currently used.
        // Our HTTP client is configured to follow redirects so 3xx responses are not expected here.
        if (!isHttpSuccess(statusCode)) {
            if (statusCode == 501) {
                throw new AwsNotImplementException("AWS does not implement %s".formatted(uri));
            }
            var body = new String(response.body(), UTF_8);
            throw new IllegalStateException(
                    "Unable to read error response with Status Code: %s from Fortanix for request: %s, body %s".formatted(response.statusCode(), uri,
                            body));
        }
    }

    private void sendRequestExpectingNoResponse(HttpRequest request) {
        try {
            var response = client.send(request, HttpResponse.BodyHandlers.discarding());
            if (!isHttpSuccess(response.statusCode())) {
                if (response.statusCode() == 501) {
                    throw new AwsNotImplementException("AWS do not implement %s".formatted(request.uri()));
                }
                throw new IllegalStateException("Unexpected response: %d to request %s".formatted(response.statusCode(), request.uri()));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Request to %s failed".formatted(request), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private boolean isHttpSuccess(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private static <T> T decodeJson(TypeReference<T> valueTypeRef, byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, valueTypeRef);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String encodeJson(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to encode the request body", e);
        }
    }

}
