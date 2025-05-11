/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.provider.aws.kms.config.LongTermCredentialsProviderConfig;
import io.kroxylicious.kms.provider.aws.kms.credentials.LongTermCredentialsProvider;
import io.kroxylicious.kms.provider.aws.kms.model.CreateAliasRequest;
import io.kroxylicious.kms.provider.aws.kms.model.CreateKeyRequest;
import io.kroxylicious.kms.provider.aws.kms.model.CreateKeyResponse;
import io.kroxylicious.kms.provider.aws.kms.model.DeleteAliasRequest;
import io.kroxylicious.kms.provider.aws.kms.model.DescribeKeyRequest;
import io.kroxylicious.kms.provider.aws.kms.model.DescribeKeyResponse;
import io.kroxylicious.kms.provider.aws.kms.model.ErrorResponse;
import io.kroxylicious.kms.provider.aws.kms.model.RotateKeyRequest;
import io.kroxylicious.kms.provider.aws.kms.model.ScheduleKeyDeletionRequest;
import io.kroxylicious.kms.provider.aws.kms.model.ScheduleKeyDeletionResponse;
import io.kroxylicious.kms.provider.aws.kms.model.UpdateAliasRequest;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractAwsKmsTestKmsFacade implements TestKmsFacade<Config, String, AwsKmsEdek> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int MINIMUM_ALLOWED_EXPIRY_DAYS = 7;

    private static final String TRENT_SERVICE_DESCRIBE_KEY = "TrentService.DescribeKey";
    private static final String TRENT_SERVICE_CREATE_KEY = "TrentService.CreateKey";
    private static final String TRENT_SERVICE_CREATE_ALIAS = "TrentService.CreateAlias";
    private static final String TRENT_SERVICE_UPDATE_ALIAS = "TrentService.UpdateAlias";
    private static final String TRENT_SERVICE_ROTATE_KEY = "TrentService.RotateKeyOnDemand";
    private static final String TRENT_SERVICE_DELETE_ALIAS = "TrentService.DeleteAlias";
    private static final String TRENT_SERVICE_SCHEDULE_KEY_DELETION = "TrentService.ScheduleKeyDeletion";

    private static final TypeReference<CreateKeyResponse> CREATE_KEY_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<DescribeKeyResponse> DESCRIBE_KEY_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<ScheduleKeyDeletionResponse> SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<ErrorResponse> ERROR_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private final HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();

    protected AbstractAwsKmsTestKmsFacade() {
    }

    protected abstract void startKms();

    protected abstract void stopKms();

    @Override
    public final void start() {
        startKms();
    }

    @NonNull
    protected abstract URI getAwsUrl();

    @Override
    public Config getKmsServiceConfig() {
        var credentialsProviderConfig = new LongTermCredentialsProviderConfig(new InlinePassword(getAccessKey()), new InlinePassword(getSecretKey()));
        return new Config(getAwsUrl(), null, null, credentialsProviderConfig, null, getRegion(), null);
    }

    protected abstract String getRegion();

    protected abstract String getSecretKey();

    protected abstract String getAccessKey();

    @Override
    public final Class<AwsKmsService> getKmsServiceClass() {
        return AwsKmsService.class;
    }

    @Override
    public final void stop() {
        stopKms();
    }

    @Override
    public final TestKekManager getTestKekManager() {
        return new AwsKmsTestKekManager();
    }

    class AwsKmsTestKekManager implements TestKekManager {
        @Override
        public void generateKek(String alias) {
            final CreateKeyRequest createKey = new CreateKeyRequest("key for alias: " + alias);
            var createRequest = createRequest(createKey, TRENT_SERVICE_CREATE_KEY);
            var createKeyResponse = sendRequest(alias, createRequest, CREATE_KEY_RESPONSE_TYPE_REF);

            final CreateAliasRequest createAlias = new CreateAliasRequest(createKeyResponse.keyMetadata().keyId(), AwsKms.ALIAS_PREFIX + alias);
            var aliasRequest = createRequest(createAlias, TRENT_SERVICE_CREATE_ALIAS);
            sendRequestExpectingNoResponse(aliasRequest);
        }

        @Override
        public DescribeKeyResponse read(String alias) {
            final DescribeKeyRequest describeKey = new DescribeKeyRequest(AwsKms.ALIAS_PREFIX + alias);
            var request = createRequest(describeKey, TRENT_SERVICE_DESCRIBE_KEY);
            return sendRequest(alias, request, DESCRIBE_KEY_RESPONSE_TYPE_REF);
        }

        @Override
        public void deleteKek(String alias) {
            var key = read(alias);
            var keyId = key.keyMetadata().keyId();
            final ScheduleKeyDeletionRequest request = new ScheduleKeyDeletionRequest(keyId, MINIMUM_ALLOWED_EXPIRY_DAYS);
            var scheduleDeleteRequest = createRequest(request, TRENT_SERVICE_SCHEDULE_KEY_DELETION);

            sendRequest(keyId, scheduleDeleteRequest, SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF);

            final DeleteAliasRequest deleteAlias = new DeleteAliasRequest(AwsKms.ALIAS_PREFIX + alias);
            var deleteAliasRequest = createRequest(deleteAlias, TRENT_SERVICE_DELETE_ALIAS);
            sendRequestExpectingNoResponse(deleteAliasRequest);
        }

        @Override
        public void rotateKek(String alias) {
            var key = read(alias);

            final RotateKeyRequest rotateKey = new RotateKeyRequest(key.keyMetadata().keyId());
            var rotateKeyRequest = createRequest(rotateKey, TRENT_SERVICE_ROTATE_KEY);

            sendRequestExpectingNoResponse(rotateKeyRequest);
        }

        private void pseudoRotate(String alias) {

            // mimic rotate by creating a new key and repoint the alias at it, leaving the original key in place.
            final CreateKeyRequest request = new CreateKeyRequest("[rotated] key for alias: " + alias);
            var keyRequest = createRequest(request, TRENT_SERVICE_CREATE_KEY);
            var createKeyResponse = sendRequest(alias, keyRequest, CREATE_KEY_RESPONSE_TYPE_REF);

            final UpdateAliasRequest update = new UpdateAliasRequest(createKeyResponse.keyMetadata().keyId(), AwsKms.ALIAS_PREFIX + alias);
            var aliasRequest = createRequest(update, TRENT_SERVICE_UPDATE_ALIAS);
            sendRequestExpectingNoResponse(aliasRequest);
        }

        private HttpRequest createRequest(Object request, String target) {
            var body = encodeJson(request).getBytes(UTF_8);

            return AwsV4SigningHttpRequestBuilder
                    .newBuilder(LongTermCredentialsProvider.fixedCredentials(getAccessKey(), getSecretKey()), getRegion(), "kms", Instant.now())
                    .uri(getAwsUrl())
                    .header(AwsKms.CONTENT_TYPE_HEADER, AwsKms.APPLICATION_X_AMZ_JSON_1_1)
                    .header(AwsKms.X_AMZ_TARGET_HEADER, target)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
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
            ErrorResponse error;
            // AWS API states that only the 200 response is currently used.
            // Our HTTP client is configured to follow redirects so 3xx responses are not expected here.
            if (!isHttpSuccess(statusCode)) {
                if (statusCode == 501) {
                    throw new AwsNotImplementException("AWS does not implement %s".formatted(uri));
                }
                try {
                    error = decodeJson(ERROR_RESPONSE_TYPE_REF, response.body());
                    if (error.isNotFound()) {
                        throw new UnknownAliasException(key);
                    }
                    else {
                        throw new IllegalStateException("unexpected response %s (AWS error: %s) for request: %s".formatted(response.statusCode(), error, uri));
                    }
                }
                catch (UncheckedIOException e) {
                    throw new IllegalStateException("Unable to read error response with Status Code: %s from AWS for request: %s".formatted(response.statusCode(), uri));
                }
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
}
