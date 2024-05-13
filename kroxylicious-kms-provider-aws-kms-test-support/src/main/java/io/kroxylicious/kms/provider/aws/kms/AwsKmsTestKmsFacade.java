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
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Objects;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.aws.kms.model.CreateAliasRequest;
import io.kroxylicious.kms.provider.aws.kms.model.CreateKeyRequest;
import io.kroxylicious.kms.provider.aws.kms.model.CreateKeyResponse;
import io.kroxylicious.kms.provider.aws.kms.model.DeleteAliasRequest;
import io.kroxylicious.kms.provider.aws.kms.model.DescribeKeyRequest;
import io.kroxylicious.kms.provider.aws.kms.model.DescribeKeyResponse;
import io.kroxylicious.kms.provider.aws.kms.model.ErrorResponse;
import io.kroxylicious.kms.provider.aws.kms.model.ScheduleKeyDeletionRequest;
import io.kroxylicious.kms.provider.aws.kms.model.ScheduleKeyDeletionResponse;
import io.kroxylicious.kms.provider.aws.kms.model.UpdateAliasRequest;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.UnknownAliasException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AwsKmsTestKmsFacade extends AbstractAwsKmsTestKmsFacade {
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:3.4");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<CreateKeyResponse> CREATE_KEY_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<DescribeKeyResponse> DESCRIBE_KEY_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<ScheduleKeyDeletionResponse> SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<ErrorResponse> ERROR_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final String TRENT_SERVICE_DESCRIBE_KEY = "TrentService.DescribeKey";
    private static final String TRENT_SERVICE_CREATE_KEY = "TrentService.CreateKey";
    private static final String TRENT_SERVICE_CREATE_ALIAS = "TrentService.CreateAlias";
    private static final String TRENT_SERVICE_UPDATE_ALIAS = "TrentService.UpdateAlias";
    private static final String TRENT_SERVICE_DELETE_ALIAS = "TrentService.DeleteAlias";
    private static final String TRENT_SERVICE_SCHEDULE_KEY_DELETION = "TrentService.ScheduleKeyDeletion";
    private final HttpClient client = HttpClient.newHttpClient();
    private LocalStackContainer localStackContainer;

    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Override
    @SuppressWarnings("resource")
    public void startKms() {
        localStackContainer = new LocalStackContainer(LOCALSTACK_IMAGE) {
            @Override
            @SuppressWarnings("java:S1874")
            public LocalStackContainer withFileSystemBind(String hostPath, String containerPath) {
                // Workaround problem with LocalStackContainer on the Mac that manifests
                // under both Docker and Podman when the Container mounts the
                // docker.sock. It turns out that the mount is required by the Lambda Provider
                // so skipping it has no consequence for our use-case.
                // https://docs.localstack.cloud/getting-started/installation/#docker
                // TODO raise testcontainer issue
                return this;
            }
        }.withServices(LocalStackContainer.Service.KMS);

        localStackContainer.start();
    }

    @Override
    public void stopKms() {
        if (localStackContainer != null) {
            localStackContainer.close();
        }
    }

    private static <T> T decodeJson(TypeReference<T> valueTypeRef, byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, valueTypeRef);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    @NonNull
    protected URI getAwsUrl() {
        return localStackContainer.getEndpointOverride(LocalStackContainer.Service.KMS);
    }

    @Override
    protected String getRegion() {
        return localStackContainer.getRegion();
    }

    @Override
    protected String getSecretKey() {
        return localStackContainer.getSecretKey();
    }

    @Override
    protected String getAccessKey() {
        return localStackContainer.getAccessKey();
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new AwsKmsTestKekManager();
    }

    class AwsKmsTestKekManager implements TestKekManager {
        @Override
        public void generateKek(String alias) {
            Objects.requireNonNull(alias);

            if (exists(alias)) {
                throw new AlreadyExistsException(alias);
            }
            else {
                create(alias);
            }
        }

        @Override
        public void rotateKek(String alias) {
            Objects.requireNonNull(alias);

            if (!exists(alias)) {
                throw new UnknownAliasException(alias);
            }
            else {
                rotate(alias);
            }
        }

        @Override
        public void deleteKek(String alias) {
            if (!exists(alias)) {
                throw new UnknownAliasException(alias);
            }
            else {
                delete(alias);
            }
        }

        @Override
        public boolean exists(String alias) {
            try {
                read(alias);
                return true;
            }
            catch (UnknownAliasException uae) {
                return false;
            }
        }

        private void create(String alias) {
            final CreateKeyRequest createKey = new CreateKeyRequest("key for alias : " + alias);
            var createRequest = createRequest(createKey, TRENT_SERVICE_CREATE_KEY);
            var createKeyResponse = sendRequest(alias, createRequest, CREATE_KEY_RESPONSE_TYPE_REF);

            final CreateAliasRequest createAlias = new CreateAliasRequest(createKeyResponse.keyMetadata().keyId(), AwsKmsKms.ALIAS_PREFIX + alias);
            var aliasRequest = createRequest(createAlias, TRENT_SERVICE_CREATE_ALIAS);
            sendRequestExpectingNoResponse(aliasRequest);
        }

        private DescribeKeyResponse read(String alias) {
            final DescribeKeyRequest describeKey = new DescribeKeyRequest(AwsKmsKms.ALIAS_PREFIX + alias);
            var request = createRequest(describeKey, TRENT_SERVICE_DESCRIBE_KEY);
            return sendRequest(alias, request, DESCRIBE_KEY_RESPONSE_TYPE_REF);
        }

        private void rotate(String alias) {
            // RotateKeyOnDemand is not implemented in localstack.
            // https://docs.localstack.cloud/references/coverage/coverage_kms/#:~:text=Show%20Tests-,RotateKeyOnDemand,-ScheduleKeyDeletion
            // https://github.com/localstack/localstack/issues/10723

            // mimic a rotate by creating a new key and repoint the alias at it, leaving the original
            // key in place.
            final CreateKeyRequest request = new CreateKeyRequest("[rotated] key for alias : " + alias);
            var keyRequest = createRequest(request, TRENT_SERVICE_CREATE_KEY);
            var createKeyResponse = sendRequest(alias, keyRequest, CREATE_KEY_RESPONSE_TYPE_REF);

            final UpdateAliasRequest update = new UpdateAliasRequest(createKeyResponse.keyMetadata().keyId(), AwsKmsKms.ALIAS_PREFIX + alias);
            var aliasRequest = createRequest(update, TRENT_SERVICE_UPDATE_ALIAS);
            sendRequestExpectingNoResponse(aliasRequest);
        }

        private void delete(String alias) {
            var key = read(alias);
            var keyId = key.keyMetadata().keyId();
            final ScheduleKeyDeletionRequest request = new ScheduleKeyDeletionRequest(keyId, 7 /* Minimum allowed */);
            var scheduleDeleteRequest = createRequest(request, TRENT_SERVICE_SCHEDULE_KEY_DELETION);

            sendRequest(keyId, scheduleDeleteRequest, SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF);

            final DeleteAliasRequest deleteAlias = new DeleteAliasRequest(AwsKmsKms.ALIAS_PREFIX + alias);
            var deleteAliasRequest = createRequest(deleteAlias, TRENT_SERVICE_DELETE_ALIAS);
            sendRequestExpectingNoResponse(deleteAliasRequest);
        }

        private <R> R sendRequest(String key, HttpRequest request, TypeReference<R> valueTypeRef) {
            try {
                HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
                if (response.statusCode() != 200) {
                    var er = decodeJson(ERROR_RESPONSE_TYPE_REF, response.body());
                    if (er.type().equalsIgnoreCase("NotFoundException")) {
                        throw new UnknownAliasException(key);
                    }
                    throw new IllegalStateException("unexpected response %s (%s) for request: %s".formatted(response.statusCode(), er, request.uri()));
                }
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
                throw new IllegalStateException("Interrupted during REST API call : %s".formatted(request.uri()), e);
            }
        }

        private HttpRequest createRequest(Object request, String target) {
            var body = getBody(request).getBytes(UTF_8);

            return AwsV4SigningHttpRequestBuilder.newBuilder(getAccessKey(), getSecretKey(), getRegion(), "kms", Instant.now())
                    .uri(getAwsUrl())
                    .header(AwsKmsKms.CONTENT_TYPE_HEADER, AwsKmsKms.APPLICATION_X_AMZ_JSON_1_1)
                    .header(AwsKmsKms.X_AMZ_TARGET_HEADER, target)
                    .POST(BodyPublishers.ofByteArray(body))
                    .build();
        }

        private void sendRequestExpectingNoResponse(HttpRequest request) {
            try {
                var response = client.send(request, HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() != 200) {
                    throw new IllegalStateException("Unexpected response : %d to request %s".formatted(response.statusCode(), request.uri()));
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

        private String getBody(Object obj) {
            try {
                return OBJECT_MAPPER.writeValueAsString(obj);
            }
            catch (JsonProcessingException e) {
                throw new UncheckedIOException("Failed to create request body", e);
            }
        }
    }

}
