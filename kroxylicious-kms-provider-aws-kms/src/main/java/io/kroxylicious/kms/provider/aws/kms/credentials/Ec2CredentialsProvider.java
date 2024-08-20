/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.kroxylicious.kms.provider.aws.kms.config.Ec2CredentialsProviderConfig;
import io.kroxylicious.kms.service.KmsException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Get {@link Credentials} from the metadata server of the EC2 instance.
 */
public class Ec2CredentialsProvider implements CredentialsProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ec2CredentialsProvider.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final TypeReference<SecurityCredentials> SECURITY_CREDENTIALS_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    @SuppressWarnings("java:S1313")
    private static final URI DEFAULT_IP4_METADATA_ENDPOINT = URI.create("http://169.254.169.254/");
    private static final String X_AWS_EC_2_METADATA_TOKEN_TTL_SECONDS = "X-aws-ec2-metadata-token-ttl-seconds";
    private static final String X_AWS_EC_2_METADATA_TOKEN = "X-aws-ec2-metadata-token";

    /**
     * EC2 Token Retrieval Endpoint.
     * Note that  use of "latest" is
     * <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html#imds-considerations">required</a>.
     */
    static final String TOKEN_RETRIEVAL_ENDPOINT = "/latest/api/token";

    /**
     * EC2 Meta-data security credentials endpoint.
     */
    static final String META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT = "/2024-04-11/meta-data/iam/security-credentials/";
    private final Ec2CredentialsProviderConfig config;
    private final HttpClient client;

    private final AtomicReference<CompletionStage<SecurityCredentials>> current = new AtomicReference<>();

    public Ec2CredentialsProvider(@NonNull Ec2CredentialsProviderConfig config) {
        LOGGER.warn("KWDEBUG creating Ec2CredentialsProvider {}", config);
        Objects.requireNonNull(config);
        this.config = config;
        this.client = createClient();
    }

    private HttpClient createClient() {
        var builder = HttpClient.newBuilder();
        return builder
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    @NonNull
    @Override
    public CompletionStage<SecurityCredentials> getCredentials() {

        var newValue = new CompletableFuture<SecurityCredentials>();
        if (current.compareAndSet(null, newValue)) {
            refreshCredential(newValue);
        }

        return current.get();
    }

    private void refreshCredential(CompletableFuture<SecurityCredentials> newValue) {
        // error handling
        LOGGER.warn("KWDEBUG refreshCredential");
        client.sendAsync(createTokenRequest(), HttpResponse.BodyHandlers.ofString())
                .thenApply(Ec2CredentialsProvider::checkResponseStatus)
                .thenApply(tokenResponse -> client.sendAsync(createSecurityCredentialsRequest(tokenResponse.body()), HttpResponse.BodyHandlers.ofByteArray())
                        .thenApply(Ec2CredentialsProvider::checkResponseStatus)
                        .thenApply(HttpResponse::body)
                        .thenApply(bytes -> decodeJson(SECURITY_CREDENTIALS_RESPONSE_TYPE_REF, bytes))
                        .thenApply(newValue::complete)
                        .exceptionally(newValue::completeExceptionally))
                .handle((x, y) -> {
                    if (y != null) {
                        newValue.completeExceptionally(y);
                    }
                    return null;
                });

        // handle refresh
    }

    private HttpRequest createTokenRequest() {
        return HttpRequest.newBuilder()
                .uri(getMetadataEndpoint().resolve(TOKEN_RETRIEVAL_ENDPOINT))
                .header(X_AWS_EC_2_METADATA_TOKEN_TTL_SECONDS, "21600") // What does this timeout correspond to?
                .PUT(HttpRequest.BodyPublishers.noBody())
                .build();
    }

    private URI getMetadataEndpoint() {
        return Optional.ofNullable(config.metadataEndpoint()).orElse(DEFAULT_IP4_METADATA_ENDPOINT);
    }

    private HttpRequest createSecurityCredentialsRequest(String token) {
        LOGGER.warn("KWDEBUG createSecurityCredentialsRequest for {}", token);

        return HttpRequest.newBuilder()
                .uri(getMetadataEndpoint().resolve(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + config.iamRole()))
                .header(X_AWS_EC_2_METADATA_TOKEN, token)
                .GET()
                .build();
    }

    private static <T> T decodeJson(TypeReference<T> valueTypeRef, byte[] bytes) {
        LOGGER.warn("KWDEBUG decodeJson got {}", bytes);

        try {
            return OBJECT_MAPPER.readValue(bytes, valueTypeRef);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @NonNull
    private static <O> HttpResponse<O> checkResponseStatus(@NonNull HttpResponse<O> response) {
        var statusCode = response.statusCode();
        LOGGER.warn("KWDEBUG checkResponseStatus {}", statusCode);
        var httpSuccess = statusCode >= 200 && statusCode < 300;
        if (!httpSuccess) {
            var body = response.body() instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(response.body());
            throw new KmsException("Operation failed, HTTP status code %d, response: %s".formatted(statusCode, body));
        }
        return response;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SecurityCredentials(@JsonProperty(value = "Code") @NonNull String code,
                                      @JsonProperty(value = "AccessKeyId") @NonNull String accessKey,
                                      @JsonProperty(value = "SecretAccessKey") @NonNull String secretKey,

                                      @JsonProperty(value = "Token") @NonNull String token,
                                      @JsonProperty(value = "Expiration") @NonNull Instant expiration)
            implements Credentials {

        public SecurityCredentials {
            Objects.requireNonNull(code);
            Objects.requireNonNull(accessKey);
            Objects.requireNonNull(secretKey);
            Objects.requireNonNull(token);
            Objects.requireNonNull(expiration);
            LOGGER.warn("KWDEBUG Got code " + code);
            LOGGER.warn("KWDEBUG Got accessKey " + accessKey);
            LOGGER.warn("KWDEBUG Got secretKey " + secretKey);
            LOGGER.warn("KWDEBUG Got token " + token);
        }

        @Override
        public Optional<String> securityToken() {
            return Optional.of(token);
        }
    }

}
