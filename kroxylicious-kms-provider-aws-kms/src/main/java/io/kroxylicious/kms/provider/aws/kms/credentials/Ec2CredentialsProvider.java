/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.kroxylicious.kms.provider.aws.kms.config.Ec2CredentialsProviderConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Get {@link Credentials} from the metadata server of the EC2 instance.
 */
public class Ec2CredentialsProvider implements CredentialsProvider {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final TypeReference<SecurityCredentials> SECURITY_CREDENTIALS_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private final Ec2CredentialsProviderConfig config;
    private final HttpClient client;

    private AtomicReference<CompletionStage<SecurityCredentials>> current = new AtomicReference<>();

    public Ec2CredentialsProvider(@NonNull Ec2CredentialsProviderConfig config) {
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
        client.sendAsync(createTokenRequest(), HttpResponse.BodyHandlers.ofString())
                .thenApply(tokenResponse -> client.sendAsync(createSecurityCredentialsRequest(tokenResponse.body()), HttpResponse.BodyHandlers.ofByteArray())
                        .thenApply(HttpResponse::body)
                        .thenApply(bytes -> decodeJson(SECURITY_CREDENTIALS_RESPONSE_TYPE_REF, bytes))
                        .thenApply(newValue::complete)
                        .exceptionally(newValue::completeExceptionally));

        // handle refresh
    }

    private HttpRequest createTokenRequest() {
        return HttpRequest.newBuilder()
                .uri(config.metadataEndpoint().resolve("/latest/api/token"))
                .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
                .PUT(HttpRequest.BodyPublishers.noBody())
                .build();
    }

    private HttpRequest createSecurityCredentialsRequest(String token) {
        return HttpRequest.newBuilder()
                .uri(config.metadataEndpoint().resolve("/latest/meta-data/iam/security-credentials/" + config.iamRole()))
                .header("X-aws-ec2-metadata-token", token)
                .GET()
                .build();
    }

    private static <T> T decodeJson(TypeReference<T> valueTypeRef, byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, valueTypeRef);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SecurityCredentials(@JsonProperty(value = "Code") @NonNull String code,
                                      @JsonProperty(value = "AccessKeyId") @NonNull String accessKey,
                                      @JsonProperty(value = "SecretAccessKey") @NonNull String secretKey,
                                      @JsonProperty(value = "Expiration") @NonNull Instant expiration)
            implements Credentials {

        public SecurityCredentials {
            Objects.requireNonNull(code);
            Objects.requireNonNull(accessKey);
            Objects.requireNonNull(secretKey);
            Objects.requireNonNull(expiration);
        }

    }

}
