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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.kroxylicious.kms.provider.aws.kms.config.Ec2MetadataCredentialsProviderConfig;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Provider that obtains {@link Credentials} from the metadata server of the EC2 instance.
 * <p>
 * The provider will keep returning the same credential until the credential reaches
 * a configured factor of the credential's lifespan.  At which point, a preemptive
 * background refresh of the credential will be performed.  Until the refresh is complete
 * the caller will continue to receive the existing credential.  Once the refresh is complete
 * subsequent calls will see the updated credential.
 * </p>
 * <p>
 * If an error occurs whilst retrieving the credential, the next call will cause the
 * provider to try again.  A progress backoff is applied to retry attempts.
 * </p>
 */
public class Ec2MetadataCredentialsProvider implements CredentialsProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(Ec2MetadataCredentialsProvider.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final TypeReference<SecurityCredentials> SECURITY_CREDENTIALS_RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    @SuppressWarnings("java:S1313")
    private static final URI DEFAULT_IP4_METADATA_ENDPOINT = URI.create("http://169.254.169.254/");
    private static final Duration HTTP_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration HTTP_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final String AWS_METADATA_TOKEN_TTL_SECONDS_HEADER = "X-aws-ec2-metadata-token-ttl-seconds";
    private static final String AWS_METADATA_TOKEN_HEADER = "X-aws-ec2-metadata-token";
    private static final String AWS_TOKEN_EXPIRATION_SECONDS = "60";
    private static final double DEFAULT_CREDENTIALS_LIFETIME_FACTOR = 0.80;

    /**
     * EC2 Token Retrieval Endpoint.
     * Note that  use of "latest" is
     * <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html#imds-considerations">required</a>.
     */
    private static final String TOKEN_RETRIEVAL_ENDPOINT = "/latest/api/token";

    /**
     * EC2 Meta-data security credentials endpoint.  AWS recommend that latest is used.  That's the approach taken by their
     * own SDK.
     */
    private static final String META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT = "/latest/meta-data/iam/security-credentials/";

    private final Clock systemClock;
    private final AtomicReference<CompletableFuture<SecurityCredentials>> current = new AtomicReference<>();

    private final AtomicLong tokenRefreshErrorCount = new AtomicLong();
    private final Ec2MetadataCredentialsProviderConfig config;
    private final HttpClient client;

    private final ScheduledExecutorService executorService;
    @SuppressWarnings({ "java:S2245", "java:S2119" }) // Random used for backoff jitter, it does not need to be securely random.
    private final ExponentialBackoff backoff = new ExponentialBackoff(500, 2, 60000, new Random().nextDouble());
    private final Double lifetimeFactor;
    private final URI uri;

    /**
     * Creates the EC2 metadata credentials provider.
     *
     * @param config config.
     */
    public Ec2MetadataCredentialsProvider(@NonNull Ec2MetadataCredentialsProviderConfig config) {
        this(config, Clock.systemUTC());
    }

    @VisibleForTesting
    Ec2MetadataCredentialsProvider(@NonNull Ec2MetadataCredentialsProviderConfig config, @NonNull Clock systemClock) {
        Objects.requireNonNull(config);
        Objects.requireNonNull(systemClock);
        this.config = config;
        this.systemClock = systemClock;
        this.lifetimeFactor = Optional.ofNullable(config.credentialLifetimeFactor()).orElse(DEFAULT_CREDENTIALS_LIFETIME_FACTOR);
        this.uri = Optional.ofNullable(config.metadataEndpoint()).orElse(DEFAULT_IP4_METADATA_ENDPOINT);
        this.executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            var thread = new Thread(r, Ec2MetadataCredentialsProvider.class.getName() + "thread");
            thread.setDaemon(true);
            return thread;
        });
        this.client = createClient();
    }

    private HttpClient createClient() {
        var builder = HttpClient.newBuilder();
        return builder
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(HTTP_CONNECT_TIMEOUT)
                .build();
    }

    @NonNull
    @Override
    public CompletionStage<SecurityCredentials> getCredentials() {
        var newCredFuture = new CompletableFuture<SecurityCredentials>();
        var witness = current.compareAndExchange(null, newCredFuture);
        if (witness == null) {
            // there's no current credential, let's create one
            executorService.execute(() -> refreshCredential(newCredFuture));
            return newCredFuture.minimalCompletionStage();
        }
        else if (isExpired(witness) || witness.isCompletedExceptionally()) {
            // current credential is expired, or it has been completed exceptionally.
            // throw it away and generate a new one.
            // we don't normally expect to follow the expired path as the preemptive refresh ought to have
            // caused its refresh before its expiration.
            current.compareAndSet(witness, null);
            return getCredentials();
        }

        return witness.minimalCompletionStage();
    }

    private void scheduleCredentialRefresh(long delay) {
        LOGGER.debug("Scheduling refresh of AWS credentials in {}ms", delay);

        var refreshedCredFuture = new CompletableFuture<SecurityCredentials>();
        executorService.schedule(() -> {
            refreshCredential(refreshedCredFuture);
            refreshedCredFuture.thenApply(sc -> {
                var previous = current.getAndSet(refreshedCredFuture);
                // the previous future have been already complete, but for safety, complete it anyway.
                Optional.ofNullable(previous).ifPresent(f -> f.complete(sc));
                return null;
            });
        }, delay, TimeUnit.MILLISECONDS);
    }

    private boolean isExpired(CompletableFuture<SecurityCredentials> witness) {
        if (witness.isDone() && !witness.isCompletedExceptionally()) {
            try {
                return Optional.ofNullable(witness.getNow(null))
                        .map(SecurityCredentials::expiration)
                        .map(exp -> systemClock.instant().isAfter(exp))
                        .orElse(false);
            }
            catch (CancellationException | CompletionException e) {
                return false;
            }
        }
        return false;
    }

    private void refreshCredential(CompletableFuture<SecurityCredentials> future) {
        getToken()
                .thenCompose(tokenResponse -> client.sendAsync(createSecurityCredentialsRequest(tokenResponse.body()), HttpResponse.BodyHandlers.ofByteArray()))
                .thenApply(Ec2MetadataCredentialsProvider::checkResponseStatus)
                .thenApply(HttpResponse::body)
                .thenApply(this::toSecurityCredentials)
                .thenApply(this::checkSuccessfulState)
                .whenComplete((credentials, t) -> propagateResultToFuture(credentials, t, future));
    }

    private void propagateResultToFuture(SecurityCredentials credentials, Throwable t, CompletableFuture<SecurityCredentials> target) {
        final long refreshDelay;
        if (t != null) {
            LOGGER.warn("Refresh of EC2 credentials failed. Is IAM role {} assigned to this EC2 instance?", config.iamRole(), t);
            tokenRefreshErrorCount.incrementAndGet();
            target.completeExceptionally(t);

            refreshDelay = backoff.backoff(tokenRefreshErrorCount.get());
        }
        else {
            var expiration = credentials.expiration();
            LOGGER.debug("Obtained AWS credentials from EC2 metadata using IAM role {}, expiry {}", config.iamRole(), expiration);
            tokenRefreshErrorCount.set(0);
            target.complete(credentials);

            refreshDelay = (long) Math.max(0, this.lifetimeFactor * (expiration.toEpochMilli() - systemClock.instant().toEpochMilli()));
        }
        scheduleCredentialRefresh(refreshDelay);
    }

    private CompletableFuture<HttpResponse<String>> getToken() {
        return client.sendAsync(createTokenRequest(), HttpResponse.BodyHandlers.ofString())
                .thenApply(Ec2MetadataCredentialsProvider::checkResponseStatus);
    }

    private HttpRequest createTokenRequest() {
        return HttpRequest.newBuilder()
                .uri(getMetadataEndpoint().resolve(TOKEN_RETRIEVAL_ENDPOINT))
                .header(AWS_METADATA_TOKEN_TTL_SECONDS_HEADER, AWS_TOKEN_EXPIRATION_SECONDS)
                .PUT(HttpRequest.BodyPublishers.noBody())
                .timeout(HTTP_REQUEST_TIMEOUT)
                .build();
    }

    private HttpRequest createSecurityCredentialsRequest(String token) {
        return HttpRequest.newBuilder()
                .uri(getMetadataEndpoint().resolve(META_DATA_IAM_SECURITY_CREDENTIALS_ENDPOINT + config.iamRole()))
                .header(AWS_METADATA_TOKEN_HEADER, token)
                .timeout(HTTP_REQUEST_TIMEOUT)
                .GET()
                .build();
    }

    private URI getMetadataEndpoint() {
        return uri;
    }

    private SecurityCredentials toSecurityCredentials(byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, SECURITY_CREDENTIALS_RESPONSE_TYPE_REF);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to unmarshal '%s' as a SecurityCredential." + bodyToString(bytes), e);
        }
    }

    private SecurityCredentials checkSuccessfulState(SecurityCredentials sc) {
        LOGGER.debug("AWS returned security credential : {} ", sc);
        if (!"success".equals(sc.code().toLowerCase(Locale.ROOT))) {
            throw new KmsException(
                    "Unexpected code value in SecurityCredentials object returned from AWS.  Expecting code='Success', got code='%s'".formatted(sc.code()));
        }
        return sc;
    }

    @NonNull
    private static <O> HttpResponse<O> checkResponseStatus(@NonNull HttpResponse<O> response) {
        var statusCode = response.statusCode();
        var httpSuccess = statusCode >= 200 && statusCode < 300;
        if (!httpSuccess) {
            var uri = response.request().uri();
            var body = bodyToString(response.body());
            throw new KmsException("Operation failed, request uri: %s, HTTP status code %d, response: %s".formatted(uri, statusCode, body));
        }
        return response;
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }

    private static <B> String bodyToString(B body) {
        return body instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(body);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SecurityCredentials(@JsonProperty(value = "Code") @NonNull String code,
                                      @JsonProperty(value = "AccessKeyId") @NonNull String accessKeyId,
                                      @JsonProperty(value = "SecretAccessKey") @NonNull String secretAccessKey,
                                      @JsonProperty(value = "Token") @NonNull String token,
                                      @JsonProperty(value = "Expiration") @NonNull Instant expiration)
            implements Credentials {
        public SecurityCredentials {
            Objects.requireNonNull(code);
            Objects.requireNonNull(accessKeyId);
            Objects.requireNonNull(secretAccessKey);
            Objects.requireNonNull(token);
            Objects.requireNonNull(expiration);
        }

        @Override
        public String toString() {
            return "SecurityCredentials{" +
                    "code='" + code + '\'' +
                    ", accessKeyId='" + accessKeyId + '\'' +
                    ", secretAccessKey='***************" +
                    ", token='***************'" +
                    ", expiration=" + expiration +
                    '}';
        }

        @NonNull
        @Override
        public Optional<String> securityToken() {
            return Optional.of(token);
        }
    }

}
