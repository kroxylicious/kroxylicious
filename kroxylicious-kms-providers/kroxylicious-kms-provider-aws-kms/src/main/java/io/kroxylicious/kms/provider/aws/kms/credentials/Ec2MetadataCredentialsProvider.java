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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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

/**
 * Provider that obtains {@link Credentials} from the metadata server of the EC2 instance.
 *
 * @see AbstractRefreshingCredentialsProvider for the shared async-refresh state machine.
 */
public class Ec2MetadataCredentialsProvider extends AbstractRefreshingCredentialsProvider<Ec2MetadataCredentialsProvider.SecurityCredentials> {
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

    private final Ec2MetadataCredentialsProviderConfig config;
    private final HttpClient client;
    private final URI uri;

    /**
     * Creates the EC2 metadata credentials provider.
     *
     * @param config config.
     */
    public Ec2MetadataCredentialsProvider(Ec2MetadataCredentialsProviderConfig config) {
        this(config, Clock.systemUTC());
    }

    @VisibleForTesting
    Ec2MetadataCredentialsProvider(Ec2MetadataCredentialsProviderConfig config,
                                   Clock systemClock) {
        super(Ec2MetadataCredentialsProvider.class.getName() + "thread",
                LOGGER,
                Objects.requireNonNull(systemClock),
                Optional.ofNullable(Objects.requireNonNull(config).credentialLifetimeFactor()).orElse(DEFAULT_CREDENTIALS_LIFETIME_FACTOR));
        this.config = config;
        this.uri = Optional.ofNullable(config.metadataEndpoint()).orElse(DEFAULT_IP4_METADATA_ENDPOINT);
        this.client = createClient();
    }

    private HttpClient createClient() {
        var builder = HttpClient.newBuilder();
        return builder
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(HTTP_CONNECT_TIMEOUT)
                .build();
    }

    @Override
    protected CompletionStage<SecurityCredentials> fetchCredentials() {
        return getToken()
                .thenCompose(tokenResponse -> client.sendAsync(createSecurityCredentialsRequest(tokenResponse.body()), HttpResponse.BodyHandlers.ofByteArray()))
                .thenApply(Ec2MetadataCredentialsProvider::checkResponseStatus)
                .thenApply(HttpResponse::body)
                .thenApply(this::toSecurityCredentials)
                .thenApply(this::checkSuccessfulState);
    }

    @Override
    protected Instant expirationOf(SecurityCredentials credentials) {
        return credentials.expiration();
    }

    @Override
    protected void onRefreshFailure(Throwable t) {
        LOGGER.atWarn()
                .setCause(LOGGER.isDebugEnabled() ? t : null)
                .addKeyValue("iamRole", config.iamRole())
                .addKeyValue("error", t.getMessage())
                .log(LOGGER.isDebugEnabled()
                        ? "refresh of EC2 credentials failed, is IAM role assigned to this EC2 instance?"
                        : "refresh of EC2 credentials failed, is IAM role assigned to this EC2 instance? Increase log level to DEBUG for stacktrace");
    }

    @Override
    protected void onRefreshSuccess(SecurityCredentials credentials) {
        LOGGER.atDebug()
                .addKeyValue("iamRole", config.iamRole())
                .addKeyValue("expiration", credentials.expiration())
                .log("Obtained AWS credentials from EC2 metadata");
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
        LOGGER.atDebug()
                .addKeyValue("credential", sc::toString)
                .log("AWS returned security credential");
        if (!"success".equals(sc.code().toLowerCase(Locale.ROOT))) {
            throw new KmsException(
                    "Unexpected code value in SecurityCredentials object returned from AWS.  Expecting code='Success', got code='%s'".formatted(sc.code()));
        }
        return sc;
    }

    private static <O> HttpResponse<O> checkResponseStatus(HttpResponse<O> response) {
        var statusCode = response.statusCode();
        var httpSuccess = statusCode >= 200 && statusCode < 300;
        if (!httpSuccess) {
            var uri = response.request().uri();
            var body = bodyToString(response.body());
            throw new KmsException("Operation failed, request uri: %s, HTTP status code %d, response: %s".formatted(uri, statusCode, body));
        }
        return response;
    }

    private static <B> String bodyToString(B body) {
        return body instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(body);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SecurityCredentials(@JsonProperty(value = "Code") String code,
                                      @JsonProperty(value = "AccessKeyId") String accessKeyId,
                                      @JsonProperty(value = "SecretAccessKey") String secretAccessKey,
                                      @JsonProperty(value = "Token") String token,
                                      @JsonProperty(value = "Expiration") Instant expiration)
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

        @Override
        public Optional<String> securityToken() {
            return Optional.of(token);
        }
    }

}
