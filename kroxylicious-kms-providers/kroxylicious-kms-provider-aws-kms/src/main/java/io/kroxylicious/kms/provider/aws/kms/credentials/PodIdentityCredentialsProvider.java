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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.kroxylicious.kms.provider.aws.kms.config.PodIdentityCredentialsProviderConfig;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * Provider that obtains temporary {@link Credentials} from the
 * <a href="https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html">EKS Pod Identity</a>
 * Agent.  The agent is reachable on a link-local address inside the pod's network namespace and
 * authenticates the request with a projected service-account token whose path is supplied via the
 * {@code AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE} environment variable.
 * <p>
 * Pod Identity is the AWS-recommended successor to IRSA on EKS.  Compared with IRSA it requires
 * neither an OIDC trust-policy edit nor a direct STS round-trip from the workload — the agent
 * handles the STS exchange on the pod's behalf and exposes a simple JSON endpoint that returns
 * the same payload shape that EC2 IMDS has used for years.
 * </p>
 *
 * @see AbstractRefreshingCredentialsProvider for the shared async-refresh state machine.
 */
public class PodIdentityCredentialsProvider extends AbstractRefreshingCredentialsProvider<PodIdentityCredentialsProvider.PodIdentityCredentials> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PodIdentityCredentialsProvider.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final TypeReference<PodIdentityCredentials> CREDENTIALS_TYPE_REF = new TypeReference<>() {
    };

    private static final Duration HTTP_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration HTTP_CONNECT_TIMEOUT = Duration.ofSeconds(10);

    static final String ENV_FULL_URI = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
    static final String ENV_AUTH_TOKEN_FILE = "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE";

    private final URI credentialsFullUri;
    private final Path authorizationTokenFile;
    private final HttpClient client;

    /**
     * Creates the EKS Pod Identity credentials provider.
     *
     * @param config provider config.
     */
    public PodIdentityCredentialsProvider(PodIdentityCredentialsProviderConfig config) {
        this(config, System::getenv, Clock.systemUTC());
    }

    @VisibleForTesting
    PodIdentityCredentialsProvider(PodIdentityCredentialsProviderConfig config,
                                   Function<String, String> env,
                                   Clock systemClock) {
        super(PodIdentityCredentialsProvider.class.getName() + "thread",
                LOGGER,
                Objects.requireNonNull(systemClock),
                Optional.ofNullable(Objects.requireNonNull(config).credentialLifetimeFactor()).orElse(DEFAULT_CREDENTIALS_LIFETIME_FACTOR));
        Objects.requireNonNull(env);

        var uriString = config.credentialsFullUri() != null ? config.credentialsFullUri().toString() : env.apply(ENV_FULL_URI);
        if (uriString == null || uriString.isBlank()) {
            throw new KmsException(
                    "podIdentityCredentials missing 'credentialsFullUri': set it in YAML or the AWS_CONTAINER_CREDENTIALS_FULL_URI environment variable");
        }
        this.credentialsFullUri = URI.create(uriString);
        var scheme = this.credentialsFullUri.getScheme();
        if (scheme == null || !(scheme.equals("http") || scheme.equals("https"))) {
            throw new KmsException("podIdentityCredentials 'credentialsFullUri' must use http or https scheme, got: " + uriString);
        }

        var tokenFileString = config.authorizationTokenFile() != null ? config.authorizationTokenFile().toString() : env.apply(ENV_AUTH_TOKEN_FILE);
        if (tokenFileString == null || tokenFileString.isBlank()) {
            throw new KmsException(
                    "podIdentityCredentials missing 'authorizationTokenFile': set it in YAML or the AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE environment variable");
        }
        this.authorizationTokenFile = Path.of(tokenFileString);

        this.client = createClient();
    }

    private HttpClient createClient() {
        return HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(HTTP_CONNECT_TIMEOUT)
                .build();
    }

    @Override
    protected CompletionStage<PodIdentityCredentials> fetchCredentials() {
        var token = readAuthorizationToken();
        var request = HttpRequest.newBuilder()
                .uri(credentialsFullUri)
                .header("Authorization", token)
                .header("Accept", "application/json")
                .timeout(HTTP_REQUEST_TIMEOUT)
                .GET()
                .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(this::parseCredentialsResponse);
    }

    @Override
    protected Instant expirationOf(PodIdentityCredentials credentials) {
        return credentials.expiration();
    }

    @Override
    protected void onRefreshFailure(Throwable t) {
        LOGGER.atWarn()
                .setCause(LOGGER.isDebugEnabled() ? t : null)
                .addKeyValue("credentialsFullUri", credentialsFullUri)
                .addKeyValue("error", t.getMessage())
                .log(LOGGER.isDebugEnabled()
                        ? "refresh of EKS Pod Identity credentials failed; check that the pod-identity agent is reachable and the projected token is valid"
                        : "refresh of EKS Pod Identity credentials failed; increase log level to DEBUG for stacktrace");
    }

    @Override
    protected void onRefreshSuccess(PodIdentityCredentials credentials) {
        LOGGER.atDebug()
                .addKeyValue("credentialsFullUri", credentialsFullUri)
                .addKeyValue("expiration", credentials.expiration())
                .log("Obtained AWS credentials from EKS Pod Identity agent");
    }

    private String readAuthorizationToken() {
        try {
            var token = Files.readString(authorizationTokenFile, StandardCharsets.UTF_8).trim();
            if (token.isEmpty()) {
                throw new KmsException("Pod Identity authorization token file " + authorizationTokenFile + " is empty");
            }
            return token;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read Pod Identity authorization token file " + authorizationTokenFile, e);
        }
    }

    private PodIdentityCredentials parseCredentialsResponse(HttpResponse<byte[]> response) {
        var status = response.statusCode();
        var body = response.body();
        if (status < 200 || status >= 300) {
            throw new KmsException("Pod Identity agent request failed with HTTP %d: %s".formatted(status, bodyAsString(body)));
        }
        try {
            return OBJECT_MAPPER.readValue(body, CREDENTIALS_TYPE_REF);
        }
        catch (IOException e) {
            throw new KmsException("Failed to parse Pod Identity agent response: " + bodyAsString(body), e);
        }
    }

    private static String bodyAsString(byte[] body) {
        return body == null ? "" : new String(body, StandardCharsets.UTF_8);
    }

    @VisibleForTesting
    URI credentialsFullUri() {
        return credentialsFullUri;
    }

    @VisibleForTesting
    Path authorizationTokenFile() {
        return authorizationTokenFile;
    }

    /**
     * The temporary credentials returned by the EKS Pod Identity agent.  The payload shape is the
     * same one EC2 IMDS has used for years (sans the EC2-specific {@code Code} field).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PodIdentityCredentials(@JsonProperty("AccessKeyId") String accessKeyId,
                                         @JsonProperty("SecretAccessKey") String secretAccessKey,
                                         @JsonProperty("Token") String token,
                                         @JsonProperty("Expiration") Instant expiration)
            implements Credentials {
        public PodIdentityCredentials {
            Objects.requireNonNull(accessKeyId);
            Objects.requireNonNull(secretAccessKey);
            Objects.requireNonNull(token);
            Objects.requireNonNull(expiration);
        }

        @Override
        public Optional<String> securityToken() {
            return Optional.of(token);
        }

        @Override
        public String toString() {
            return "PodIdentityCredentials{" +
                    "accessKeyId='" + accessKeyId + '\'' +
                    ", secretAccessKey='***************'" +
                    ", token='***************'" +
                    ", expiration=" + expiration +
                    '}';
        }
    }
}
