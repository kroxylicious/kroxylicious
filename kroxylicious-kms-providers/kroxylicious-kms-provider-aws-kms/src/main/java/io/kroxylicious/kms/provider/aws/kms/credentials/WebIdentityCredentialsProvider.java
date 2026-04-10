/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
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
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.kroxylicious.kms.provider.aws.kms.config.WebIdentityCredentialsProviderConfig;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Provider that obtains temporary {@link Credentials} from AWS STS by exchanging a Kubernetes
 * service-account OIDC token via the
 * <a href="https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html">AssumeRoleWithWebIdentity</a>
 * action.  This is the IRSA flow used on Amazon EKS.
 * <p>
 * The web-identity exchange is unsigned (the JWT is the credential), so this provider does
 * <em>not</em> go through the SigV4 signing path.  The session token returned by STS is then
 * used by the {@code AwsV4SigningHttpRequestBuilder} when calling AWS KMS.
 * </p>
 *
 * @see AbstractRefreshingCredentialsProvider for the shared async-refresh state machine.
 */
public class WebIdentityCredentialsProvider extends AbstractRefreshingCredentialsProvider<WebIdentityCredentialsProvider.AssumedRoleCredentials> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebIdentityCredentialsProvider.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final TypeReference<AssumeRoleWithWebIdentityResponseEnvelope> RESPONSE_TYPE_REF = new TypeReference<>() {
    };
    private static final TypeReference<StsErrorEnvelope> ERROR_TYPE_REF = new TypeReference<>() {
    };

    private static final Duration HTTP_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration HTTP_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final String STS_API_VERSION = "2011-06-15";
    private static final String STS_ACTION = "AssumeRoleWithWebIdentity";
    private static final int MAX_SESSION_NAME_LENGTH = 64;

    static final String ENV_ROLE_ARN = "AWS_ROLE_ARN";
    static final String ENV_WEB_IDENTITY_TOKEN_FILE = "AWS_WEB_IDENTITY_TOKEN_FILE";
    static final String ENV_ROLE_SESSION_NAME = "AWS_ROLE_SESSION_NAME";
    static final String ENV_AWS_REGION = "AWS_REGION";

    private final HttpClient client;
    private final URI stsEndpointUrl;
    private final String roleArn;
    private final Path webIdentityTokenFile;
    private final String roleSessionName;
    private final @Nullable Integer durationSeconds;

    /**
     * Creates the IRSA web-identity credentials provider.
     *
     * @param config provider config.
     * @param defaultRegion AWS region inherited from the surrounding {@code Config} (used as a
     *                      fallback for the STS region/endpoint).
     */
    public WebIdentityCredentialsProvider(WebIdentityCredentialsProviderConfig config, String defaultRegion) {
        this(config, defaultRegion, System::getenv, Clock.systemUTC());
    }

    @VisibleForTesting
    WebIdentityCredentialsProvider(WebIdentityCredentialsProviderConfig config,
                                   String defaultRegion,
                                   Function<String, String> env,
                                   Clock systemClock) {
        super(WebIdentityCredentialsProvider.class.getName() + "thread",
                LOGGER,
                Objects.requireNonNull(systemClock),
                Optional.ofNullable(Objects.requireNonNull(config).credentialLifetimeFactor()).orElse(DEFAULT_CREDENTIALS_LIFETIME_FACTOR));
        Objects.requireNonNull(defaultRegion);
        Objects.requireNonNull(env);

        this.roleArn = requireValue(config.roleArn(), env.apply(ENV_ROLE_ARN), "roleArn",
                "set 'roleArn' in webIdentityCredentials or the AWS_ROLE_ARN environment variable");
        var tokenFileString = config.webIdentityTokenFile() != null ? config.webIdentityTokenFile().toString() : env.apply(ENV_WEB_IDENTITY_TOKEN_FILE);
        if (tokenFileString == null || tokenFileString.isEmpty()) {
            throw new KmsException(
                    "webIdentityCredentials missing 'webIdentityTokenFile': set it in YAML or the AWS_WEB_IDENTITY_TOKEN_FILE environment variable");
        }
        this.webIdentityTokenFile = Path.of(tokenFileString);

        var resolvedRegion = firstNonBlank(config.stsRegion(), env.apply(ENV_AWS_REGION), defaultRegion);
        this.stsEndpointUrl = config.stsEndpointUrl() != null
                ? config.stsEndpointUrl()
                : URI.create("https://sts." + resolvedRegion + ".amazonaws.com");

        this.roleSessionName = sanitiseSessionName(
                firstNonBlank(config.roleSessionName(), env.apply(ENV_ROLE_SESSION_NAME), generatedSessionName()));

        this.durationSeconds = config.durationSeconds();

        this.client = createClient();
    }

    private static String requireValue(@Nullable String configValue, @Nullable String envValue, String name, String hint) {
        var resolved = firstNonBlankOrNull(configValue, envValue);
        if (resolved == null) {
            throw new KmsException("webIdentityCredentials missing '" + name + "': " + hint);
        }
        return resolved;
    }

    private static @Nullable String firstNonBlankOrNull(@Nullable String... values) {
        for (var v : values) {
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        return null;
    }

    private static String firstNonBlank(@Nullable String... values) {
        var v = firstNonBlankOrNull(values);
        if (v == null) {
            throw new KmsException("required value not provided");
        }
        return v;
    }

    private static String generatedSessionName() {
        return "kroxylicious-" + UUID.randomUUID();
    }

    private static String sanitiseSessionName(String raw) {
        // STS allows [\w+=,.@-]{2,64}; replace anything else with '-' and truncate.
        var cleaned = raw.replaceAll("[^\\w+=,.@-]", "-");
        if (cleaned.length() > MAX_SESSION_NAME_LENGTH) {
            cleaned = cleaned.substring(0, MAX_SESSION_NAME_LENGTH);
        }
        return cleaned;
    }

    private HttpClient createClient() {
        return HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(HTTP_CONNECT_TIMEOUT)
                .build();
    }

    @Override
    protected CompletionStage<AssumedRoleCredentials> fetchCredentials() {
        var token = readWebIdentityToken();
        var request = createAssumeRoleRequest(token);
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(this::parseAssumeRoleResponse);
    }

    @Override
    protected Instant expirationOf(AssumedRoleCredentials credentials) {
        return credentials.expiration();
    }

    @Override
    protected void onRefreshFailure(Throwable t) {
        LOGGER.atWarn()
                .setCause(LOGGER.isDebugEnabled() ? t : null)
                .addKeyValue("roleArn", roleArn)
                .addKeyValue("error", t.getMessage())
                .log(LOGGER.isDebugEnabled()
                        ? "refresh of IRSA credentials failed; check the IAM trust policy on the role and that the projected service-account token is valid"
                        : "refresh of IRSA credentials failed; increase log level to DEBUG for stacktrace");
    }

    @Override
    protected void onRefreshSuccess(AssumedRoleCredentials credentials) {
        LOGGER.atDebug()
                .addKeyValue("roleArn", roleArn)
                .addKeyValue("expiration", credentials.expiration())
                .log("Obtained AWS credentials from STS AssumeRoleWithWebIdentity");
    }

    private String readWebIdentityToken() {
        try {
            var token = Files.readString(webIdentityTokenFile, StandardCharsets.UTF_8).trim();
            if (token.isEmpty()) {
                throw new KmsException("web identity token file " + webIdentityTokenFile + " is empty");
            }
            return token;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read web identity token file " + webIdentityTokenFile, e);
        }
    }

    private HttpRequest createAssumeRoleRequest(String webIdentityToken) {
        var form = new StringBuilder()
                .append("Action=").append(STS_ACTION)
                .append("&Version=").append(STS_API_VERSION)
                .append("&RoleArn=").append(URLEncoder.encode(roleArn, StandardCharsets.UTF_8))
                .append("&RoleSessionName=").append(URLEncoder.encode(roleSessionName, StandardCharsets.UTF_8))
                .append("&WebIdentityToken=").append(URLEncoder.encode(webIdentityToken, StandardCharsets.UTF_8));
        if (durationSeconds != null) {
            form.append("&DurationSeconds=").append(durationSeconds);
        }

        return HttpRequest.newBuilder()
                .uri(stsEndpointUrl)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .timeout(HTTP_REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofString(form.toString(), StandardCharsets.UTF_8))
                .build();
    }

    private AssumedRoleCredentials parseAssumeRoleResponse(HttpResponse<byte[]> response) {
        var status = response.statusCode();
        var body = response.body();
        if (status < 200 || status >= 300) {
            throw new KmsException(formatStsError(status, body));
        }
        final AssumeRoleWithWebIdentityResponseEnvelope envelope;
        try {
            envelope = OBJECT_MAPPER.readValue(body, RESPONSE_TYPE_REF);
        }
        catch (IOException e) {
            throw new KmsException("Failed to parse AssumeRoleWithWebIdentity response: " + bodyAsString(body), e);
        }
        if (envelope.assumeRoleWithWebIdentityResponse() == null
                || envelope.assumeRoleWithWebIdentityResponse().assumeRoleWithWebIdentityResult() == null
                || envelope.assumeRoleWithWebIdentityResponse().assumeRoleWithWebIdentityResult().credentials() == null) {
            throw new KmsException("AssumeRoleWithWebIdentity response missing credentials: " + bodyAsString(body));
        }
        return envelope.assumeRoleWithWebIdentityResponse().assumeRoleWithWebIdentityResult().credentials();
    }

    private String formatStsError(int status, byte[] body) {
        try {
            var error = OBJECT_MAPPER.readValue(body, ERROR_TYPE_REF);
            if (error.errorResponse() != null && error.errorResponse().error() != null) {
                var inner = error.errorResponse().error();
                return "STS AssumeRoleWithWebIdentity failed with HTTP %d: code=%s, message=%s".formatted(status, inner.code(), inner.message());
            }
        }
        catch (IOException ignored) {
            // fall through to raw body
        }
        return "STS AssumeRoleWithWebIdentity failed with HTTP %d: %s".formatted(status, bodyAsString(body));
    }

    private static String bodyAsString(byte[] body) {
        return body == null ? "" : new String(body, StandardCharsets.UTF_8);
    }

    @VisibleForTesting
    String roleArn() {
        return roleArn;
    }

    @VisibleForTesting
    String roleSessionName() {
        return roleSessionName;
    }

    @VisibleForTesting
    URI stsEndpointUrl() {
        return stsEndpointUrl;
    }

    /**
     * The temporary credentials returned by STS AssumeRoleWithWebIdentity.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AssumedRoleCredentials(@JsonProperty("AccessKeyId") String accessKeyId,
                                         @JsonProperty("SecretAccessKey") String secretAccessKey,
                                         @JsonProperty("SessionToken") String sessionToken,
                                         @JsonProperty("Expiration") Instant expiration)
            implements Credentials {
        public AssumedRoleCredentials {
            Objects.requireNonNull(accessKeyId);
            Objects.requireNonNull(secretAccessKey);
            Objects.requireNonNull(sessionToken);
            Objects.requireNonNull(expiration);
        }

        @Override
        public Optional<String> securityToken() {
            return Optional.of(sessionToken);
        }

        @Override
        public String toString() {
            return "AssumedRoleCredentials{" +
                    "accessKeyId='" + accessKeyId + '\'' +
                    ", secretAccessKey='***************'" +
                    ", sessionToken='***************'" +
                    ", expiration=" + expiration +
                    '}';
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record AssumeRoleWithWebIdentityResponseEnvelope(@JsonProperty("AssumeRoleWithWebIdentityResponse") @Nullable AssumeRoleWithWebIdentityResponseBody assumeRoleWithWebIdentityResponse) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record AssumeRoleWithWebIdentityResponseBody(@JsonProperty("AssumeRoleWithWebIdentityResult") @Nullable AssumeRoleWithWebIdentityResult assumeRoleWithWebIdentityResult) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record AssumeRoleWithWebIdentityResult(@JsonProperty("Credentials") @Nullable AssumedRoleCredentials credentials) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record StsErrorEnvelope(@JsonProperty("ErrorResponse") @Nullable StsErrorResponse errorResponse) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record StsErrorResponse(@JsonProperty("Error") @Nullable StsError error) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record StsError(@JsonProperty("Code") String code, @JsonProperty("Message") String message) {}
}
