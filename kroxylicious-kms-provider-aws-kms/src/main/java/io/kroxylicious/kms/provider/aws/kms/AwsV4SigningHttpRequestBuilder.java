/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import io.kroxylicious.kms.provider.aws.kms.credentials.Credentials;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static java.net.http.HttpRequest.BodyPublisher;
import static java.net.http.HttpRequest.Builder;

/**
 * An implementation of HttpRequestBuilder that signs AWS requests
 * accordance with <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html">AWS v4</a>.
 */
class AwsV4SigningHttpRequestBuilder implements Builder {

    private static final Pattern CONSECUTIVE_WHITESPACE = Pattern.compile("\\s+");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    private static final HexFormat HEX_FORMATTER = HexFormat.of();

    private static final String NO_PAYLOAD_HEXED_SHA256 = HEX_FORMATTER.formatHex(newSha256Digester().digest(new byte[]{}));

    private static final String X_AMZ_DATE_HEADER = "X-Amz-Date";

    private static final String X_AMZ_SECURITY_TOKEN_HEADER = "X-Amz-Security-Token";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String HOST_HEADER = "Host";
    private static final String AWS_4_REQUEST = "aws4_request";

    private final String region;
    private final String service;
    private final Instant date;
    private final Builder builder;
    private final Credentials credentials;
    private @Nullable String payloadHexedSha56;

    /**
     * Creates an AwsV4SigningHttpRequestBuilder builder.
     *
     * @param credentials AWS credentials
     * @param region AWS region
     * @param service AWS service
     * @param date request date
     * @return a new request builder
     */
    public static Builder newBuilder(Credentials credentials,
                                     String region,
                                     String service,
                                     Instant date) {
        return new AwsV4SigningHttpRequestBuilder(region, service, date, HttpRequest.newBuilder(), credentials);
    }

    private AwsV4SigningHttpRequestBuilder(String region, String service, Instant date, Builder builder, Credentials credentials) {
        Objects.requireNonNull(credentials);
        Objects.requireNonNull(region);
        Objects.requireNonNull(service);
        Objects.requireNonNull(date);
        Objects.requireNonNull(builder);
        this.credentials = credentials;
        this.region = region;
        this.service = service;
        this.date = date;
        this.builder = builder;
    }

    @Override
    public Builder expectContinue(boolean enable) {
        builder.expectContinue(enable);
        return this;
    }

    @Override
    public Builder version(HttpClient.Version version) {
        builder.version(version);
        return this;
    }

    @Override
    public Builder header(String name, String value) {
        builder.header(name, value);
        return this;
    }

    @Override
    public Builder headers(String... headers) {
        builder.headers(headers);
        return this;
    }

    @Override
    public Builder timeout(Duration duration) {
        builder.timeout(duration);
        return this;
    }

    @Override
    public Builder setHeader(String name, String value) {
        builder.setHeader(name, value);
        return this;
    }

    @Override
    public Builder GET() {
        builder.GET();
        return this;
    }

    @Override
    public Builder POST(BodyPublisher bodyPublisher) {
        builder.POST(digestingPublisher(bodyPublisher));
        return this;
    }

    @Override
    public Builder PUT(BodyPublisher bodyPublisher) {
        builder.PUT(digestingPublisher(bodyPublisher));
        return this;
    }

    @Override
    public Builder DELETE() {
        builder.DELETE();
        return this;
    }

    @Override
    public Builder method(String method, BodyPublisher bodyPublisher) {
        builder.method(method, digestingPublisher(bodyPublisher));
        return this;
    }

    @Override
    public Builder copy() {
        return new AwsV4SigningHttpRequestBuilder(region, service, date, builder.copy(), credentials);
    }

    @Override
    public Builder uri(URI uri) {
        builder.uri(uri);
        return this;
    }

    @Override
    public HttpRequest build() {
        signRequest();
        return builder.build();
    }

    private BodyPublisher digestingPublisher(BodyPublisher bodyPublisher) {
        var items = new ArrayList<BodyPublisher>();

        bodyPublisher.subscribe(new Flow.Subscriber<>() {
            final MessageDigest digest = newSha256Digester();

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
                digest.update(item.array());
                items.add(HttpRequest.BodyPublishers.ofByteArray(item.array()));
            }

            @Override
            public void onError(Throwable throwable) {
                throw new IllegalStateException(throwable);
            }

            @Override
            public void onComplete() {
                payloadHexedSha56 = HEX_FORMATTER.formatHex(digest.digest());
            }
        });

        return HttpRequest.BodyPublishers.concat(items.toArray(new BodyPublisher[]{}));
    }

    /**
     * This method implements <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html">signs the request</a>
     * using the AWS v4 algorithm.
     */
    private void signRequest() {
        var isoDateTime = DATE_TIME_FORMATTER.format(date);
        var isoDate = isoDateTime.substring(0, 8);
        var unsignedRequest = builder.build();

        // Note: AWS only specifies signing behaviour for headers with a single value.
        var allHeaders = new HashMap<>(getSingleValuedHeaders(unsignedRequest));
        allHeaders.put(HOST_HEADER, getHostHeaderForSigning(unsignedRequest.uri()));
        allHeaders.put(X_AMZ_DATE_HEADER, isoDateTime);

        var canonicalRequest = computeCanonicalRequest(allHeaders, unsignedRequest);
        var stringToSign = computeStringToSign(canonicalRequest, isoDateTime, isoDate);
        var authorization = computeAuthorization(canonicalRequest, stringToSign, isoDate);

        builder.header(AUTHORIZATION_HEADER, authorization);
        builder.header(X_AMZ_DATE_HEADER, isoDateTime);
        // The security token is added as a header but is not part of the authorization
        // https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html
        credentials.securityToken().ifPresent(securityToken -> builder.header(X_AMZ_SECURITY_TOKEN_HEADER, securityToken));
    }

    private CanonicalRequestResult computeCanonicalRequest(Map<String, String> allHeaders,
                                                           HttpRequest unsignedRequest) {
        var method = unsignedRequest.method();
        var uri = unsignedRequest.uri();
        var path = Optional.ofNullable(uri.getPath()).filter(Predicate.not(String::isEmpty)).orElse("/");
        var query = Optional.ofNullable(uri.getQuery()).orElse("");

        var canonicalRequestLines = new ArrayList<String>();
        canonicalRequestLines.add(method);
        canonicalRequestLines.add(path);
        canonicalRequestLines.add(query);
        var hashedHeaders = new ArrayList<String>(allHeaders.size());
        var headerKeysSorted = allHeaders.keySet().stream().sorted(Comparator.comparing(n -> n.toLowerCase(Locale.ROOT))).toList();
        for (String key : headerKeysSorted) {
            hashedHeaders.add(key.toLowerCase(Locale.ROOT));
            canonicalRequestLines.add(key.toLowerCase(Locale.ROOT) + ":" + normalizeHeaderValue((allHeaders).get(key)));
        }
        canonicalRequestLines.add(null);
        var signedHeaders = String.join(";", hashedHeaders);
        canonicalRequestLines.add(signedHeaders);
        canonicalRequestLines.add(payloadHexedSha56 == null ? NO_PAYLOAD_HEXED_SHA256 : payloadHexedSha56);
        var canonicalRequestBody = canonicalRequestLines.stream().map(line -> line == null ? "" : line).collect(Collectors.joining("\n"));
        var canonicalRequestHash = HEX_FORMATTER.formatHex(sha256(canonicalRequestBody.getBytes(StandardCharsets.UTF_8)));
        return new CanonicalRequestResult(signedHeaders, canonicalRequestHash);
    }

    private record CanonicalRequestResult(String signedHeaders, String canonicalRequestHash) {}

    private StringToSignResult computeStringToSign(CanonicalRequestResult canonicalRequestResult,
                                                   String isoDateTime,
                                                   String isoDate) {
        var stringToSignLines = new ArrayList<String>();
        stringToSignLines.add("AWS4-HMAC-SHA256");
        stringToSignLines.add(isoDateTime);
        var credentialScope = isoDate + "/" + region + "/" + service + "/" + AWS_4_REQUEST;
        stringToSignLines.add(credentialScope);
        stringToSignLines.add(canonicalRequestResult.canonicalRequestHash());
        var stringToSign = String.join("\n", stringToSignLines);
        return new StringToSignResult(credentialScope, stringToSign);
    }

    private record StringToSignResult(String credentialScope, String stringToSign) {}

    private String computeAuthorization(CanonicalRequestResult canonicalRequestResult,
                                        StringToSignResult stringToSignResult,
                                        String isoDate) {
        var dateHmac = hmac(("AWS4" + credentials.secretAccessKey()).getBytes(StandardCharsets.UTF_8), isoDate);
        var regionHmac = hmac(dateHmac, this.region);
        var serviceHmac = hmac(regionHmac, this.service);
        var signHmac = hmac(serviceHmac, AWS_4_REQUEST);
        var signature = HEX_FORMATTER.formatHex(hmac(signHmac, stringToSignResult.stringToSign()));

        return "AWS4-HMAC-SHA256 Credential=" + credentials.accessKeyId() + "/" + stringToSignResult.credentialScope() + ", SignedHeaders="
                + canonicalRequestResult.signedHeaders()
                + ", Signature=" + signature;
    }

    /**
     * This implementation must match the HTTP client's computation of the contents of the Host header.
     *
     * @param uri metadataEndpoint
     * @return host string
     */
    @VisibleForTesting
    String getHostHeaderForSigning(URI uri) {
        int port = uri.getPort();
        String host = uri.getHost();

        boolean defaultPort;
        if (port == -1) {
            defaultPort = true;
        }
        else if (uri.getScheme().toLowerCase(Locale.ROOT).equals("https")) {
            defaultPort = port == 443;
        }
        else {
            defaultPort = port == 80;
        }

        if (defaultPort) {
            return host;
        }
        else {
            return host + ":" + port;
        }
    }

    /**
     * Canonical header values must remove excess white space before and after values, and convert sequential spaces to a single
     * space. See <a href="https://github.com/aws/aws-sdk-java-v2/blob/26bb6dcf058b08f55665f931d02937238b00e576/core/auth/src/test/java/software/amazon/awssdk/auth/signer/Aws4SignerTest.java#L182">canonicalizedHeaderString_valuesWithExtraWhitespace_areTrimmed</a>
     *
     * @param value header value
     * @return normalised header value
     */
    private static String normalizeHeaderValue(String value) {
        return CONSECUTIVE_WHITESPACE.matcher(value).replaceAll(" ").trim();
    }

    private static byte[] sha256(byte[] bytes) {
        var digest = newSha256Digester();
        digest.update(bytes);
        return digest.digest();
    }

    private static byte[] hmac(byte[] key, String msg) {
        try {
            var mac = newHmacSha256();
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(msg.getBytes(StandardCharsets.UTF_8));
        }
        catch (InvalidKeyException e) {
            throw new KmsException("Failed to initialize hmac", e);
        }
    }

    private static Mac newHmacSha256() {
        try {
            return Mac.getInstance("HmacSHA256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new KmsException("Failed to create SHA-256 hmac", e);
        }
    }

    private static MessageDigest newSha256Digester() {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new KmsException("Failed to create SHA-256 digester", e);
        }
    }

    private static Map<String, String> getSingleValuedHeaders(HttpRequest request) {
        return request.headers().map().entrySet().stream()
                .filter(AwsV4SigningHttpRequestBuilder::hasSingleValue)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
    }

    private static boolean hasSingleValue(Map.Entry<String, List<String>> e) {
        return e.getValue() != null && e.getValue().size() == 1;
    }
}
