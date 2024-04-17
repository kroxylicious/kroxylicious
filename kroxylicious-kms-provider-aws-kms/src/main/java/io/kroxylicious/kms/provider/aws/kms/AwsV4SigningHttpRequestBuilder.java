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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import io.kroxylicious.kms.service.KmsException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.net.http.HttpRequest.BodyPublisher;
import static java.net.http.HttpRequest.Builder;

/**
 * An implementation of HttpRequestBuilder that signs AWS requests
 * accordance with <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html">AWS v4</a>.
 */
class AwsV4SigningHttpRequestBuilder implements Builder {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    private static final HexFormat HEX_FORMATTER = HexFormat.of();

    private static final String NO_PAYLOAD_HEXED_SHA256 = HEX_FORMATTER.formatHex(getSha256Digester().digest(new byte[]{}));

    private static final String X_AMZ_DATE_HEADER = "X-Amz-Date";
    public static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String HOST_HEADER = "Host";
    private static final String AWS_4_REQUEST = "aws4_request";

    private final String accessKey;
    private final String secretKey;
    private final String region;
    private final String service;
    private final Instant date;
    private final Builder builder;
    private String payloadHexedSha56;

    /**
     *
     * Creates an AwsV4SigningHttpRequestBuilder builder.
     *
     * @param accessKey AWS access key
     * @param secretKey AWS secret key
     * @param region AWS region
     * @param service AWS service
     * @param date request date
     * @return a new request builder
     */
    public static Builder newBuilder(@NonNull String accessKey, @NonNull String secretKey, @NonNull String region, @NonNull String service, @NonNull Instant date) {
        return new AwsV4SigningHttpRequestBuilder(accessKey, secretKey, region, service, date, HttpRequest.newBuilder());
    }

    private AwsV4SigningHttpRequestBuilder(String accessKey, String secretKey, String region, String service, Instant date, Builder builder) {
        Objects.requireNonNull(accessKey);
        Objects.requireNonNull(secretKey);
        Objects.requireNonNull(region);
        Objects.requireNonNull(service);
        Objects.requireNonNull(date);
        Objects.requireNonNull(builder);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
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
        return new AwsV4SigningHttpRequestBuilder(accessKey, secretKey, region, service, date, builder.copy());
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

    @NonNull
    private BodyPublisher digestingPublisher(BodyPublisher bodyPublisher) {
        var items = new ArrayList<BodyPublisher>();

        bodyPublisher.subscribe(new Flow.Subscriber<>() {
            final MessageDigest digest = getSha256Digester();

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

        var allHeaders = new HashMap<String, String>();
        unsignedRequest.headers().map().keySet().forEach(k -> unsignedRequest.headers().firstValue(k).ifPresent(value -> allHeaders.put(k, value)));
        allHeaders.put(HOST_HEADER, computeHostHeader(unsignedRequest.uri()));
        allHeaders.put(X_AMZ_DATE_HEADER, isoDateTime);

        try {
            var canonicalRequest = computeCanonicalRequest(allHeaders, unsignedRequest);
            var stringToSign = computeStringToSign(canonicalRequest, isoDateTime, isoDate);
            var authorization = computeAuthorization(canonicalRequest, stringToSign, isoDate);

            builder.header(AUTHORIZATION_HEADER, authorization);
            builder.header(X_AMZ_DATE_HEADER, isoDateTime);
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to sign the request", e);
        }
    }

    @NonNull
    private CanonicalRequestResult computeCanonicalRequest(Map<String, String> allHeaders, HttpRequest unsignedRequest) {
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
            canonicalRequestLines.add(key.toLowerCase(Locale.ROOT) + ":" + normalizeSpaces((allHeaders).get(key)));
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

    @NonNull
    private StringToSignResult computeStringToSign(CanonicalRequestResult canonicalRequestResult, String isoDateTime, String isoDate) {
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

    @NonNull
    private String computeAuthorization(CanonicalRequestResult canonicalRequestResult, StringToSignResult stringToSignResult, String isoDate) {
        var dateHmac = hmac(("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8), isoDate);
        var regionHmac = hmac(dateHmac, this.region);
        var serviceHmac = hmac(regionHmac, this.service);
        var signHmac = hmac(serviceHmac, AWS_4_REQUEST);
        var signature = HEX_FORMATTER.formatHex(hmac(signHmac, stringToSignResult.stringToSign()));

        return "AWS4-HMAC-SHA256 Credential=" + accessKey + "/" + stringToSignResult.credentialScope() + ", SignedHeaders=" + canonicalRequestResult.signedHeaders()
                + ", Signature=" + signature;
    }

    private String computeHostHeader(URI uri) {
        if ((uri.getScheme().equals("http") && uri.getPort() == 80) || (uri.getScheme().equals("https") && uri.getPort() == 443)) {
            return uri.getHost();
        }
        else {
            return uri.getHost() + ":" + uri.getPort();
        }
    }

    private static String normalizeSpaces(String value) {
        return value.replaceAll("\\s+", " ").trim();
    }

    private static byte[] sha256(byte[] bytes) {
        var digest = getSha256Digester();
        digest.update(bytes);
        return digest.digest();
    }

    private static byte[] hmac(byte[] key, String msg) {
        try {
            var mac = getHmacSHA256();
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(msg.getBytes(StandardCharsets.UTF_8));
        }
        catch (InvalidKeyException e) {
            throw new KmsException("Failed to initialize hmac", e);
        }
    }

    @NonNull
    private static Mac getHmacSHA256() {
        try {
            return Mac.getInstance("HmacSHA256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new KmsException("Failed to create SHA-256 hmac", e);
        }
    }

    private static MessageDigest getSha256Digester() {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new KmsException("Failed to create SHA-256 digester", e);
        }
    }
}
