/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A Kubernetes syntax API version, as used for versioning Kroxylicious APIs, e.g. {@code v1alpha1}, {@code v1beta2}, or {@code v2}.
 * (This class would be called ApiVersion, but that's too easily confused with the Kafka ApiVersions RPC).
 */
public class Version implements Comparable<Version> {

    static final String ENV_VAR = "KROXYLICIOUS_ALLOWED_UNSTABLE_APIS";

    private static final Set<String> ALLOWED_UNSTABLE_APIS;

    static {
        var apis = System.getenv(ENV_VAR);
        if (apis != null) {
            ALLOWED_UNSTABLE_APIS = Arrays.stream(apis.strip().split("\\s*,\\s*")).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
        }
        else {
            ALLOWED_UNSTABLE_APIS = Collections.emptySet();
        }
    }

    /**
     * Return whether an API is stable, or has been explicitly allowed.
     * @param api The API.
     * @param version The API's version.
     * @return true if and only if the API is stable, or has been explicitly allowed
     */
    public static boolean isAllowedApi(String api, Version version) {
        Objects.requireNonNull(api);
        Objects.requireNonNull(version);
        if ("io.kroxylicious.proxy.filter.FilterFactory".equals(api)
                && version.equals(Version.parse("v1beta1"))) {
            // Requiring an explicit opt-in for the key API of the whole proxy would just lead to user frustration.
            // So always allow this as a special case.
            return true;
        }
        if (version.isStable()) {
            return true;
        }
        else {
            return ALLOWED_UNSTABLE_APIS.contains(api);
        }
    }

    public static class DisallowedUnstableApiException extends RuntimeException {
        public DisallowedUnstableApiException(String message) {
            super(Objects.requireNonNull(message));
        }
    }

    public static class IncompatibleApiVersionException extends RuntimeException {
        public IncompatibleApiVersionException(String message) {
            super(Objects.requireNonNull(message));
        }
    }

    /**
     * Throws {@link DisallowedUnstableApiException} if the given API's version
     * is not stable and has not been explicitly allowed.
     * @param api The API.
     * @param version The API's version.
     * @throws DisallowedUnstableApiException unless the API is allowed
     */
    public static void throwUnlessApiIsAllowed(String api, Version version) {
        Objects.requireNonNull(api);
        Objects.requireNonNull(version);
        if (!isAllowedApi(api, version)) {
            throw new DisallowedUnstableApiException("API '" + api + "' has unstable version " + version + ", which you have not opted into using." +
                    " To opt-in to using this unstable API include '" + api + "'" +
                    " in the comma-separated list of APIs given as the value of the " + ENV_VAR + " environment variable." +
                    " For example '" + ENV_VAR + "=" + api + Optional.ofNullable(System.getenv(ENV_VAR)).map(envVarValue -> "," + envVarValue).orElse("") + "'.");
        }
    }

    private final int value;

    private Version(int value) {
        this.value = value;
    }

    /**
     * Parse the given string as an API version
     * @param apiVersion The string to parse
     * @return The version.
     * @throws IllegalArgumentException if the string is not a version or if there are too many digits in one of the numeric parts of the version.
     */
    public static Version parse(String apiVersion) {
        var matcher = Pattern.compile("v(?<major>\\d+)(?:(?<stability>alpha|beta)(?<minor>\\d+))?").matcher(apiVersion);
        if (matcher.matches()) {
            var major = Integer.parseInt(matcher.group("major"));
            var stabilityStr = matcher.group("stability");
            int stability;
            int minor;
            if (stabilityStr != null) {
                stability = stabilityStr.charAt(0) == 'a' ? 0 : 1;
                String minorStr = matcher.group("minor");
                minor = Integer.parseInt(minorStr);
            }
            else {
                stability = 2;
                minor = 0;
            }
            // Both major and minor can be upto 15 bits (unsigned). That's 32767 decimal.
            // If you have more versions than that then you have other problems.
            if ((major & 0b111111111111111_11_000000000000000) != 0) {
                throw new IllegalArgumentException("Invalid API version: " + apiVersion);
            }
            if ((minor & 0b111111111111111_11_000000000000000) != 0) {
                throw new IllegalArgumentException("Invalid API version: " + apiVersion);
            }
            if (major == 0 || (stability != 2 && minor == 0)) {
                throw new IllegalArgumentException("Invalid API version: " + apiVersion);
            }

            return new Version(major << 17 | stability << 15 | minor);
        }
        else {
            throw new IllegalArgumentException("Invalid API version: " + apiVersion);
        }
    }

    /**
     * The major part of the version number.
     * For example in {@code v2alpha1} the major part is 2.
     * @return The major part of the version number.
     */
    public int major() {
        return value >>> 17;
    }

    /**
     * The minor part of the version number.
     * For example in {@code v2alpha1} the minor part is 1.
     * @return The minor part of the version number.
     */
    public int minor() {
        return value & ~0xffff8000;
    }

    /**
     * Returns whether this is a stable version (lacking {@code alpha} or {@code beta}).
     * @return true if and only if this is a stable version (lacking {@code alpha} or {@code beta}).
     */
    public boolean isStable() {
        return ((value >> 15) & 0x000000003) == 2;
    }

    /**
     * Returns whether a plugin compiled against {@code compiledAgainst} is compatible
     * with this (running) version.
     * <p>Stable versions are backwards-compatible within a major version, so any prior version
     * with the same major is compatible. Unstable versions are only compatible if exactly equal.</p>
     * @param compiledAgainst The version the plugin was compiled against.
     * @return true if and only if the versions are compatible.
     */
    public boolean isCompatibleWith(Version compiledAgainst) {
        if (this.equals(compiledAgainst)) {
            return true;
        }
        return this.isStable() && this.major() == compiledAgainst.major();
    }

    private String stability() {
        int i = (value >> 15) & 0x000000003;
        return switch (i) {
            case 0 -> "alpha";
            case 1 -> "beta";
            case 2 -> "";
            default -> throw new IllegalStateException("" + i);
        };
    }

    @Override
    public String toString() {
        return "v" + Integer.toUnsignedString(major()) + (isStable() ? "" : stability() + Integer.toUnsignedString(minor()));
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Version version)) {
            return false;
        }
        return value == version.value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public int compareTo(Version o) {
        return Integer.compareUnsigned(value, o.value);
    }
}
