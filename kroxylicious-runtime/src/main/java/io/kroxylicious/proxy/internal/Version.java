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
        if ("io.kroxylicious.proxy.filter.FilterFactory".equals(api)) {
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

    private enum Stability {
        ALPHA,
        BETA,
        STABLE
    }

    private static final Pattern VERSION_PATTERN = Pattern.compile(
            "v(?<major>\\d+)(?:(?<stability>alpha|beta)(?<minor>\\d+))?");

    private final int major;
    private final Stability stability;
    private final int minor;

    private Version(int major,
                    Stability stability,
                    int minor) {
        this.major = major;
        this.stability = stability;
        this.minor = minor;
    }

    /**
     * Parse the given string as an API version.
     * @param apiVersion The string to parse
     * @return The version.
     * @throws IllegalArgumentException if the string is not a valid version.
     */
    public static Version parse(String apiVersion) {
        var matcher = VERSION_PATTERN.matcher(apiVersion);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid API version: " + apiVersion);
        }

        int major = Integer.parseInt(matcher.group("major"));
        if (major == 0) {
            throw new IllegalArgumentException("Invalid API version: " + apiVersion);
        }

        String stabilityStr = matcher.group("stability");
        if (stabilityStr == null) {
            return new Version(major, Stability.STABLE, 0);
        }

        int minor = Integer.parseInt(matcher.group("minor"));
        if (minor == 0) {
            throw new IllegalArgumentException("Invalid API version: " + apiVersion);
        }
        Stability stability = "alpha".equals(stabilityStr) ? Stability.ALPHA : Stability.BETA;
        return new Version(major, stability, minor);
    }

    /**
     * The major part of the version number.
     * For example in {@code v2alpha1} the major part is 2.
     * @return The major part of the version number.
     */
    public int major() {
        return major;
    }

    /**
     * The minor part of the version number.
     * For example in {@code v2alpha1} the minor part is 1.
     * @return The minor part of the version number.
     */
    public int minor() {
        return minor;
    }

    /**
     * Returns whether this is a stable version (lacking {@code alpha} or {@code beta}).
     * @return true if and only if this is a stable version (lacking {@code alpha} or {@code beta}).
     */
    public boolean isStable() {
        return stability == Stability.STABLE;
    }

    /**
     * Returns whether a plugin compiled against {@code compiledAgainst} is compatible
     * with this (running) version.
     * <p>Versions are compatible only if exactly equal. Unstable versions can evolve
     * incompatibly, and stable version bumps (e.g. {@code v1} to {@code v2}) signal
     * breaking changes, so exact match is the only guarantee we can offer.</p>
     * @param compiledAgainst The version the plugin was compiled against.
     * @return true if and only if the versions are compatible.
     */
    public boolean isCompatibleWith(Version compiledAgainst) {
        return this.equals(compiledAgainst);
    }

    @Override
    public String toString() {
        return switch (stability) {
            case STABLE -> "v" + major;
            case ALPHA -> "v" + major + "alpha" + minor;
            case BETA -> "v" + major + "beta" + minor;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Version other)) {
            return false;
        }
        return major == other.major
                && stability == other.stability
                && minor == other.minor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, stability, minor);
    }

    @Override
    public int compareTo(Version o) {
        int cmp = Integer.compare(major, o.major);
        if (cmp != 0) {
            return cmp;
        }
        cmp = stability.compareTo(o.stability);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(minor, o.minor);
    }
}
