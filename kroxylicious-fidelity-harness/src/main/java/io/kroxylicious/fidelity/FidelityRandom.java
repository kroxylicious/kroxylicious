/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Random;

import org.apache.kafka.common.protocol.types.BoundField;

import io.kroxylicious.proxy.tag.ThreadSafe;

/**
 * Manages the random seed for fidelity test suites.
 * <p>
 * Ensures that fidelity tests explore different values on each CI run by default while remaining
 * fully reproducible. A base seed is selected once per test run, either from the system property
 * {@code fidelity.seed}, or generated randomly.
 * Each test case's seed is derived deterministically from the base seed and a stable case identifier
 * (message name, direction, version, and optional nullable field name), making individual test
 * reproducibility completely independent of test execution order.
 */
@ThreadSafe
public final class FidelityRandom {

    /**
     * System property name to override the base seed.
     */
    public static final String SEED_PROPERTY = "fidelity.seed";

    private static final Object LOCK = new Object();
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static volatile Long effectiveBaseSeed;

    private FidelityRandom() {
    }

    /**
     * Returns the base seed for this test run, initializing it on first access if not yet set.
     *
     * <p>This method is thread-safe; initialization occurs exactly once regardless of concurrent calls.
     *
     * @return the effective base seed
     */
    public static long baseSeed() {
        if (effectiveBaseSeed == null) {
            synchronized (LOCK) {
                if (effectiveBaseSeed == null) {
                    effectiveBaseSeed = initializeBaseSeed();
                }
            }
        }
        return effectiveBaseSeed;
    }

    @SuppressWarnings("java:S106")
    private static long initializeBaseSeed() {
        String prop = System.getProperty(SEED_PROPERTY);
        long seed;
        if (prop != null && !prop.isBlank()) {
            seed = parseSeed(prop);
        }
        else {
            seed = SECURE_RANDOM.nextLong();
        }
        // CHECKSTYLE:OFF
        System.out.println("Fidelity tests randomized with seed " + seed + ". To reproduce, use -D" + SEED_PROPERTY + "=" + seed);
        // CHECKSTYLE:ON
        return seed;
    }

    static long parseSeed(String str) {
        String trimmed = str.trim();
        try {
            return Long.decode(trimmed);
        }
        catch (NumberFormatException e) {
            try {
                return Long.parseLong(trimmed);
            }
            catch (NumberFormatException ex) {
                return Long.parseUnsignedLong(trimmed, 16);
            }
        }
    }

    /**
     * Derives a deterministic 64-bit seed from a base seed and a case identifier.
     *
     * @param baseSeed the base seed
     * @param caseIdentifier the unique, stable identifier for the test case
     * @return the derived seed
     */
    public static long deriveSeed(long baseSeed, String caseIdentifier) {
        Objects.requireNonNull(caseIdentifier, "caseIdentifier must not be null");
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(baseSeed);
            digest.update(buffer.array());
            digest.update(caseIdentifier.getBytes(StandardCharsets.UTF_8));
            byte[] hash = digest.digest();
            return ByteBuffer.wrap(hash).getLong();
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest algorithm not available", e);
        }
    }

    /**
     * Derives the seed for the given case identifier using the current {@link #baseSeed()}.
     *
     * @param caseIdentifier the test case identifier
     * @return the derived seed
     */
    public static long seedFor(String caseIdentifier) {
        return deriveSeed(baseSeed(), caseIdentifier);
    }

    /**
     * Creates a {@link Random} instance initialized with a seed derived from the current {@link #baseSeed()}
     * and the given case identifier.
     *
     * @param caseIdentifier the test case identifier
     * @return a new Random instance
     */
    @SuppressWarnings("java:S2245") // pseudo-random number generator is intentional for reproducible fidelity tests
    public static Random randomFor(String caseIdentifier) {
        return new Random(seedFor(caseIdentifier));
    }

    /**
     * Creates a {@link Random} instance for a message and version.
     *
     * @param message the Kafka or Kroxylicious message instance
     * @param version the protocol version
     * @return a new Random instance
     */
    public static Random randomFor(Object message, short version) {
        return randomFor(caseIdentifier(message, version));
    }

    /**
     * Creates a {@link Random} instance for a message, version, and nullable field.
     *
     * @param message the Kafka or Kroxylicious message instance
     * @param version the protocol version
     * @param nullableField the nullable field being tested
     * @return a new Random instance
     */
    public static Random randomFor(Object message, short version, BoundField nullableField) {
        return randomFor(caseIdentifier(message, version, nullableField));
    }

    /**
     * Generates a stable case identifier from a message instance and protocol version.
     *
     * @param message the message instance
     * @param version the protocol version
     * @return the case identifier matching {@link AllMessages.VersionedMessage#label()} format
     */
    public static String caseIdentifier(Object message, short version) {
        Objects.requireNonNull(message, "message must not be null");
        String simpleName = message.getClass().getSimpleName();
        String prefix = parseMessageNameAndDirection(simpleName);
        return prefix + " - v" + version;
    }

    /**
     * Generates a stable case identifier from a message instance, protocol version, and nullable field.
     *
     * @param message the message instance
     * @param version the protocol version
     * @param nullableField the nullable field
     * @return the case identifier
     */
    public static String caseIdentifier(Object message, short version, BoundField nullableField) {
        Objects.requireNonNull(nullableField, "nullableField must not be null");
        return caseIdentifier(message, version) + " - null " + nullableField.def.name;
    }

    static String parseMessageNameAndDirection(String simpleName) {
        if (simpleName.endsWith("RequestData") || simpleName.endsWith("ResponseData")) {
            return simpleName.substring(0, simpleName.length() - 4);
        }
        return simpleName;
    }

    static void resetBaseSeedForTesting() {
        synchronized (LOCK) {
            effectiveBaseSeed = null;
        }
    }

    static void setBaseSeedForTesting(long seed) {
        synchronized (LOCK) {
            effectiveBaseSeed = seed;
        }
    }
}
