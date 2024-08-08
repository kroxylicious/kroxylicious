/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.encryption.config.CipherOverrideConfig;
import io.kroxylicious.filter.encryption.config.CipherOverrides;
import io.kroxylicious.filter.encryption.config.CipherSpec;
import io.kroxylicious.filter.encryption.config.EncryptionConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class Aes implements CipherManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(Aes.class);
    private static final String DEFAULT_AES256_GCM_TRANSFORMATION = "AES_256/GCM/NoPadding";
    private static final Set<String> ALLOWED_AES256_GCM_TRANSFORMATIONS = Set.of(DEFAULT_AES256_GCM_TRANSFORMATION, "AES/GCM/NoPadding");

    public static Aes aes256gcm128(CipherOverrideConfig config) {
        String transformation = overrideTransformationElseDefault(config);
        String provider = getProvider(config);
        return new Aes(transformation, provider, (byte) 0, CipherSpec.AES_256_GCM_128);
    }

    @Nullable
    private static String getProvider(CipherOverrideConfig config) {
        if (config.overridesMap().containsKey(CipherSpec.AES_256_GCM_128)) {
            CipherOverrides cipherOverrides = config.overridesMap().get(CipherSpec.AES_256_GCM_128);
            return cipherOverrides.provider();
        }
        else {
            return null;
        }
    }

    private static @NonNull String overrideTransformationElseDefault(CipherOverrideConfig config) {
        String transformation = DEFAULT_AES256_GCM_TRANSFORMATION;
        if (config.overridesMap().containsKey(CipherSpec.AES_256_GCM_128)) {
            CipherOverrides cipherOverrides = config.overridesMap().get(CipherSpec.AES_256_GCM_128);
            String overrideTransformation = cipherOverrides.transformation();
            if (overrideTransformation != null) {
                if (ALLOWED_AES256_GCM_TRANSFORMATIONS.contains(overrideTransformation)) {
                    LOGGER.info("overriding AES256 GCM_128 transformation to {}", overrideTransformation);
                    transformation = overrideTransformation;
                }
                else {
                    throw new EncryptionConfigurationException(
                            "AES_256_GCM_128 override transformation: " + overrideTransformation + " is not one of the allowed values: "
                                    + ALLOWED_AES256_GCM_TRANSFORMATIONS.stream().sorted().toList());
                }
            }
        }
        return transformation;
    }

    private static final int IV_SIZE_BYTES = 12;
    private static final int TAG_LENGTH_BITS = 128;
    private final String transformation;
    private final String provider;
    private final byte serializedId;
    private final CipherSpec spec;
    private final SecureRandom rng;

    private Aes(String transformation, @Nullable String provider, byte serializedId, CipherSpec spec) {
        this.transformation = transformation;
        this.provider = provider;
        this.serializedId = serializedId;
        this.spec = spec;
        rng = new SecureRandom();
    }

    @Override
    public byte serializedId() {
        return serializedId;
    }

    @Override
    public CipherSpec name() {
        return spec;
    }

    @Override
    public Cipher newCipher() {
        try {
            if (provider != null) {
                return Cipher.getInstance(transformation, provider);
            }
            else {
                return Cipher.getInstance(transformation);
            }
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException | NoSuchProviderException e) {
            throw new DekException(e);
        }
    }

    @Override
    public long maxEncryptionsPerKey() {
        return 1L << 32; // 2^32
    }

    @Override
    public Supplier<AlgorithmParameterSpec> paramSupplier() {
        var generator = new Wrapping96BitCounter(rng);
        var iv = new byte[IV_SIZE_BYTES];
        return () -> {
            generator.generateIv(iv);
            return new GCMParameterSpec(TAG_LENGTH_BITS, iv);
        };
    }

    @Override
    public int constantParamsSize() {
        return IV_SIZE_BYTES;
    }

    @Override
    public int size(AlgorithmParameterSpec parameterSpec) {
        return constantParamsSize();
    }

    @Override
    public void writeParameters(
                                ByteBuffer parametersBuffer,
                                AlgorithmParameterSpec params) {
        parametersBuffer.put(((GCMParameterSpec) params).getIV());
    }

    @Override
    public GCMParameterSpec readParameters(ByteBuffer parametersBuffer) {
        byte[] b = new byte[IV_SIZE_BYTES];
        parametersBuffer.get(b);
        return new GCMParameterSpec(TAG_LENGTH_BITS, b);
    }

    // equality important as the class is used as a map key in AbstractResolver
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Aes aes = (Aes) o;
        return serializedId == aes.serializedId && Objects.equals(transformation, aes.transformation) && spec == aes.spec;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformation, serializedId, spec);
    }
}
