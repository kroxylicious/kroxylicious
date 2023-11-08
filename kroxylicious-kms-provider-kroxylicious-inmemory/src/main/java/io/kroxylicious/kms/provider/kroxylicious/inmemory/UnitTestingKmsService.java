/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;

import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * <p>A service interface for {@link InMemoryKms} useful for unit testing.
 * An instance of this class encapsulates the set of keys and aliases which will be shared between
 * all {@link Kms} instances created from the same service instance.
 * A different instance of this class will have an independent set of keys and aliases.</p>
 *
 * <p>You can obtain an instance via {@link ServiceLoader} or just use the factory method
 * {@link #newInstance()}.</p>
 *
 *
 * @see IntegrationTestingKmsService
 */
@Plugin(configType = UnitTestingKmsService.Config.class)
public class UnitTestingKmsService implements KmsService<UnitTestingKmsService.Config, UUID, InMemoryEdek> {

    public static UnitTestingKmsService newInstance() {
        return (UnitTestingKmsService) ServiceLoader.load(KmsService.class).stream()
                .filter(p -> p.type() == UnitTestingKmsService.class)
                .findFirst()
                .map(ServiceLoader.Provider::get)
                .orElse(null);
    }

    public record Config(
                         int numIvBytes,
                         int numAuthBits) {
        public Config {
            if (numIvBytes < 1) {
                throw new IllegalArgumentException();
            }
            if (numAuthBits < 1) {
                throw new IllegalArgumentException();
            }
        }

        public Config() {
            this(12, 128);
        }

    }

    private final AtomicInteger numGeneratedDeks = new AtomicInteger();

    private final Map<UUID, SecretKey> keys = new ConcurrentHashMap<>();
    private final Map<String, UUID> aliases = new ConcurrentHashMap<>();

    @NonNull
    @Override
    public InMemoryKms buildKms(Config options) {
        return new InMemoryKms(options.numIvBytes(),
                options.numAuthBits(),
                keys,
                aliases,
                numGeneratedDeks);
    }

}
