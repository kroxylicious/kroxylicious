/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Objects;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ConfigurationDefinition;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Convenience FilterContributor for Filters that require configuration.
 * @param <T> The Configuration Type
 */
public abstract class ConfigurableFilterContributor<T extends BaseConfig> implements FilterContributor {

    private final String typeName;
    private final ConfigurationDefinition configDefiniton;
    @NonNull
    private final Class<T> configClazz;

    protected ConfigurableFilterContributor(@NonNull String typeName, @NonNull Class<T> configClazz, boolean configRequired) {
        this.configClazz = configClazz;
        Objects.requireNonNull(typeName, "typeName was null");
        Objects.requireNonNull(configClazz, "configClazz was null");
        this.typeName = typeName;
        this.configDefiniton = new ConfigurationDefinition(configClazz, configRequired);
    }

    @NonNull
    @Override
    public String getTypeName() {
        return typeName;
    }

    @NonNull
    @Override
    public ConfigurationDefinition getConfigDefinition() {
        return configDefiniton;
    }

    @NonNull
    @Override
    public Filter getInstance(FilterConstructContext context) {
        try {
            T config = configClazz.cast(context.getConfig());
            return getInstance(context, config);
        }
        catch (ClassCastException e) {
            throw new IllegalStateException("failed to cast config object to type specified by contributor");
        }
    }

    /**
     * Creates an instance of the service.
     *
     * @param context   context containing service configuration which may be null if the service instance does not accept configuration.
     * @param config   the configuration object.
     * @return the service instance.
     */
    @NonNull
    protected abstract Filter getInstance(FilterConstructContext context, T config);
}
