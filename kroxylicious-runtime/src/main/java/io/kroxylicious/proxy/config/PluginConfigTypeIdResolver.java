/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;

import io.kroxylicious.proxy.plugin.VersionMismatchException;

import edu.umd.cs.findbugs.annotations.Nullable;

public class PluginConfigTypeIdResolver extends TypeIdResolverBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(PluginConfigTypeIdResolver.class);
    private static final boolean REQUIRE_PLUGIN_VERSIONS = determineVersionRequirement();

    private final PluginFactory<?> providers;
    private @Nullable JavaType superType;

    PluginConfigTypeIdResolver(PluginFactory<?> providers) {
        this.providers = providers;
    }

    @Override
    public void init(JavaType baseType) {
        superType = baseType;
    }

    @Override
    public String idFromValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String idFromValueAndType(Object value, Class<?> suggestedType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonTypeInfo.Id getMechanism() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) {
        PluginTypeReference ref = PluginTypeReference.parse(id);

        // When version is present, try composite key first (enables disambiguation)
        String lookupKey = ref.hasVersion() ? id : ref.pluginName();

        // Resolve config type (this validates plugin exists)
        Class<?> configType = providers.configType(lookupKey);

        // Validate version compatibility
        String pluginVersion = providers.pluginVersion(lookupKey);
        validateVersion(ref, pluginVersion);

        return context.constructSpecializedType(Objects.requireNonNull(superType), configType);
    }

    private static boolean determineVersionRequirement() {
        // Environment variable override (for testing/migration)
        String envVar = System.getenv("KROXYLICIOUS_REQUIRE_PLUGIN_VERSIONS");
        if (envVar != null) {
            return Boolean.parseBoolean(envVar);
        }

        // Check runtime version
        Package pkg = PluginConfigTypeIdResolver.class.getPackage();
        String version = pkg != null ? pkg.getImplementationVersion() : null;
        if (version != null && !version.contains("SNAPSHOT")) {
            // Require versions for 1.0.0+, optional for 0.x
            return version.compareTo("1.0.0") >= 0;
        }

        return false; // Default: optional for development builds
    }

    private void validateVersion(PluginTypeReference ref, @Nullable String pluginVersion) {
        boolean configHasVersion = ref.hasVersion();
        boolean pluginHasVersion = pluginVersion != null;

        if (configHasVersion && pluginHasVersion) {
            // Both specified - must match exactly
            if (!ref.version().equals(pluginVersion)) {
                throw new VersionMismatchException(
                        "Version mismatch for plugin '" + ref.pluginName() + "': " +
                                "config specifies '" + ref.version() + "' but " +
                                "plugin implements '" + pluginVersion + "'");
            }
        }
        else if (configHasVersion && !pluginHasVersion) {
            // Config has version but plugin doesn't
            throw new VersionMismatchException(
                    "Plugin '" + ref.pluginName() + "' does not declare a version " +
                            "but config references '" + ref.version() + "'");
        }
        else if (!configHasVersion && pluginHasVersion) {
            // Plugin has version but config doesn't - warn or error based on policy
            handleMissingVersionInConfig(ref.pluginName(), pluginVersion);
        }
        // Neither has version - allowed for backward compatibility
    }

    private void handleMissingVersionInConfig(String pluginName, String pluginVersion) {
        String message = "Plugin '" + pluginName + "' declares version '" + pluginVersion +
                "' but config does not specify it. " +
                "Update config to: type: " + pluginName + "/" + pluginVersion;

        if (REQUIRE_PLUGIN_VERSIONS) {
            throw new VersionMismatchException(message);
        }
        else {
            LOGGER.atWarn()
                    .addKeyValue("plugin", pluginName)
                    .addKeyValue("pluginVersion", pluginVersion)
                    .log(message + " (required in 1.0.0+)");
        }
    }
}
