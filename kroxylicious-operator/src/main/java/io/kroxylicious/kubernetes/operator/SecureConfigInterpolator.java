/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Interpolates a KafkaProxyFilter's configTemplate using a set of {@link SecureConfigProvider}s, producing a complete
 * configuration for a filter, plus any necessary volumes and volumeMounts for files referred to from that configuration.
 */
public class SecureConfigInterpolator {

    private static final Pattern PATTERN = Pattern.compile("^\\$\\{"
            + "(?<providerName>[a-z]+)"
            + ":(?<path>[a-zA-Z0-9_.-]+)"
            + ":(?<key>[a-zA-Z0-9_.-]+)"
            + "}$");

    private final Map<String, SecureConfigProvider> providers;
    private final Path mountPathBase;

    public SecureConfigInterpolator(String mountPathBase, Map<String, SecureConfigProvider> providers) {
        this.providers = providers;
        this.mountPathBase = Path.of(mountPathBase);
    }

    InterpolationResult interpolate(Object configTemplate) {
        // use sets so that it doesn't matter is two providers require the same volume or mount (with exactly the same definition)
        var volumes = new LinkedHashSet<Volume>();
        var mounts = new LinkedHashSet<VolumeMount>();

        var interpolated = interpolateRecursive(configTemplate, volumes, mounts);

        return new InterpolationResult(interpolated,
                volumes, mounts);
    }

    private @Nullable Object interpolateRecursive(
                                                  @Nullable final Object jsonValue,
                                                  @NonNull Set<Volume> volumes,
                                                  @NonNull Set<VolumeMount> mounts) {
        if (jsonValue == null) {
            return jsonValue;
        }
        else if (jsonValue instanceof Map<?, ?> object) {
            var newObject = new LinkedHashMap<>(1 + (int) (object.size() / 0.75f));
            for (var entry : object.entrySet()) {
                String fieldName = entry.getKey().toString();
                Object v = interpolateRecursive(entry.getValue(), volumes, mounts);
                newObject.put(fieldName, v);
            }
            return newObject;
        }
        else if (jsonValue instanceof List<?> array) {
            var newArray = new ArrayList<>(array.size());
            for (var value : array) {
                newArray.add(interpolateRecursive(value, volumes, mounts));
            }
            return newArray;
        }
        else if (jsonValue instanceof String text) {
            return maybeInterpolateString(volumes, mounts, text);
        }
        else if (jsonValue instanceof Number) {
            return jsonValue;
        }
        else if (jsonValue instanceof Boolean) {
            return jsonValue;
        }
        else {
            throw new IllegalStateException(jsonValue + " is not a valid JSON object");
        }
    }

    @NonNull
    private String maybeInterpolateString(@NonNull Set<Volume> volumes, @NonNull Set<VolumeMount> mounts, String text) {
        Matcher matcher = PATTERN.matcher(text);
        String replacement;
        if (matcher.matches()) {
            String providerName = matcher.group("providerName");
            String path = matcher.group("path");
            String key = matcher.group("key");
            var provider = providers.get(providerName);
            if (provider == null) {
                replacement = text;
            }
            else {
                var containerFile = provider.containerFile(providerName, path, key, mountPathBase);
                Volume volume = containerFile.volume();
                VolumeMount mount = containerFile.mount();
                Path containerPath = containerFile.containerPath();
                if (volume != null) {
                    volumes.add(volume);
                }
                if (mount != null) {
                    mounts.add(mount);
                }
                replacement = containerPath.toString();
            }
        }
        else {
            replacement = text;
        }
        return replacement;
    }

    record InterpolationResult(@Nullable Object config,
                               @NonNull Set<Volume> volumes,
                               @NonNull Set<VolumeMount> mounts) {

        InterpolationResult {
            Objects.requireNonNull(volumes);
            Objects.requireNonNull(mounts);
        }
    }

}
