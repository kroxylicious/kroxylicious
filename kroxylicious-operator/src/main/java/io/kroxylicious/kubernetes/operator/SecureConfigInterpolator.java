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
import java.util.stream.Collectors;

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

    private static final Interpolated NULL_INTERPOLATED = new Interpolated(null, List.of());

    private final Map<String, SecureConfigProvider> providers;
    private final Path mountPathBase;

    public SecureConfigInterpolator(String mountPathBase, Map<String, SecureConfigProvider> providers) {
        this.providers = providers;
        this.mountPathBase = Path.of(mountPathBase);
    }

    InterpolationResult interpolate(Object configTemplate) {
        // use sets so that it doesn't matter is two providers require the same volume or mount (with exactly the same definition)

        var interpolated = interpolateRecursive(configTemplate);
        var volumes = interpolated.containerFileReferences().stream()
                .map(ContainerFileReference::volume)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        var mounts = interpolated.containerFileReferences().stream()
                .map(ContainerFileReference::mount)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return new InterpolationResult(interpolated.interpolatedObject(),
                volumes, mounts);
    }

    private record Interpolated(@Nullable Object interpolatedObject, @NonNull List<ContainerFileReference> containerFileReferences) {

    }

    private @NonNull Interpolated interpolateRecursive(@Nullable final Object jsonValue) {
        if (jsonValue == null) {
            return NULL_INTERPOLATED;
        }
        else if (jsonValue instanceof Map<?, ?> object) {
            return interpolateObject(object);
        }
        else if (jsonValue instanceof List<?> array) {
            return interpolateArray(array);
        }
        else if (jsonValue instanceof String text) {
            return maybeInterpolateString(text);
        }
        else if (jsonValue instanceof Number) {
            return new Interpolated(jsonValue, List.of());
        }
        else if (jsonValue instanceof Boolean) {
            return new Interpolated(jsonValue, List.of());
        }
        else {
            throw new IllegalStateException(jsonValue + " is not a valid JSON object");
        }
    }

    private @NonNull Interpolated interpolateArray(List<?> array) {
        var values = new ArrayList<>(array.size());
        var containerFiles = new ArrayList<ContainerFileReference>(array.size());
        for (var value : array) {
            Interpolated interpolated = interpolateRecursive(value);
            values.add(interpolated.interpolatedObject());
            containerFiles.addAll(interpolated.containerFileReferences());
        }
        return new Interpolated(values, containerFiles);
    }

    private @NonNull Interpolated interpolateObject(Map<?, ?> object) {
        var newObject = new LinkedHashMap<>(1 + (int) (object.size() / 0.75f));
        List<ContainerFileReference> containerFileReferences = new ArrayList<>();
        for (var entry : object.entrySet()) {
            String fieldName = entry.getKey().toString();
            Interpolated v = interpolateRecursive(entry.getValue());
            containerFileReferences.addAll(v.containerFileReferences());
            newObject.put(fieldName, v.interpolatedObject());
        }
        return new Interpolated(newObject, containerFileReferences);
    }

    @NonNull
    private Interpolated maybeInterpolateString(String text) {
        Matcher matcher = PATTERN.matcher(text);
        String replacement;
        ArrayList<ContainerFileReference> containerFileReferences = new ArrayList<>();
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
                Path containerPath = containerFile.containerPath();
                containerFileReferences.add(containerFile);
                replacement = containerPath.toString();
            }
        }
        else {
            replacement = text;
        }
        return new Interpolated(replacement, containerFileReferences);
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
