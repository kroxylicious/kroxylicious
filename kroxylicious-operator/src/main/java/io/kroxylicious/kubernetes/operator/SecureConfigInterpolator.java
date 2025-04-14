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

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Interpolates a KafkaProxyFilter's configTemplate using a set of {@link SecureConfigProvider}s, producing a complete
 * configuration for a filter, plus any necessary volumes and volumeMounts for files referred to from that configuration.
 */
public class SecureConfigInterpolator {

    @SuppressWarnings("java:S5852")
    // This regex is vulnerable to polynomial runtime due to backtracking
    // because it's used with `Matcher.find()`,
    // however the input is controlled only by RBAC-authorized users
    private static final Pattern PATTERN = Pattern.compile("(?<quoting>\\^*)\\$\\{"
            + "(?<providerName>[a-z]{1,63})"
            + ":(?<path>[a-zA-Z0-9_.-]{1,63})"
            + ":(?<key>[a-zA-Z0-9_.-]+)"
            + "}");

    private record InterpolatedValue(@Nullable Object interpolatedValue, List<ContainerFileReference> containerFileReferences) {

    }

    private static final InterpolatedValue NULL_INTERPOLATED_VALUE = new InterpolatedValue(null, List.of());

    static final SecureConfigInterpolator DEFAULT_INTERPOLATOR = new SecureConfigInterpolator("/opt/kroxylicious/secure", Map.<String, SecureConfigProvider> of(
            "secret", MountedResourceConfigProvider.SECRET_PROVIDER,
            "configmap", MountedResourceConfigProvider.CONFIGMAP_PROVIDER));

    private final Map<String, SecureConfigProvider> providers;
    private final Path mountPathBase;

    public SecureConfigInterpolator(String mountPathBase, Map<String, SecureConfigProvider> providers) {
        this.providers = providers;
        this.mountPathBase = Path.of(mountPathBase);
    }

    InterpolationResult interpolate(Object configTemplate) {
        // use sets so that it doesn't matter is two providers require the same volume or mount (with exactly the same definition)

        var interpolated = interpolateValue(configTemplate);
        var volumes = interpolated.containerFileReferences().stream()
                .map(ContainerFileReference::volume)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        var mounts = interpolated.containerFileReferences().stream()
                .map(ContainerFileReference::mount)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return new InterpolationResult(interpolated.interpolatedValue(),
                volumes, mounts);
    }

    /**
     * Interpolate a JSON value.
     * Note this method is indirectly recursive.
     * @param jsonValue The json value (could be any of array, object, string, etc)
     * @return The interpolated value
     */
    private InterpolatedValue interpolateValue(@Nullable final Object jsonValue) {
        if (jsonValue == null) {
            return NULL_INTERPOLATED_VALUE;
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
            return new InterpolatedValue(jsonValue, List.of());
        }
        else if (jsonValue instanceof Boolean) {
            return new InterpolatedValue(jsonValue, List.of());
        }
        else {
            throw new IllegalStateException(jsonValue + " is not a valid JSON object");
        }
    }

    private InterpolatedValue interpolateArray(List<?> array) {
        var values = new ArrayList<>(array.size());
        var containerFiles = new ArrayList<ContainerFileReference>(array.size());
        for (var value : array) {
            InterpolatedValue interpolatedValue = interpolateValue(value);
            values.add(interpolatedValue.interpolatedValue());
            containerFiles.addAll(interpolatedValue.containerFileReferences());
        }
        return new InterpolatedValue(values, containerFiles);
    }

    private InterpolatedValue interpolateObject(Map<?, ?> object) {
        var newObject = new LinkedHashMap<>(1 + (int) (object.size() / 0.75f));
        List<ContainerFileReference> containerFileReferences = new ArrayList<>();
        for (var entry : object.entrySet()) {
            String fieldName = entry.getKey().toString();
            InterpolatedValue v = interpolateValue(entry.getValue());
            containerFileReferences.addAll(v.containerFileReferences());
            newObject.put(fieldName, v.interpolatedValue());
        }
        return new InterpolatedValue(newObject, containerFileReferences);
    }

    private InterpolatedValue maybeInterpolateString(String text) {
        Matcher matcher = PATTERN.matcher(text);
        ArrayList<ContainerFileReference> containerFileReferences = new ArrayList<>();
        var sb = new StringBuilder();
        while (matcher.find()) {
            String quoting = matcher.group("quoting");
            String providerName = matcher.group("providerName");
            String path = matcher.group("path");
            String key = matcher.group("key");

            if (quoting.length() % 2 == 1) {

                String replacement = Matcher.quoteReplacement("^".repeat(quoting.length() / 2) + "${" + providerName + ":" + path + ":" + key + "}");
                matcher.appendReplacement(sb, replacement);
            }
            else {
                if (matcher.start() != 0 || matcher.end() != text.length()) {
                    throw new InterpolationException("Config provider placeholders cannot be preceded or followed by other characters");
                }
                var provider = providers.get(providerName);
                if (provider == null) {
                    throw new InterpolationException("Unknown config provider '" + providerName + "', known providers are: " + providers.keySet());
                }
                else {
                    var containerFile = provider.containerFile(providerName, path, key, mountPathBase);
                    Path containerPath = containerFile.containerPath();
                    containerFileReferences.add(containerFile);
                    String replacement = Matcher.quoteReplacement("^".repeat(quoting.length() / 2) + containerPath);
                    matcher.appendReplacement(sb, replacement);
                }
            }
        }
        matcher.appendTail(sb);
        return new InterpolatedValue(sb.toString(), containerFileReferences);
    }

    record InterpolationResult(@Nullable Object config,
                               Set<Volume> volumes,
                               Set<VolumeMount> mounts) {

        InterpolationResult {
            Objects.requireNonNull(volumes);
            Objects.requireNonNull(mounts);
        }
    }

}
