/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.micrometer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;

import io.kroxylicious.proxy.classloader.ClassloaderUtils;

public class MicrometerConfiguration {

    private static final MicrometerConfiguration EMPTY = new MicrometerConfiguration(null, null, null);
    private static final List<String> BINDER_PACKAGES = getBinderPackages();

    private final List<Class<? extends MeterBinder>> binders;
    private final Optional<Class<? extends MicrometerConfigurationHook>> configurationHook;
    private final List<Tag> commonTags;

    private static List<String> getBinderPackages() {
        String binderPackage = "io.micrometer.core.instrument.binder";
        return List.of(binderPackage + ".jvm", binderPackage + ".system");
    }

    public MicrometerConfiguration(List<String> binders, String configurationHookClass, Map<String, String> commonTags) {
        this.binders = configureBinders(binders);
        this.configurationHook = loadConfigurationHookClass(configurationHookClass);
        this.commonTags = toTags(commonTags);
    }

    public static MicrometerConfiguration empty() {
        return EMPTY;
    }

    private List<Tag> toTags(Map<String, String> commonTags) {
        if (commonTags == null) {
            return List.of();
        }
        return commonTags.entrySet().stream().map(entry -> Tag.of(entry.getKey(), entry.getValue())).collect(Collectors.toList());
    }

    private Optional<Class<? extends MicrometerConfigurationHook>> loadConfigurationHookClass(String configurationHookClass) {
        if (configurationHookClass == null) {
            return Optional.empty();
        }
        Class<? extends MicrometerConfigurationHook> clazz = ClassloaderUtils.loadClassIfPresent(MicrometerConfigurationHook.class, configurationHookClass)
                .orElseThrow(() -> new IllegalArgumentException("could not load micrometer configuration hook: " + configurationHookClass));
        return Optional.ofNullable(clazz);
    }

    private List<Class<? extends MeterBinder>> configureBinders(List<String> binders) {
        if (binders == null) {
            return List.of();
        }
        return binders.stream().map(this::toBinderClass).collect(Collectors.toList());
    }

    private Class<? extends MeterBinder> toBinderClass(String binder) {
        Stream<String> literalCandidate = Stream.of(binder);
        Stream<String> packageClassCandidates = BINDER_PACKAGES.stream().map(binderPackage -> binderPackage + "." + binder);
        return Stream.concat(literalCandidate, packageClassCandidates)
                .map(candidateClassName -> ClassloaderUtils.loadClassIfPresent(MeterBinder.class, candidateClassName))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst().orElseThrow(() -> new IllegalArgumentException("could not locate binder class for " + binder));
    }

    public List<Class<? extends MeterBinder>> getBinders() {
        return binders;
    }

    public Optional<Class<? extends MicrometerConfigurationHook>> loadConfigurationHookClass() {
        return configurationHook;
    }

    public List<Tag> getCommonTags() {
        return commonTags;
    }
}
