/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.proxy.classloader.ClassloaderUtils;
import io.kroxylicious.proxy.config.ProxyConfig;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;

public class BuiltinFilterContributor implements FilterContributor {

    @Override
    public Class<? extends FilterConfig> getConfigType(String name) {
        Optional<Class<? extends KrpcFilter>> clazz = findFilterClass(name);
        return clazz.map(this::lookupConfigFromClasspath).orElse(null);
    }

    @Override
    public KrpcFilter getFilter(String name, ProxyConfig proxyConfig, FilterConfig filterConfig) {
        Optional<Class<? extends KrpcFilter>> clazz = findFilterClass(name);
        return clazz.map(aClass -> maybeLoadFilterReflectively(proxyConfig, filterConfig, aClass)).orElse(null);
    }

    private Optional<Class<? extends KrpcFilter>> findFilterClass(String name) {
        return builtinClass(name).or(() -> ClassloaderUtils.loadClassIfPresent(KrpcFilter.class, name));
    }

    public Optional<Class<? extends KrpcFilter>> builtinClass(String name) {
        switch (name) {
            case "ApiVersions":
                return Optional.of(ApiVersionsFilter.class);
            case "BrokerAddress":
                return Optional.of(BrokerAddressFilter.class);
            case "ProduceRequestTransformation":
                return Optional.of(ProduceRequestTransformationFilter.class);
            case "FetchResponseTransformation":
                return Optional.of(FetchResponseTransformationFilter.class);
            default:
                return Optional.empty();
        }
    }

    private Class<? extends FilterConfig> lookupConfigFromClasspath(Class<? extends KrpcFilter> clazz) {
        return maybeGetFilterConfigClassFromConstructor(clazz);
    }

    private static Class<? extends FilterConfig> maybeGetFilterConfigClassFromConstructor(Class<? extends KrpcFilter> aClass) {
        Optional<Constructor<? extends KrpcFilter>> maybeConstructor = getFilterConstructor(aClass);
        Optional<Class<? extends FilterConfig>> aClass1 = maybeConstructor.map(targetConstructor -> {
            List<Class<?>> filterConfigParams = Arrays.stream(targetConstructor.getParameterTypes()).filter(FilterConfig.class::isAssignableFrom)
                    .collect(Collectors.toList());
            if (filterConfigParams.size() > 1) {
                throw new IllegalArgumentException("constructor " + targetConstructor + " had more than one parameter that was a FilterConfig, expecting zero or one");
            }
            if (filterConfigParams.isEmpty()) {
                // empty config
                return FilterConfig.class;
            }
            else {
                return filterConfigParams.get(0).asSubclass(FilterConfig.class);
            }
        });
        return aClass1.orElse(null);
    }

    private static Optional<Constructor<? extends KrpcFilter>> getFilterConstructor(Class<? extends KrpcFilter> aClass) {
        return getTargetConstructor(aClass).or(() -> getOnlyConstructor(aClass));
    }

    private static Optional<? extends Constructor<? extends KrpcFilter>> getOnlyConstructor(Class<? extends KrpcFilter> aClass) {
        Constructor<?>[] constructors = aClass.getConstructors();
        if (constructors.length != 1) {
            throw new IllegalArgumentException("more than one constructor found on a class with no FilterCreator annotated constructor");
        }
        else {
            try {
                return Optional.of(aClass.getConstructor(constructors[0].getParameterTypes()));
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private KrpcFilter maybeLoadFilterReflectively(ProxyConfig proxyConfig, final FilterConfig filterConfig, Class<? extends KrpcFilter> clazz) {
        Optional<Constructor<? extends KrpcFilter>> tc = getFilterConstructor(clazz);
        return tc.map(onlyConstructor -> {
            Object[] args = createArguments(proxyConfig, filterConfig, onlyConstructor);
            try {
                return onlyConstructor.newInstance(args);
            }
            catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }).orElse(null);

    }

    private static Object[] createArguments(ProxyConfig proxyConfig, FilterConfig filterConfig, Constructor<? extends KrpcFilter> onlyConstructor) {
        return Arrays.stream(onlyConstructor.getParameterTypes()).map(aClass -> {
            if (aClass == ProxyConfig.class) {
                return proxyConfig;
            }
            else if (FilterConfig.class.isAssignableFrom(aClass)) {
                return filterConfig;
            }
            else {
                throw new IllegalArgumentException("cannot handle Filter constructor parameter of type: " + aClass);
            }
        }).toArray(Object[]::new);
    }

    private static Optional<Constructor<? extends KrpcFilter>> getTargetConstructor(Class<? extends KrpcFilter> aClass) {
        Stream<Constructor<?>> constructorStream = Arrays.stream(aClass.getConstructors())
                .filter(constructor -> constructor.getAnnotation(FilterCreator.class) != null);
        List<Constructor<?>> collect = constructorStream.collect(Collectors.toList());
        if (collect.size() > 1) {
            throw new IllegalArgumentException("found >1 constructors annotated with FilterCreator on " + aClass.getName());
        }
        else if (collect.isEmpty()) {
            return Optional.empty();
        }
        else {
            try {
                Constructor<? extends KrpcFilter> constructor = aClass.getConstructor(collect.get(0).getParameterTypes());
                return Optional.of(constructor);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
