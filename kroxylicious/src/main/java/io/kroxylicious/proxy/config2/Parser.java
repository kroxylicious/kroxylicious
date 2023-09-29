/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.ConstructorDetector;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import io.kroxylicious.proxy.config2.api.FilterPlugin;
import io.kroxylicious.proxy.config2.api.MicrometerPlugin;
import io.kroxylicious.proxy.config2.api.Plugin;
import io.kroxylicious.proxy.config2.api.UnresolvedConfig;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.service.HostPort;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Parser {
    private final static ObjectMapper MAPPER = createObjectMapper();

    public static ObjectMapper createObjectMapper() {
        return new ObjectMapper(new YAMLFactory())
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new SimpleModule().addSerializer(HostPort.class, new ToStringSerializer()))
                .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY)
                .setConstructorDetector(ConstructorDetector.USE_PROPERTIES_BASED)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
                .configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true)
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    }

    private static UnresolvedConfig parseUnresolved(String stream) {
        try {
            return MAPPER.readValue(stream, UnresolvedConfig.class);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ResolvedConfig resolveConfig(UnresolvedConfig uc) {
//        var loaders = serviceLoaders(uc.filters());
//        // group plugins by service class
//        // load service class
//        // for each filter plugin create a filter factory
//        var boundFilters = uc.filters().stream().map(fd -> {
//            var serviceLoader = loaders.get(fd.serviceClass());
//            for (FilterFactory filterFactory : serviceLoader) {
//                // TODO verify shortnames are not ambiguous
//                // TODO verify longnames are not ambiguous
//                if (filterFactory.filterType().getName().equals(fd.type())) { // TODO support short names
//                    // TODO validate types dynamically
//                    var config = MAPPER.convertValue(fd.config(), filterFactory.configType());
//                    filterFactory.validateConfiguration(config);
//                    return new BoundFilter(filterFactory, config);
//                }
//            }
//            throw new RuntimeException();
//        }).toList();
        List<BoundFilter<?,?>> boundFilters = resolvePlugins(uc.filters(),
                FilterPlugin::type,
                FilterPlugin::config,
                FilterFactory::filterType,
                FilterFactory::configType,
                (filterFactory, config) -> (BoundFilter<?, ?>) new BoundFilter(filterFactory, config));

        List<BoundMicrometer> boundMicrometers = resolvePlugins(Optional.ofNullable(uc.micrometerPlugins()).orElse(List.of()),
                MicrometerPlugin::type,
                MicrometerPlugin::config,
                MicrometerPlugin.MicrometerFactory::micrometerType,
                MicrometerPlugin.MicrometerFactory::configType,
                BoundMicrometer::new);

        return new ResolvedConfig(boundFilters, uc.useIoUring());
    }

    record BoundMicrometer(MicrometerPlugin.MicrometerFactory filterFactory, Object config) { }

    private static <P extends Plugin<F>, F, B> List<B> resolvePlugins(
            List<P> plugins,
            Function<P, String> typeNameFn,
            Function<P, JsonNode> confS,
            Function<F, Class<?>> tyneFn,
            Function<F, Class<?>> confFn,
            BiFunction<F, Object, B> ctor) {
        var servicesByServiceClass = serviceLoaders(plugins);
        var bound = plugins.stream()
                .map(fd -> resolvePlugin(fd, typeNameFn, confS, tyneFn, confFn, ctor, servicesByServiceClass))
                .toList();
        return bound;
    }

    private static <P extends Plugin<F>, F, B> B resolvePlugin(P plugin,
                                                               Function<P, String> typeNameFn,
                                                               Function<P, JsonNode> confS,
                                                               Function<F, Class<?>> tyneFn,
                                                               Function<F, Class<?>> confFn,
                                                               BiFunction<F, Object, B> ctor,
                                                               Map<Class<F>, ServiceLoader<F>> servicesByServiceClass) {
        var serviceLoader = servicesByServiceClass.get(plugin.serviceClass());
        for (F factory : serviceLoader) {
            // TODO verify shortnames are not ambiguous
            // TODO verify longnames are not ambiguous
            if (tyneFn.apply(factory).getName().equals(typeNameFn.apply(plugin))) { // TODO support short names
                // TODO validate types dynamically
                var config = MAPPER.convertValue(confS.apply(plugin), confFn.apply(factory));
                //filterFactory.validateConfiguration(config);
                return ctor.apply(factory, config);
            }
        }
        throw new RuntimeException();
    }

    private static <S, P extends Plugin<S>> Map<Class<S>, ServiceLoader<S>> serviceLoaders(List<P> filters) {
        return filters.stream()
                .map(Plugin::serviceClass)
                .distinct()
                .collect(Collectors.toMap(sc -> sc,
                        ServiceLoader::load
                ));
    }

    public static ResolvedConfig parse(String content) {
        return resolveConfig(parseUnresolved(content));
    }

    // TODO support converting an unresolved tree back to YAML?

    public static void main(String[] a) {
        var resolvedConfig = Parser.parse("""
                useIoUring: true
                filters:
                - type: io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter
                  config:
                    transformation: io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter$UpperCasing
                """);

        System.out.println(resolvedConfig);
        // TODO build filter chain factory from the resolvedConfig.filters (change its constructor??)
    }

}
