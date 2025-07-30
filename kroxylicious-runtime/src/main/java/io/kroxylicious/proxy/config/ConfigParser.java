/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.ConstructorDetector;
import com.fasterxml.jackson.databind.cfg.HandlerInstantiator;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedParameter;
import com.fasterxml.jackson.databind.introspect.AnnotatedWithParams;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

public class ConfigParser implements PluginFactoryRegistry {

    private static final ObjectMapper MAPPER = createObjectMapper();
    private static final ServiceBasedPluginFactoryRegistry pluginFactoryRegistry = new ServiceBasedPluginFactoryRegistry();

    @Override
    public <T> PluginFactory<T> pluginFactory(Class<T> pluginClass) {
        return pluginFactoryRegistry.pluginFactory(pluginClass);
    }

    public Configuration parseConfiguration(String configuration) {
        try {
            return MAPPER.readValue(configuration, Configuration.class);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Couldn't parse configuration", e);
        }
    }

    public Configuration parseConfiguration(InputStream configuration) {
        try {
            return MAPPER.readValue(configuration, Configuration.class);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Couldn't parse configuration", e);
        }
    }

    public String toYaml(Configuration configuration) {
        try {
            return MAPPER.writeValueAsString(configuration);
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to encode configuration as YAML", e);
        }
    }

    public static ObjectMapper createObjectMapper() {
        return (ObjectMapper) createBaseObjectMapper()
                .registerModule(new PluginModule())
                .setHandlerInstantiator(new PluginHandlerInstantiator());
    }

    /**
     * A simple object mapper for parsing kroxylicious configuration, without our plugin loading extensions
     * @return object mapper
     */
    @VisibleForTesting
    public static ObjectMapper createBaseObjectMapper() {
        return new ObjectMapper(new YAMLFactory())
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new SimpleModule().addSerializer(HostPort.class, new ToStringSerializer()))
                .setVisibility(PropertyAccessor.ALL, Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
                .setVisibility(PropertyAccessor.CREATOR, Visibility.ANY)
                .setConstructorDetector(ConstructorDetector.USE_PROPERTIES_BASED)
                .enable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY)
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    }

    private static class PluginModule extends SimpleModule {
        PluginModule() {
            super("Kroxylicious");
        }

        @Override
        public void setupModule(SetupContext context) {
            context.insertAnnotationIntrospector(new PluginAnnotationIntrospector());
            super.setupModule(context);
        }
    }

    private static class PluginHandlerInstantiator extends HandlerInstantiator {
        @Override
        public @Nullable JsonDeserializer<?> deserializerInstance(DeserializationConfig config, Annotated annotated, Class<?> deserClass) {
            return null;
        }

        @Override
        public @Nullable KeyDeserializer keyDeserializerInstance(DeserializationConfig config, Annotated annotated, Class<?> keyDeserClass) {
            return null;
        }

        @Override
        public @Nullable JsonSerializer<?> serializerInstance(SerializationConfig config, Annotated annotated, Class<?> serClass) {
            return null;
        }

        @Override
        public @Nullable TypeResolverBuilder<?> typeResolverBuilderInstance(MapperConfig<?> config, Annotated annotated, Class<?> builderClass) {
            return null;
        }

        @Override
        public @Nullable TypeIdResolver typeIdResolverInstance(MapperConfig<?> config, Annotated annotated, Class<?> resolverClass) {
            if (resolverClass == PluginConfigTypeIdResolver.class) {
                PluginImplName pluginImplName = null;
                if (annotated instanceof AnnotatedParameter ap) {
                    pluginImplName = pluginReferenceFromParameter(annotated, ap);
                }
                else if (annotated instanceof AnnotatedField af) {
                    pluginImplName = pluginReferenceFromField(annotated, af);
                }
                if (pluginImplName == null) {
                    throw new PluginDiscoveryException("Couldn't find @" + PluginImplName.class.getSimpleName() + " on member referred to by @"
                            + PluginImplConfig.class.getSimpleName() + " on " + annotated);
                }
                return newResolver(pluginImplName.value());
            }
            return null;
        }

        private @Nullable PluginImplName pluginReferenceFromParameter(Annotated annotated, AnnotatedParameter ap) {
            PluginImplName pluginImplName;
            var pcAnno = ap.getAnnotation(PluginImplConfig.class);
            if (pcAnno == null) {
                // pcAnno is guaranteed non-null because PluginAnnotationIntrospector will only return the PluginConfigTypeIdResolver.class
                // for properties annotated @PluginConfig
                throw new PluginDiscoveryException(annotated + " lacked the @" + PluginImplConfig.class.getName() + " annotation");
            }
            pluginImplName = findPluginReferenceAnnotation(ap.getOwner(), pcAnno.implNameProperty());
            return pluginImplName;
        }

        private @Nullable PluginImplName pluginReferenceFromField(Annotated annotated, AnnotatedField af) {
            PluginImplName pluginImplName = null;
            var pcAnno = af.getAnnotation(PluginImplConfig.class);
            if (pcAnno == null) {
                // pcAnno is guaranteed non-null because PluginAnnotationIntrospector will only return the PluginConfigTypeIdResolver.class
                // for properties annotated @PluginConfig
                throw new PluginDiscoveryException(annotated + " lacked the @" + PluginImplConfig.class.getName() + " annotation");
            }
            var ctors = ((AnnotatedClass) af.getTypeContext()).getConstructors();
            for (var ctor : ctors) {
                pluginImplName = findPluginReferenceAnnotation(ctor, pcAnno.implNameProperty());
                if (pluginImplName != null) {
                    break;
                }
            }
            return pluginImplName;
        }

        private static PluginConfigTypeIdResolver newResolver(Class<?> pluginInterface) {
            var providersByName = pluginFactoryRegistry.pluginFactory(pluginInterface);
            return new PluginConfigTypeIdResolver(providersByName);
        }

        private @Nullable PluginImplName findPluginReferenceAnnotation(AnnotatedWithParams owner, String instanceNameProperty) {
            AnnotatedElement parameterOwner = owner.getAnnotated();
            if (parameterOwner instanceof Constructor<?> ctor) {
                return findPluginReferenceAnnotation(instanceNameProperty, ctor);
            }
            else {
                throw new IllegalStateException("Unsupported owner: " + owner);
            }
        }

        private static @Nullable PluginImplName findPluginReferenceAnnotation(String instanceNameProperty, Constructor<?> ctor) {
            Parameter[] parameters = ctor.getParameters();
            for (Parameter parameter : parameters) {
                if (instanceNameProperty.equals(parameter.getName())) {
                    PluginImplName annotation = parameter.getAnnotation(PluginImplName.class);
                    if (annotation != null) {
                        return annotation;
                    }
                }
            }
            return null;
        }
    }
}
