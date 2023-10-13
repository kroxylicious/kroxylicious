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
import com.fasterxml.jackson.databind.cfg.ConstructorDetector;
import com.fasterxml.jackson.databind.cfg.HandlerInstantiator;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedConstructor;
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

import io.kroxylicious.proxy.plugin.PluginConfig;
import io.kroxylicious.proxy.plugin.PluginReference;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

public class ConfigParser implements PluginFactoryRegistry {

    private static final ObjectMapper MAPPER = createObjectMapper();
    private static final ServiceBasedPluginFactoryRegistry pluginFactoryRegistry = new ServiceBasedPluginFactoryRegistry();

    @Override
    public <T> @NonNull PluginFactory<T> pluginFactory(@NonNull Class<T> pluginClass) {
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
        return (ObjectMapper) new ObjectMapper(new YAMLFactory())
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new SimpleModule().addSerializer(HostPort.class, new ToStringSerializer()))
                .setVisibility(PropertyAccessor.ALL, Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
                .setVisibility(PropertyAccessor.CREATOR, Visibility.ANY)
                .setConstructorDetector(ConstructorDetector.USE_PROPERTIES_BASED)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
                .configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true)
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT)
                .registerModule(new PluginModule())
                .setHandlerInstantiator(new PluginHandlerInstantiator());
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
        public JsonDeserializer<?> deserializerInstance(DeserializationConfig config, Annotated annotated, Class<?> deserClass) {
            return null;
        }

        @Override
        public KeyDeserializer keyDeserializerInstance(DeserializationConfig config, Annotated annotated, Class<?> keyDeserClass) {
            return null;
        }

        @Override
        public JsonSerializer<?> serializerInstance(SerializationConfig config, Annotated annotated, Class<?> serClass) {
            return null;
        }

        @Override
        public TypeResolverBuilder<?> typeResolverBuilderInstance(MapperConfig<?> config, Annotated annotated, Class<?> builderClass) {
            return null;
        }

        @Override
        public TypeIdResolver typeIdResolverInstance(MapperConfig<?> config, Annotated annotated, Class<?> resolverClass) {
            if (resolverClass == PluginConfigTypeIdResolver.class) {
                PluginReference pluginReference = null;
                if (annotated instanceof AnnotatedParameter ap) {
                    pluginReference = pluginReferenceFromParameter(annotated, ap);
                }
                else if (annotated instanceof AnnotatedField af) {
                    pluginReference = pluginReferenceFromField(annotated, af);
                }
                if (pluginReference == null) {
                    throw new PluginDiscoveryException("Couldn't find @" + PluginReference.class.getSimpleName() + " on " + annotated);
                }
                return newResolver(pluginReference.value());
            }
            return null;
        }

        private PluginReference pluginReferenceFromField(Annotated annotated, AnnotatedField af) {
            PluginReference pluginReference = null;
            var pcAnno = af.getAnnotation(PluginConfig.class);
            if (pcAnno == null) {
                // pcAnno is guaranteed non-null because PluginAnnotationIntrospector will only return the PluginConfigTypeIdResolver.class
                // for properties annotated @PluginConfig
                throw new PluginDiscoveryException(annotated + " lacked the @" + PluginConfig.class.getName() + " annotation");
            }
            var ctors = ((AnnotatedClass) af.getTypeContext()).getConstructors();
            for (var ctor : ctors) {
                pluginReference = findPluginNameAnnotation(ctor, pcAnno.instanceNameProperty());
                if (pluginReference != null) {
                    break;
                }
            }
            return pluginReference;
        }

        private PluginReference pluginReferenceFromParameter(Annotated annotated, AnnotatedParameter ap) {
            PluginReference pluginReference;
            var pcAnno = ap.getAnnotation(PluginConfig.class);
            if (pcAnno == null) {
                // pcAnno is guaranteed non-null because MyAnnotationIntrospector will only return the PluginConfigTypeIdResolver.class
                // for properties annotated @PluginConfig
                throw new PluginDiscoveryException(annotated + " lacked the @" + PluginConfig.class.getName() + " annotation");
            }
            pluginReference = findPluginReferenceAnnotation(ap.getOwner(), pcAnno.instanceNameProperty());
            return pluginReference;
        }

        private static PluginConfigTypeIdResolver newResolver(Class<?> pluginInterface) {
            var providersByName = pluginFactoryRegistry.load(pluginInterface);
            return new PluginConfigTypeIdResolver(providersByName);
        }

        private PluginReference findPluginReferenceAnnotation(AnnotatedWithParams owner, String instanceNameProperty) {
            AnnotatedElement parameterOwner = owner.getAnnotated();
            if (parameterOwner instanceof Constructor<?> ctor) {
                return findPluginReferenceAnnotation(instanceNameProperty, ctor);
            }
            else if (owner instanceof AnnotatedConstructor ac) {
                return findPluginReferenceAnnotation(instanceNameProperty, ac);
            }
            else {
                throw new IllegalStateException("Unsupported owner: " + owner);
            }
        }

        private static PluginReference findPluginReferenceAnnotation(String instanceNameProperty, AnnotatedConstructor ac) {
            for (int i = 0; i < ac.getParameterCount(); i++) {
                var oap = ac.getParameter(i);
                if (instanceNameProperty.equals(oap.getName())) {
                    PluginReference annotation = oap.getAnnotation(PluginReference.class);
                    if (annotation != null) {
                        return annotation;
                    }
                }
            }
            return null;
        }

        private static PluginReference findPluginReferenceAnnotation(String instanceNameProperty, Constructor<?> ctor) {
            Parameter[] parameters = ctor.getParameters();
            for (Parameter parameter : parameters) {
                if (instanceNameProperty.equals(parameter.getName())) {
                    PluginReference annotation = parameter.getAnnotation(PluginReference.class);
                    if (annotation != null) {
                        return annotation;
                    }
                }
            }
            return null;
        }
    }
}
