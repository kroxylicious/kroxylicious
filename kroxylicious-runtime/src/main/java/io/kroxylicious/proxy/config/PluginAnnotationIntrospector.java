/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.lang.annotation.Annotation;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.OptBoolean;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;

import io.kroxylicious.proxy.plugin.PluginImplConfig;

class PluginAnnotationIntrospector extends JacksonAnnotationIntrospector {

    @Override
    protected <A extends Annotation> A _findAnnotation(
            Annotated ann,
            Class<A> annoClass
    ) {
        if (annoClass == JsonTypeIdResolver.class) {
            var pc = _findAnnotation(ann, PluginImplConfig.class);
            if (pc != null) {
                return (A) synthesizeJsonTypeIdResolver();
            }
        } else if (annoClass == JsonTypeInfo.class) {
            var pc = _findAnnotation(ann, PluginImplConfig.class);
            if (pc != null) {
                return (A) synthesizeJsonTypeInfo(pc);
            }
        }
        return super._findAnnotation(ann, annoClass);
    }

    /**
     * Returns a fake (i.e. not obtained through reflection)
     * JsonTypeInfo instance which looks like this:
     * <pre>{@code
     * @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
     * }</pre>
     * where "type" is taken from the {@link PluginImplConfig#implNameProperty()}.
     * @param pc The plugin config annotation
     * @return The fake annotation instance
     */
    private static JsonTypeInfo synthesizeJsonTypeInfo(PluginImplConfig pc) {
        return new JsonTypeInfo() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return JsonTypeInfo.class;
            }

            @Override
            public Id use() {
                return Id.NAME;
            }

            @Override
            public As include() {
                return As.EXTERNAL_PROPERTY;
            }

            @Override
            public String property() {
                return pc.implNameProperty();
            }

            @Override
            public Class<?> defaultImpl() {
                return JsonTypeInfo.class;
            }

            @Override
            public boolean visible() {
                return false;
            }

            @Override
            public OptBoolean requireTypeIdForSubtypes() {
                return OptBoolean.DEFAULT;
            }
        };
    }

    /**
     * Returns a fake (i.e. not obtained through reflection)
     * JsonTypeIdResolver instance which looks like this:
     * <pre>{@code
     * @JsonTypeIdResolver(FilterConfigTypeIdResolver.class)
     * }</pre>
     * The {@link io.kroxylicious.proxy.config.ConfigParser}'s HandlerInstantiator will be responsible for instantiating this,
     * passing it the plugin manager to use to look up the plugin named
     * by the id in the synthetic @JsonTypeInfo returned by {@link #synthesizeJsonTypeInfo(PluginImplConfig)}
     * @return The annotation instance.
     */
    private static JsonTypeIdResolver synthesizeJsonTypeIdResolver() {
        return new JsonTypeIdResolver() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return JsonTypeIdResolver.class;
            }

            @Override
            public Class<? extends TypeIdResolver> value() {
                return PluginConfigTypeIdResolver.class;
            }
        };
    }

}
