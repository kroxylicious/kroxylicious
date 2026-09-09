/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Optional;

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.TaggedFields;
import org.apache.kafka.common.protocol.types.Type;

/**
 * Walks Kafka's authoritative runtime protocol schema for a message and drives the configured
 * {@link FieldPopulationStrategy} over each field, invoking the corresponding generated setter on
 * whichever {@code *Data} instance ({@code io.kroxylicious.*} or {@code org.apache.kafka.*}) was handed in.
 * <p>
 * Kafka's schema, not Kroxylicious's, is authoritative here: the fidelity being proven is that
 * Kroxylicious's generated classes match Kafka's wire behaviour, so population must be driven by
 * Kafka's own understanding of each field's type.
 */
public final class SchemaDrivenMessagePopulator implements MessagePopulator {

    private final FieldPopulationStrategy strategy;

    /**
     * Construct the populator with the provided strategy
     * @param strategy decides the value, if any, for each field visited
     */
    public SchemaDrivenMessagePopulator(FieldPopulationStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public PopulationResult populate(Object instance, short version) {
        Class<?> kafkaClass = kafkaClassFor(instance);
        Schema schema = kafkaSchemaFor(kafkaClass, version);
        for (BoundField field : schema.fields()) {
            FieldDecision decision;
            try {
                decision = strategy.resolve(field);
            }
            catch (UnsupportedOperationException e) {
                throw withStructContext(e, kafkaClass, field.def.type);
            }
            if (decision instanceof FieldDecision.Value(Object value1)) {
                invokeSetter(instance, field, value1);
                continue;
            }
            if (field.def.type instanceof TaggedFields taggedFields && taggedFields.numFields() == 0) {
                // An empty tagged-fields section has nothing to populate.
                continue;
            }
            throw new UnsupportedOperationException(
                    "Composite/array field walking is not yet supported: " + field.def.name);
        }
        return new PopulationResult.Populated();
    }

    private static Class<?> kafkaClassFor(Object instance) {
        String kafkaClassName = "org.apache.kafka.common.message." + instance.getClass().getSimpleName();
        try {
            return Class.forName(kafkaClassName);
        }
        catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not resolve Kafka class for " + kafkaClassName, e);
        }
    }

    private static Schema kafkaSchemaFor(Class<?> kafkaClass, short version) {
        try {
            Schema[] schemas = (Schema[]) kafkaClass.getField("SCHEMAS").get(null);
            return schemas[version];
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Could not resolve Kafka schema for " + kafkaClass.getName(), e);
        }
    }

    /**
     * The struct's own {@link Schema} carries no name; the generated class holding it as a
     * {@code SCHEMA_N} constant does, so recover it by identity from the message's nested classes.
     */
    private static UnsupportedOperationException withStructContext(UnsupportedOperationException original, Class<?> kafkaClass, Type fieldType) {
        return structTypeOf(fieldType)
                .flatMap(structSchema -> resolveStructClassName(kafkaClass, structSchema))
                .map(className -> new UnsupportedOperationException(original.getMessage() + " (struct type: " + className + ")", original))
                .orElse(original);
    }

    private static Optional<Schema> structTypeOf(Type type) {
        if (type instanceof Schema schema) {
            return Optional.of(schema);
        }
        return type.arrayElementType().filter(Schema.class::isInstance).map(Schema.class::cast);
    }

    private static Optional<String> resolveStructClassName(Class<?> containingClass, Schema target) {
        for (Class<?> nested : containingClass.getDeclaredClasses()) {
            for (Field candidate : nested.getDeclaredFields()) {
                if (Schema.class.equals(candidate.getType()) && Modifier.isStatic(candidate.getModifiers())) {
                    try {
                        if (candidate.get(null).equals(target)) {
                            return Optional.of(nested.getSimpleName());
                        }
                    }
                    catch (IllegalAccessException e) {
                        // Not this field; keep searching.
                    }
                }
            }
            Optional<String> nestedMatch = resolveStructClassName(nested, target);
            if (nestedMatch.isPresent()) {
                return nestedMatch;
            }
        }
        return Optional.empty();
    }

    private static void invokeSetter(Object instance, BoundField field, Object value) {
        String setterName = "set" + toCamelCase(field.def.name);
        Method setter = findSetter(instance.getClass(), setterName);
        try {
            setter.invoke(instance, convertToParameterType(value, setter.getParameterTypes()[0]));
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not invoke " + setterName + " on " + instance.getClass(), e);
        }
    }

    private static Method findSetter(Class<?> instanceClass, String setterName) {
        for (Method method : instanceClass.getMethods()) {
            if (method.getName().equals(setterName) && method.getParameterCount() == 1) {
                return method;
            }
        }
        throw new IllegalStateException("No setter named " + setterName + " on " + instanceClass);
    }

    private static Object convertToParameterType(Object value, Class<?> parameterType) {
        if (value instanceof byte[] bytes && parameterType == ByteBuffer.class) {
            return ByteBuffer.wrap(bytes);
        }
        return value;
    }

    private static String toCamelCase(String snakeCaseName) {
        StringBuilder camelCaseName = new StringBuilder();
        boolean upperCaseNextChar = true;
        for (char c : snakeCaseName.toCharArray()) {
            if (c == '_') {
                upperCaseNextChar = true;
            }
            else if (upperCaseNextChar) {
                camelCaseName.append(Character.toUpperCase(c));
                upperCaseNextChar = false;
            }
            else {
                camelCaseName.append(c);
            }
        }
        return camelCaseName.toString();
    }
}
