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
import java.util.ArrayList;
import java.util.List;
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

    /**
     * Number of elements to populate for an array-of-struct field. One element is enough to prove the
     * struct round-trips correctly through the wire format; element-count variety is already exercised
     * for scalar arrays and is orthogonal to this feature.
     */
    private static final int STRUCT_ARRAY_LENGTH = 1;

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
        populateStruct(instance, kafkaClass, new StructResolutionContext(kafkaClass, instance.getClass(), version));
        return new PopulationResult.Populated();
    }

    /**
     * Kafka's code generator declares every struct in a message, however deeply the JSON nests them, as a
     * flat, static nested class directly on the top-level message class (confirmed against real generated
     * code) rather than truly nesting them to match the JSON shape. So resolving a struct's class by name
     * or by identity must always search from the top-level message class, not from the struct currently
     * being populated - {@code root} carries that fixed search root through the recursion.
     */
    private record StructResolutionContext(Class<?> rootKafkaClass, Class<?> rootInstanceClass, short version) {}

    private void populateStruct(Object instance, Class<?> kafkaClass, StructResolutionContext context) {
        Schema schema = kafkaSchemaFor(kafkaClass, context.version());
        for (BoundField field : schema.fields()) {
            FieldDecision decision;
            try {
                decision = strategy.resolve(field);
            }
            catch (RuntimeException e) {
                throw new UnsupportedOperationException("Could not populate " + describeField(field, context.rootKafkaClass()), e);
            }
            if (decision instanceof FieldDecision.Value(Object value1)) {
                invokeSetter(instance, field, value1);
                continue;
            }
            if (field.def.type instanceof TaggedFields taggedFields && taggedFields.numFields() == 0) {
                // An empty tagged-fields section has nothing to populate.
                continue;
            }
            Optional<Object> composed = composeStructValue(field, context);
            if (composed.isPresent()) {
                invokeSetter(instance, field, composed.get());
                continue;
            }
            throw new UnsupportedOperationException(
                    "Composite/array field walking is not yet supported for " + describeField(field, context.rootKafkaClass()));
        }
    }

    /**
     * Composes the value for a struct-typed (or array-of-struct-typed) field by recursively populating
     * one or more freshly constructed nested instances.
     */
    private Optional<Object> composeStructValue(BoundField field, StructResolutionContext context) {
        Type leafType = field.def.type.arrayElementType().orElse(field.def.type);
        if (!(leafType instanceof Schema structSchema)) {
            return Optional.empty();
        }
        boolean isArray = field.def.type.arrayElementType().isPresent();
        return resolveStructClass(context.rootKafkaClass(), structSchema)
                .flatMap(kafkaStructClass -> resolveNestedClassByName(context.rootInstanceClass(), kafkaStructClass.getSimpleName())
                        .map(instanceStructClass -> isArray
                                ? populateStructList(instanceStructClass, kafkaStructClass, context)
                                : populateStructInstance(instanceStructClass, kafkaStructClass, context)));
    }

    private Object populateStructInstance(Class<?> instanceStructClass, Class<?> kafkaStructClass, StructResolutionContext context) {
        Object structInstance = instantiate(instanceStructClass);
        populateStruct(structInstance, kafkaStructClass, context);
        return structInstance;
    }

    private List<Object> populateStructList(Class<?> instanceStructClass, Class<?> kafkaStructClass, StructResolutionContext context) {
        List<Object> structInstances = new ArrayList<>(STRUCT_ARRAY_LENGTH);
        for (int i = 0; i < STRUCT_ARRAY_LENGTH; i++) {
            structInstances.add(populateStructInstance(instanceStructClass, kafkaStructClass, context));
        }
        return structInstances;
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
    private static String describeField(BoundField field, Class<?> kafkaClass) {
        StringBuilder description = new StringBuilder("field '").append(field.def.name).append("' of type ").append(field.def.type);
        structTypeOf(field.def.type)
                .flatMap(structSchema -> resolveStructClass(kafkaClass, structSchema))
                .ifPresent(structClass -> description.append(" (struct type: ").append(structClass.getSimpleName()).append(")"));
        return description.toString();
    }

    private static Optional<Schema> structTypeOf(Type type) {
        if (type instanceof Schema schema) {
            return Optional.of(schema);
        }
        return type.arrayElementType().filter(Schema.class::isInstance).map(Schema.class::cast);
    }

    private static Optional<Class<?>> resolveStructClass(Class<?> containingClass, Schema target) {
        for (Class<?> nested : containingClass.getDeclaredClasses()) {
            for (Field candidate : nested.getDeclaredFields()) {
                if (Schema.class.equals(candidate.getType()) && Modifier.isStatic(candidate.getModifiers())) {
                    try {
                        if (sameStruct((Schema) candidate.get(null), target)) {
                            return Optional.of(nested);
                        }
                    }
                    catch (IllegalAccessException e) {
                        // Not this field; keep searching.
                    }
                }
            }
            Optional<Class<?>> nestedMatch = resolveStructClass(nested, target);
            if (nestedMatch.isPresent()) {
                return nestedMatch;
            }
        }
        return Optional.empty();
    }

    /**
     * A nullable struct field's type is a {@link org.apache.kafka.common.protocol.types.NullableSchema}
     * wrapping the struct's schema - it copies the struct's {@link org.apache.kafka.common.protocol.types.Field}
     * defs into a brand new {@link Schema} instance rather than reusing the struct class's own {@code SCHEMA_N}
     * object, so a plain identity/equality check on the {@link Schema} itself misses this case. The copied
     * defs are the same {@code Field} objects, though, so comparing fields by identity sees through the wrapper.
     */
    private static boolean sameStruct(Schema candidate, Schema target) {
        if (candidate.equals(target)) {
            return true;
        }
        BoundField[] candidateFields = candidate.fields();
        BoundField[] targetFields = target.fields();
        if (candidateFields.length != targetFields.length) {
            return false;
        }
        for (int i = 0; i < candidateFields.length; i++) {
            if (!candidateFields[i].def.equals(targetFields[i].def)) {
                return false;
            }
        }
        return true;
    }

    /**
     * The Kafka-side struct class found by {@link #resolveStructClass} identifies the field's type by
     * object identity, but the value must be an instance of the analogous class in {@code instance}'s
     * own class family (Kroxylicious or Kafka). That class shares the Kafka-side class's simple name but
     * is otherwise unrelated, so it can only be found by name, not by identity.
     */
    private static Optional<Class<?>> resolveNestedClassByName(Class<?> containingClass, String simpleName) {
        for (Class<?> nested : containingClass.getDeclaredClasses()) {
            if (nested.getSimpleName().equals(simpleName)) {
                return Optional.of(nested);
            }
            Optional<Class<?>> nestedMatch = resolveNestedClassByName(nested, simpleName);
            if (nestedMatch.isPresent()) {
                return nestedMatch;
            }
        }
        return Optional.empty();
    }

    private static Object instantiate(Class<?> clazz) {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not instantiate " + clazz, e);
        }
    }

    private static void invokeSetter(Object instance, BoundField field, Object value) {
        Method setter = findSetter(instance.getClass(), field.def.name);
        try {
            setter.invoke(instance, convertToParameterType(value, setter.getParameterTypes()[0]));
        }
        catch (Exception e) {
            throw new IllegalStateException("Could not invoke setter (" + setter.getName() + ") for field '" + field.def.name + "' on " + instance.getClass(), e);
        }
    }

    /**
     * Finds the generated setter for {@code fieldName} by converting each candidate setter's name back to
     * the schema's snake_case convention and comparing, rather than guessing the setter name forward from
     * {@code fieldName}. Forward guessing is lossy: Kafka's generator derives the wire field name from a
     * PascalCase Java name by lower-casing runs of leading capitals without inserting separators (e.g. both
     * {@code KRaftVersionFeature} and {@code KraftVersionFeature} would yield {@code kraft_version_feature}),
     * so a snake_case name alone cannot always be turned back into the correct Java name.
     */
    private static Method findSetter(Class<?> instanceClass, String fieldName) {
        for (Method method : instanceClass.getMethods()) {
            if (method.getParameterCount() == 1 && method.getName().startsWith("set")
                    && toSnakeCase(method.getName().substring(3)).equals(fieldName)) {
                return method;
            }
        }
        throw new IllegalStateException("No setter for field '" + fieldName + "' on " + instanceClass);
    }

    private static Object convertToParameterType(Object value, Class<?> parameterType) {
        if (value instanceof byte[] bytes && parameterType == ByteBuffer.class) {
            return ByteBuffer.wrap(bytes);
        }
        return value;
    }

    private static String toSnakeCase(String pascalCaseName) {
        StringBuilder snakeCaseName = new StringBuilder();
        boolean previousWasUpperCase = true;
        for (char c : pascalCaseName.toCharArray()) {
            if (Character.isUpperCase(c)) {
                if (!previousWasUpperCase) {
                    snakeCaseName.append('_');
                }
                snakeCaseName.append(Character.toLowerCase(c));
                previousWasUpperCase = true;
            }
            else {
                snakeCaseName.append(c);
                previousWasUpperCase = false;
            }
        }
        return snakeCaseName.toString();
    }
}
