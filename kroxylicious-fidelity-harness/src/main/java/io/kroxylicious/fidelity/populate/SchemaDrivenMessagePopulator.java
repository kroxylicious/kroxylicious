/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
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
        for (BoundField field : expandFields(schema)) {
            populateField(instance, field, context);
        }
    }

    /**
     * Kafka's generator compiles every {@code taggedVersions}/{@code tag} field in a struct's JSON spec
     * into individual {@link org.apache.kafka.common.protocol.types.Field} entries wrapped by a single
     * synthetic {@code _tagged_fields} {@link BoundField}, rather than exposing them as top-level
     * {@code BoundField}s the way {@link Schema#fields()} does - so this flattens the schema's raw field
     * list into the fields actually available to populate, splicing each wrapped tag in as its own
     * {@link BoundField} in place of the synthetic wrapper, so callers never need to know the wrapper
     * exists. Each wrapped {@code Field} shares the same shape ({@code name}/{@code type}) that
     * {@link #populateField} already relies on, so it can be adapted directly; the generated setter for a
     * tagged field is an ordinary public setter, indistinguishable in shape from a non-tagged field's
     * setter, and a tag's presence on the wire is implicit in its value being non-default, so no separate
     * registration step is needed.
     */
    private static List<BoundField> expandFields(Schema schema) {
        List<BoundField> expanded = new ArrayList<>();
        for (BoundField field : schema.fields()) {
            if (field.def.type instanceof TaggedFields taggedFields) {
                for (var entry : taggedFields.fields().entrySet()) {
                    expanded.add(new BoundField(entry.getValue(), null, entry.getKey()));
                }
            }
            else {
                expanded.add(field);
            }
        }
        return expanded;
    }

    private void populateField(Object instance, BoundField field, StructResolutionContext context) {
        FieldDecision decision;
        try {
            decision = strategy.resolve(field);
        }
        catch (RuntimeException e) {
            throw new UnsupportedOperationException("Could not populate " + describeField(field, context.rootKafkaClass()), e);
        }
        if (decision instanceof FieldDecision.Value(Object value1)) {
            invokeSetter(instance, field, value1);
            return;
        }
        Optional<Object> composed = composeStructValue(field, context);
        if (composed.isPresent()) {
            invokeSetter(instance, field, composed.get());
            return;
        }
        throw new UnsupportedOperationException(
                "Composite/array field walking is not yet supported for " + describeField(field, context.rootKafkaClass()));
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
            setter.invoke(instance, convertToParameterType(value, setter));
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

    /**
     * The strategy produces values without knowing which class family ({@code io.kroxylicious.*} or
     * {@code org.apache.kafka.*}) the setter belongs to, so a value occasionally needs converting to the
     * equivalent type from the setter's own family - e.g. a {@code Uuid} field on a Kafka-side instance
     * needs an {@code org.apache.kafka.common.Uuid}, not the strategy's {@code io.kroxylicious} one.
     */
    private static Object convertToParameterType(Object value, Method setter) {
        Class<?> parameterType = setter.getParameterTypes()[0];
        if (value instanceof byte[] bytes && parameterType == ByteBuffer.class) {
            return ByteBuffer.wrap(bytes);
        }
        if (value instanceof io.kroxylicious.kafka.common.Uuid uuid && parameterType == org.apache.kafka.common.Uuid.class) {
            return toKafkaUuid(uuid);
        }
        if (value instanceof io.kroxylicious.kafka.common.record.internal.MemoryRecords
                && org.apache.kafka.common.record.internal.BaseRecords.class.isAssignableFrom(parameterType)) {
            return org.apache.kafka.common.record.internal.MemoryRecords.EMPTY;
        }
        if (value instanceof List<?> elements) {
            List<Object> converted = convertElements(elements, elementTypeOf(setter));
            return parameterType.isAssignableFrom(value.getClass()) ? converted : newCollection(parameterType, converted);
        }
        return value;
    }

    private static org.apache.kafka.common.Uuid toKafkaUuid(io.kroxylicious.kafka.common.Uuid uuid) {
        return new org.apache.kafka.common.Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     * A scalar-array field's setter parameter type is generic (e.g. {@code List<Uuid>}), so, unlike a
     * directly-typed field, the erased parameter type alone ({@code List.class}) can't reveal which class
     * family an element should belong to - that information only survives in the setter's generic signature.
     */
    private static Optional<Class<?>> elementTypeOf(Method setter) {
        if (setter.getGenericParameterTypes()[0] instanceof ParameterizedType parameterized
                && parameterized.getActualTypeArguments()[0] instanceof Class<?> elementClass) {
            return Optional.of(elementClass);
        }
        return Optional.empty();
    }

    private static List<Object> convertElements(List<?> elements, Optional<Class<?>> elementType) {
        List<Object> converted = new ArrayList<>(elements.size());
        for (Object element : elements) {
            if (element instanceof io.kroxylicious.kafka.common.Uuid uuid && elementType.filter(org.apache.kafka.common.Uuid.class::equals).isPresent()) {
                converted.add(toKafkaUuid(uuid));
            }
            else {
                converted.add(element);
            }
        }
        return converted;
    }

    /**
     * An array-of-struct field whose generator-emitted setter declares a specialised
     * {@code ImplicitLinkedHashMultiCollection}-derived collection type (e.g. {@code BrokerCollection}) rather
     * than a plain {@code List} still exposes a no-arg constructor and {@link Collection#add}, so the composed
     * list of populated struct instances can be adapted into it without knowing the concrete type up front.
     */
    private static Collection<Object> newCollection(Class<?> collectionType, List<?> elements) {
        try {
            @SuppressWarnings("unchecked")
            Collection<Object> collection = (Collection<Object>) collectionType.getDeclaredConstructor().newInstance();
            collection.addAll(elements);
            return collection;
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not instantiate collection " + collectionType, e);
        }
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
