/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Reflectively populates a generated {@code *Data}/nested-struct instance's own fields with
 * deterministic, non-default values, so fidelity tests can exercise more than default-constructed
 * (all-zero) instances without hand-authoring a fixture per spec.
 * <p>
 * Only package-private fields are populated: the generated classes declare every real schema field with
 * default (package) access, and use {@code private} exclusively for the generator's own bookkeeping
 * (the unknown-tagged-fields list) - so that visibility split is a reliable signal of "real payload
 * field" versus "generator internals", without depending on field naming.
 * <p>
 * Each call gets its own instance: {@link #populate} recurses into nested structs, lists and
 * collections, and every level of that recursion needs the same {@link Random} and target
 * {@code version} - carrying them as instance state avoids threading both through every private method.
 */
public final class ReflectiveMessagePopulator {

    // Kroxylicious and Kafka each declare their own copy of these types under the same relative path,
    // one package per namespace - matched by fully-qualified name rather than an import of either
    // concrete class, so this harness stays symmetric between the two namespaces it's proving fidelity
    // between.
    private static final Set<String> UUID_CLASS_NAMES = Set.of(
            "io.kroxylicious.kafka.common.Uuid",
            "org.apache.kafka.common.Uuid");
    private static final Set<String> MULTI_COLLECTION_CLASS_NAMES = Set.of(
            "io.kroxylicious.kafka.common.utils.ImplicitLinkedHashMultiCollection",
            "org.apache.kafka.common.utils.ImplicitLinkedHashMultiCollection");
    private static final Set<String> BASE_RECORDS_CLASS_NAMES = Set.of(
            "io.kroxylicious.kafka.common.record.internal.BaseRecords",
            "org.apache.kafka.common.record.internal.BaseRecords");

    private final Random random;
    private final Short messageVersion;

    private ReflectiveMessagePopulator(Random random, Short messageVersion) {
        this.random = random;
        this.messageVersion = messageVersion;
    }

    /**
     * Populates {@code message}'s fields with deterministic non-default values derived from {@code seed}.
     * No schema/version information is used, so a primitive field left at a constructor-assigned
     * non-zero default (a schema-declared default value) is not overwritten, on the assumption that such
     * a value is exactly the one the field must hold below the version it was introduced in.
     *
     * @param message the instance to populate
     * @param seed the seed controlling the generated values
     */
    @SuppressFBWarnings("PREDICTABLE_RANDOM") // Deterministic pseudorandomness is the point: reproducible test fixtures, not security relevant
    public static void populate(Object message, long seed) {
        new ReflectiveMessagePopulator(new Random(seed), null).populate(message);
    }

    /**
     * Populates {@code message}'s fields with deterministic non-default values, restricted to the
     * fields actually present in {@code message}'s schema at {@code version} - so the result never
     * trips the generated {@code write()}'s own version guards for fields introduced later, or
     * excluded again earlier, than {@code version}.
     *
     * @param message the instance to populate
     * @param version the wire version {@code message} will be serialised at
     * @param seed the seed controlling the generated values
     */
    @SuppressFBWarnings("PREDICTABLE_RANDOM") // Deterministic pseudorandomness is the point: reproducible test fixtures, not security relevant
    public static void populate(Object message, short version, long seed) {
        new ReflectiveMessagePopulator(new Random(seed), version).populate(message);
    }

    private void populate(Object message) {
        Set<String> schemaFieldNames = messageVersion == null ? null : schemaFieldNamesAt(message.getClass(), messageVersion);
        for (Field field : message.getClass().getDeclaredFields()) {
            int modifiers = field.getModifiers();
            boolean generatorInternal = Modifier.isStatic(modifiers) || Modifier.isPrivate(modifiers);
            boolean outsideSchema = schemaFieldNames != null && !schemaFieldNames.contains(field.getName());
            // Fields outside the target version's schema (not yet introduced, or dropped again before
            // this version - specs aren't purely additive) must keep their constructor default: it's the
            // only value the generated write() accepts for them at this version.
            if (generatorInternal || outsideSchema) {
                continue;
            }
            field.setAccessible(true);
            Class<?> type = field.getType();
            try {
                // Without schema information we can't tell whether a primitive's constructor-assigned
                // non-zero value (e.g. an enum-like byte defaulting to 1) is a schema-declared default
                // that write() requires below its introduction version, so it's left alone in that case.
                // Once schemaFieldNames has already restricted us to fields the target version's schema
                // actually declares, no such guard is needed - the field is safe to overwrite outright.
                boolean unknownSchemaPrimitiveDefault = schemaFieldNames == null && type.isPrimitive() && !isDefaultPrimitiveValue(type, field.get(message));
                if (unknownSchemaPrimitiveDefault) {
                    continue;
                }
                field.set(message, valueFor(field.getGenericType()));
            }
            catch (IllegalAccessException e) {
                throw new IllegalStateException("Failed to populate field " + field, e);
            }
        }
    }

    /**
     * Reads the {@code SCHEMAS} array every generated message/struct class declares and returns the
     * camelCase names of the fields present in the schema at {@code version} - the same set write()
     * consults internally, so filtering against it keeps populate() from ever setting a field write()
     * would then refuse to serialise at that version.
     */
    private static Set<String> schemaFieldNamesAt(Class<?> clazz, short version) {
        try {
            Object[] schemas = (Object[]) clazz.getField("SCHEMAS").get(null);
            if (version < 0 || version >= schemas.length || schemas[version] == null) {
                return Set.of();
            }
            Object schema = schemas[version];
            Object[] boundFields = (Object[]) schema.getClass().getMethod("fields").invoke(schema);
            Set<String> names = new HashSet<>();
            for (Object boundField : boundFields) {
                Object fieldDef = boundField.getClass().getField("def").get(boundField);
                String snakeCaseName = (String) fieldDef.getClass().getField("name").get(fieldDef);
                names.add(toCamelCase(snakeCaseName));
            }
            return names;
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to read schema fields for " + clazz + " at version " + version, e);
        }
    }

    private static String toCamelCase(String snakeCaseName) {
        String[] words = snakeCaseName.split("_");
        StringBuilder camelCase = new StringBuilder(words[0]);
        for (int i = 1; i < words.length; i++) {
            String word = words[i];
            camelCase.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1));
        }
        return camelCase.toString();
    }

    private static boolean isDefaultPrimitiveValue(Class<?> type, Object currentValue) {
        if (type == short.class) {
            return ((Short) currentValue) == 0;
        }
        if (type == int.class) {
            return ((Integer) currentValue) == 0;
        }
        if (type == long.class) {
            return ((Long) currentValue) == 0L;
        }
        if (type == byte.class) {
            return ((Byte) currentValue) == 0;
        }
        if (type == boolean.class) {
            return !((Boolean) currentValue);
        }
        if (type == double.class) {
            return ((Double) currentValue) == 0.0;
        }
        throw new IllegalStateException("Unhandled primitive type " + type);
    }

    private Object valueFor(Type type) {
        return scalarValueFor(type)
                .or(() -> containerValueFor(type))
                .or(() -> uuidValueFor(type))
                .or(() -> baseRecordsValueFor(type))
                .or(() -> structValueFor(type))
                .orElseThrow(() -> new UnsupportedOperationException("Don't know how to populate a field of type " + type));
    }

    /**
     * Flat, non-recursive leaf values: primitives, their boxed equivalents, and the handful of built-in
     * reference types ({@code String}, {@code byte[]}, {@code ByteBuffer}) treated as opaque blobs rather
     * than structures to recurse into. Returns {@link Optional#empty()} for any other type, deferring to
     * {@link #valueFor}'s remaining checks.
     */
    private Optional<Object> scalarValueFor(Type type) {
        if (type == short.class || type == Short.class) {
            return Optional.of((short) (1 + randomInt(Short.MAX_VALUE)));
        }
        if (type == int.class || type == Integer.class) {
            return Optional.of(1 + randomInt(Integer.MAX_VALUE - 1));
        }
        if (type == long.class || type == Long.class) {
            return Optional.of(1L + randomInt(Integer.MAX_VALUE - 1));
        }
        if (type == byte.class || type == Byte.class) {
            return Optional.of((byte) (1 + randomInt(Byte.MAX_VALUE)));
        }
        if (type == boolean.class || type == Boolean.class) {
            return Optional.of(true);
        }
        if (type == double.class || type == Double.class) {
            return Optional.of(1.0 + random.nextInt(1_000_000));
        }
        if (type == String.class) {
            return Optional.of("value-" + random.nextInt(1_000_000));
        }
        if (type == byte[].class) {
            return Optional.of(randomBytes());
        }
        if (type == ByteBuffer.class) {
            return Optional.of(ByteBuffer.wrap(randomBytes()));
        }
        return Optional.empty();
    }

    private byte[] randomBytes() {
        byte[] bytes = new byte[4 + random.nextInt(8)];
        random.nextBytes(bytes);
        return bytes;
    }

    /**
     * Types that hold a variable number of repeated elements: a generated {@code List<T>} field, or an
     * {@code ImplicitLinkedHashMultiCollection}-based collection. Returns {@link Optional#empty()} for any
     * other type, deferring to {@link #valueFor}'s remaining checks.
     */
    private Optional<Object> containerValueFor(Type type) {
        if (type instanceof ParameterizedType parameterizedType && parameterizedType.getRawType() == List.class) {
            return Optional.of(listValueFor(parameterizedType.getActualTypeArguments()[0]));
        }
        if (type instanceof Class<?> clazz && isMultiCollectionType(clazz)) {
            return Optional.of(collectionValueFor(clazz));
        }
        return Optional.empty();
    }

    private Optional<Object> uuidValueFor(Type type) {
        if (type instanceof Class<?> clazz && isUuidType(clazz)) {
            return Optional.of(newUuid(clazz));
        }
        return Optional.empty();
    }

    private Optional<Object> baseRecordsValueFor(Type type) {
        if (type instanceof Class<?> clazz && isBaseRecordsType(clazz)) {
            return Optional.of(emptyRecords(clazz));
        }
        return Optional.empty();
    }

    private Optional<Object> structValueFor(Type type) {
        if (type instanceof Class<?> structClass) {
            return Optional.of(newStruct(structClass));
        }
        return Optional.empty();
    }

    private int randomInt(int maxValue) {
        return random.nextInt(maxValue);
    }

    private List<Object> listValueFor(Type elementType) {
        List<Object> elements = new ArrayList<>();
        int size = 1 + random.nextInt(2);
        for (int i = 0; i < size; i++) {
            elements.add(valueFor(elementType));
        }
        return elements;
    }

    @SuppressWarnings("java:S1872") // No common supertype exists to instanceof against: Kroxylicious's and
    // Kafka's Uuid are unrelated classes in unrelated packages, matched here by name on purpose.
    private static boolean isUuidType(Class<?> clazz) {
        return UUID_CLASS_NAMES.contains(clazz.getName());
    }

    private Object newUuid(Class<?> uuidClass) {
        try {
            Constructor<?> constructor = uuidClass.getDeclaredConstructor(long.class, long.class);
            return constructor.newInstance(random.nextLong(), random.nextLong());
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to construct " + uuidClass + " via its (long, long) constructor", e);
        }
    }

    @SuppressWarnings("java:S1872") // No common supertype exists to instanceof against: Kroxylicious's and
    // Kafka's ImplicitLinkedHashMultiCollection are unrelated classes in unrelated packages, matched here
    // by name on purpose.
    private static boolean isMultiCollectionType(Class<?> clazz) {
        for (Class<?> ancestor = clazz.getSuperclass(); ancestor != null; ancestor = ancestor.getSuperclass()) {
            if (MULTI_COLLECTION_CLASS_NAMES.contains(ancestor.getName())) {
                return true;
            }
        }
        return false;
    }

    private Object collectionValueFor(Class<?> collectionClass) {
        Type elementType = ((ParameterizedType) collectionClass.getGenericSuperclass()).getActualTypeArguments()[0];
        try {
            Object collection = collectionClass.getDeclaredConstructor().newInstance();
            Method add = findSingleArgAddMethod(collectionClass);
            int size = 1 + random.nextInt(2);
            for (int i = 0; i < size; i++) {
                add.invoke(collection, valueFor(elementType));
            }
            return collection;
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to populate collection " + collectionClass, e);
        }
    }

    private static Method findSingleArgAddMethod(Class<?> collectionClass) {
        for (Method method : collectionClass.getMethods()) {
            if (method.getName().equals("add") && method.getParameterCount() == 1) {
                return method;
            }
        }
        throw new IllegalStateException("No single-argument add(...) method found on " + collectionClass);
    }

    @SuppressWarnings("java:S1872") // No common supertype exists to instanceof against: Kroxylicious's and
    // Kafka's BaseRecords are unrelated interfaces in unrelated packages, matched here by name on purpose.
    private static boolean isBaseRecordsType(Class<?> clazz) {
        return BASE_RECORDS_CLASS_NAMES.contains(clazz.getName());
    }

    /**
     * {@code BaseRecords} is an interface with no general-purpose implementation to populate
     * reflectively; the canonical empty records value is a leaf-value substitution, the same kind of
     * move as using {@code Uuid.ZERO_UUID}-shaped construction or an empty {@code ByteBuffer} - not an
     * attempt to fabricate a real record batch.
     */
    private static Object emptyRecords(Class<?> baseRecordsClass) {
        try {
            Class<?> memoryRecordsClass = Class.forName(baseRecordsClass.getPackageName() + ".MemoryRecords");
            return memoryRecordsClass.getField("EMPTY").get(null);
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to look up MemoryRecords.EMPTY alongside " + baseRecordsClass, e);
        }
    }

    private Object newStruct(Class<?> structClass) {
        try {
            Object instance = structClass.getDeclaredConstructor().newInstance();
            populate(instance);
            return instance;
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate nested struct " + structClass, e);
        }
    }
}
