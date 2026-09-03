/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
 */
public final class ReflectiveMessagePopulator {

    private ReflectiveMessagePopulator() {
    }

    /**
     * Populates {@code message}'s fields with deterministic non-default values derived from {@code seed}.
     *
     * @param message the instance to populate
     * @param seed the seed controlling the generated values
     */
    @SuppressFBWarnings("PREDICTABLE_RANDOM") // Deterministic pseudorandomness is the point: reproducible test fixtures, not security relevant
    public static void populate(Object message, long seed) {
        populate(message, new Random(seed));
    }

    private static void populate(Object message, Random random) {
        for (Field field : message.getClass().getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) || Modifier.isPrivate(modifiers)) {
                continue;
            }
            field.setAccessible(true);
            Object value = valueFor(field.getGenericType(), random);
            try {
                field.set(message, value);
            }
            catch (IllegalAccessException e) {
                throw new IllegalStateException("Failed to populate field " + field, e);
            }
        }
    }

    private static Object valueFor(Type type, Random random) {
        if (type == short.class || type == Short.class) {
            return (short) (1 + random.nextInt(Short.MAX_VALUE));
        }
        if (type == int.class || type == Integer.class) {
            return 1 + random.nextInt(Integer.MAX_VALUE - 1);
        }
        if (type == String.class) {
            return "value-" + random.nextInt(1_000_000);
        }
        if (type instanceof ParameterizedType parameterizedType && parameterizedType.getRawType() == List.class) {
            return listValueFor(parameterizedType.getActualTypeArguments()[0], random);
        }
        throw new UnsupportedOperationException("Don't know how to populate a field of type " + type);
    }

    private static List<Object> listValueFor(Type elementType, Random random) {
        List<Object> elements = new ArrayList<>();
        int size = 1 + random.nextInt(2);
        for (int i = 0; i < size; i++) {
            elements.add(structValueFor(elementType, random));
        }
        return elements;
    }

    private static Object structValueFor(Type elementType, Random random) {
        if (!(elementType instanceof Class<?> structClass)) {
            throw new UnsupportedOperationException("Don't know how to populate a list element of type " + elementType);
        }
        try {
            Object instance = structClass.getDeclaredConstructor().newInstance();
            populate(instance, random);
            return instance;
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate nested struct " + structClass, e);
        }
    }
}
