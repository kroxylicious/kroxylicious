/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;

/**
 * Generates an ApiMessage instance per ApiKey. The message is built
 * with reflection and populated with randomised data. The data is built
 * from the same random seed and the ApiKeys and methods are iterated over
 * in a deterministic order so the output of `createRequestSamples` should
 * be consistent.
 * <br/>
 * The ApiMessageSampleGenerator gives the guarantee that all generated request
 * samples would require Kafka response (in concrete terms, this means that
 * the {@link ProduceRequestData} instances have an acks value != 0.
 */
public class ApiMessageSampleGenerator {

    public record ApiAndVersion(ApiKeys keys, short apiVersion) {

    }

    public static final int RANGE_MIN = 0;
    public static final int RANGE_MAX = 1024;

    private ApiMessageSampleGenerator() {

    }

    /**
     * Generates a sample request ApiMessage for all ApiKeys
     * @return ApiKeys to message
     */
    @SuppressWarnings("java:S2245") // random is used to generate test data, secure entropy not required.
    public static Map<ApiAndVersion, ApiMessage> createRequestSamples() {
        Random random = new Random(0);
        return instantiateAll(DataClasses.getRequestClasses(), random);
    }

    /**
     * Generates a sample response ApiMessage for all ApiKeys
     * @return ApiKeys to message
     */
    @SuppressWarnings("java:S2245") // random is used to generate test data, secure entropy not required.
    public static Map<ApiAndVersion, ApiMessage> createResponseSamples() {
        Random random = new Random(0);
        return instantiateAll(DataClasses.getResponseClasses(), random);
    }

    private static Map<ApiAndVersion, ApiMessage> instantiateAll(Map<ApiKeys, Class<? extends ApiMessage>> messages, Random random) {
        return messages.entrySet().stream().sorted(Comparator.comparing(apiKeysClassEntry -> apiKeysClassEntry.getKey().name))
                .flatMap(ApiMessageSampleGenerator::getSupportedVersions)
                .flatMap(entry -> instantiateSample(entry, random)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Stream<AbstractMap.SimpleEntry<ApiAndVersion, Class<? extends ApiMessage>>> getSupportedVersions(
                                                                                                                    Map.Entry<ApiKeys, Class<? extends ApiMessage>> apiKeysClassEntry) {
        short lowestSupportedVersion = apiKeysClassEntry.getKey().messageType.lowestSupportedVersion();
        short highestSupportedVersion = apiKeysClassEntry.getKey().messageType.highestSupportedVersion(true);
        return IntStream.range(lowestSupportedVersion, highestSupportedVersion + 1).mapToObj(version -> new AbstractMap.SimpleEntry<>(
                new ApiAndVersion(apiKeysClassEntry.getKey(), (short) version), apiKeysClassEntry.getValue()));
    }

    private static Stream<Map.Entry<ApiAndVersion, ApiMessage>> instantiateSample(Map.Entry<ApiAndVersion, Class<? extends ApiMessage>> entry, Random random) {
        try {
            Field schemasField = entry.getValue().getDeclaredField("SCHEMAS");
            Schema[] schemas = (Schema[]) schemasField.get(null);
            Field lowestSupportedVersion = entry.getValue().getDeclaredField("LOWEST_SUPPORTED_VERSION");
            short lowestVersion = (short) lowestSupportedVersion.get(null);
            Field highestSupportedVersion = entry.getValue().getDeclaredField("HIGHEST_SUPPORTED_VERSION");
            short highestVersion = (short) highestSupportedVersion.get(null);
            short apiVersion = entry.getKey().apiVersion;
            if (apiVersion < lowestVersion || apiVersion > highestVersion) {
                return Stream.of();
            }
            Schema schema = schemas[apiVersion];
            ApiMessage message = (ApiMessage) instantiate(entry.getValue(), random, schema);
            return Stream.of(new AbstractMap.SimpleEntry<>(entry.getKey(), message));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Message instantiate(Class<? extends Message> clazz, Random random, Schema schema) {
        try {
            Message instance = clazz.getConstructor().newInstance();
            Map<String, org.apache.kafka.common.protocol.types.Field> fieldsForVersion = Arrays.stream(schema.fields())
                    .filter(boundField -> !boundField.def.name.startsWith("_"))
                    .collect(Collectors.toMap(ApiMessageSampleGenerator::toSetterName, boundField -> boundField.def));

            Stream<Method> sortedMethods = Arrays.stream(clazz.getMethods())
                    .filter(method -> method.getName().startsWith("set"))
                    .filter(method -> fieldsForVersion.containsKey(method.getName()))
                    .sorted(Comparator.comparing(Method::getName));
            sortedMethods.forEach(method -> {
                try {
                    if (instance instanceof ProduceRequestData prd && "setAcks".equals(method.getName())) {
                        // Produce request is a special case in Kafka in that if acks == 0 no reply is to be sent.
                        prd.setAcks((short) 1);
                    }
                    else {
                        Object o = instantiateArg(method, random, fieldsForVersion.get(method.getName()).type);
                        method.invoke(instance, o);
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            return instance;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String toSetterName(BoundField boundField) {
        return "set" + Arrays.stream(boundField.def.name.split("_")).map(s -> s.substring(0, 1).toUpperCase() + s.substring(1)).collect(Collectors.joining());
    }

    @SuppressWarnings("java:S3776") // the complexity warning owing to the type ladder is a false positive in this case
    private static Object instantiateArg(Class<?> paramClass, Type paramGenericType, Random random, org.apache.kafka.common.protocol.types.Type type) {
        if (paramClass == long.class || paramClass == Long.class) {
            return random.nextLong(RANGE_MIN, RANGE_MAX);
        }
        else if (paramClass == int.class || paramClass == Integer.class) {
            return random.nextInt(RANGE_MIN, RANGE_MAX);
        }
        else if (paramClass == short.class || paramClass == Short.class) {
            return (short) random.nextInt(RANGE_MIN, RANGE_MAX);
        }
        else if (paramClass == double.class || paramClass == Double.class) {
            return random.nextDouble(RANGE_MIN, RANGE_MAX);
        }
        else if (paramClass == String.class) {
            return "random-string-" + random.nextInt(RANGE_MIN, RANGE_MAX);
        }
        else if (paramClass == byte.class || paramClass == Byte.class) {
            return (byte) random.nextInt(RANGE_MIN, 127);
        }
        else if (paramClass == byte[].class) {
            byte[] bytes = new byte[5];
            random.nextBytes(bytes);
            return bytes;
        }
        else if (paramClass == ByteBuffer.class) {
            byte[] bytes = new byte[5];
            random.nextBytes(bytes);
            return ByteBuffer.wrap(bytes).flip();
        }
        else if (paramClass == boolean.class) {
            return random.nextBoolean();
        }
        else if (paramClass == Uuid.class) {
            return randomUuid(random);
        }
        else if (paramClass == BaseRecords.class) {
            return randomMemoryRecords(random);
        }
        else if (Message.class.isAssignableFrom(paramClass)) {
            return instantiate(paramClass.asSubclass(Message.class), random, (Schema) type);
        }
        else if (List.class.isAssignableFrom(paramClass)) {
            return instantiateList(paramGenericType, random, type);
        }
        else if (ImplicitLinkedHashCollection.class.isAssignableFrom(paramClass)) {
            return instantiateMessageCollection(paramClass.asSubclass(ImplicitLinkedHashCollection.class), random, type);
        }
        else {
            throw new IllegalArgumentException("unexpected type " + paramClass.getSimpleName());
        }
    }

    private static MemoryRecords randomMemoryRecords(Random random) {
        String key = "key-" + random.nextInt(RANGE_MIN, RANGE_MAX);
        String value = "value-" + random.nextInt(RANGE_MIN, RANGE_MAX);
        long baseOffset = random.nextInt(RANGE_MIN, RANGE_MAX);
        long logAppendTime = RecordBatch.NO_TIMESTAMP;
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1), RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
                TimestampType.CREATE_TIME, baseOffset,
                logAppendTime,
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH)) {
            builder.append(new SimpleRecord(random.nextLong(RANGE_MIN, RANGE_MAX), key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)));
            return builder.build();
        }
    }

    /**
     * Takes the guts of the JVM randomUUID and byte[] constructor
     * so that we can create a random UUID using our random
     * implementation. This is so that all random data used in generation
     * comes from the same random seed to generate deterministic samples.
     * @param random the random to use
     * @return a random Uuid
     */
    private static Uuid randomUuid(Random random) {
        byte[] randomBytes = new byte[16];
        random.nextBytes(randomBytes);
        randomBytes[6] &= 0x0f; /* clear version */
        randomBytes[6] |= 0x40; /* set to version 4 */
        randomBytes[8] &= 0x3f; /* clear variant */
        randomBytes[8] |= 0x80; /* set to IETF variant */
        long mostSignificantBits = 0;
        long leastSignificantBits = 0;
        for (int i = 0; i < 8; i++) {
            mostSignificantBits = (mostSignificantBits << 8) | (randomBytes[i] & 0xff);
        }
        for (int i = 8; i < 16; i++) {
            leastSignificantBits = (leastSignificantBits << 8) | (randomBytes[i] & 0xff);
        }
        return new Uuid(mostSignificantBits, leastSignificantBits);
    }

    private static Object instantiateMessageCollection(Class<? extends ImplicitLinkedHashCollection> genericType, Random random,
                                                       org.apache.kafka.common.protocol.types.Type field) {
        try {
            ImplicitLinkedHashCollection<ImplicitLinkedHashCollection.Element> collection = genericType.getConstructor().newInstance();
            ParameterizedType paramClass1 = (ParameterizedType) genericType.getGenericSuperclass();
            Type actualTypeArgument = paramClass1.getActualTypeArguments()[0];
            Class<?> typeArgument = (Class<?>) actualTypeArgument;
            Object o = instantiateArg(typeArgument, actualTypeArgument, random, field.arrayElementType().orElseThrow());
            collection.add((ImplicitLinkedHashCollection.Element) o);
            return collection;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Object instantiateList(Type paramClass, Random random, org.apache.kafka.common.protocol.types.Type type) {
        ArrayList<Object> objects = new ArrayList<>();
        ParameterizedType paramClass1 = (ParameterizedType) paramClass;
        Type actualTypeArgument = paramClass1.getActualTypeArguments()[0];
        Class<?> typeArgument = (Class<?>) actualTypeArgument;
        objects.add(instantiateArg(typeArgument, actualTypeArgument, random, type.arrayElementType().orElseThrow()));
        return objects;
    }

    private static Object instantiateArg(Method method, Random random, org.apache.kafka.common.protocol.types.Type type) {
        int parameterCount = method.getParameterCount();
        if (parameterCount != 1) {
            throw new IllegalArgumentException("setter takes more than one arg!");
        }
        Class<?> parameterType = method.getParameterTypes()[0];
        Type genericParameterType = method.getGenericParameterTypes()[0];
        return instantiateArg(parameterType, genericParameterType, random, type);
    }

}
