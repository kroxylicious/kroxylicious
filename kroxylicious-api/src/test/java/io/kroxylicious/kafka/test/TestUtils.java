/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kroxylicious.kafka.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import io.kroxylicious.kafka.common.utils.Utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUtils {

    public static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    public static final String DIGITS = "0123456789";

    public static final String LETTERS_AND_DIGITS = LETTERS + DIGITS;

    public static final Random SEEDED_RANDOM = new Random(192348092834L);

    public static String randomString(final int len) {
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; i++)
            b.append(LETTERS_AND_DIGITS.charAt(SEEDED_RANDOM.nextInt(LETTERS_AND_DIGITS.length())));
        return b.toString();
    }

    public static File tempFile(final String prefix, final String suffix) throws IOException {
        final File file = Files.createTempFile(prefix, suffix).toFile();
        file.deleteOnExit();
        return file;
    }

    public static File tempFile() throws IOException {
        return tempFile("kafka", ".tmp");
    }

    public static File tempDirectory(final String prefix) {
        return tempDirectory(null, prefix);
    }

    public static File tempDirectory() {
        return tempDirectory(null);
    }

    public static File tempDirectory(final Path parent, String prefix) {
        final File file;
        prefix = prefix == null ? "kafka-" : prefix;
        try {
            file = parent == null ? Files.createTempDirectory(prefix).toFile() : Files.createTempDirectory(parent, prefix).toFile();
        }
        catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }

        return file;
    }

    public static <T> void checkEquals(Iterable<T> it1, Iterable<T> it2) {
        assertEquals(toList(it1), toList(it2));
    }

    public static <T> void checkEquals(Iterator<T> it1, Iterator<T> it2) {
        assertEquals(Utils.toList(it1), Utils.toList(it2));
    }

    public static <T> List<T> toList(Iterable<? extends T> iterable) {
        List<T> list = new ArrayList<>();
        for (T item : iterable)
            list.add(item);
        return list;
    }
}
