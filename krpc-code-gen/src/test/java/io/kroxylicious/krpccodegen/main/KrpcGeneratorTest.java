/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.main;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

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
class KrpcGeneratorTest {

    private static final String MESSAGE_SPECS_PATH = "message-specs/common/message";
    private static final String OUTPUT_DIR = "krpc-test-sources/krpc";
    private static final String TEST_CLASSES_DIR = "test-classes";

    @Test
    public void testHelloWorld() throws Exception {
        KrpcGenerator gen = KrpcGenerator.single()
                .withMessageSpecDir(getMessageSpecDir())
                .withMessageSpecFilter("*.json")
                .withTemplateDir(getTemplateDir())
                .withTemplateNames(List.of("hello-world/example.ftl"))
                .withOutputPackage("com.foo")
                .withOutputDir(getOutputDir())
                .withOutputFilePattern("${messageSpecName}.txt")
                .build();

        gen.generate();
    }

    @Test
    public void testKrpcData() throws Exception {
        KrpcGenerator gen = KrpcGenerator.single()
                .withMessageSpecDir(getMessageSpecDir())
                .withMessageSpecFilter("*.json")
                .withTemplateDir(getTemplateDir())
                .withTemplateNames(List.of("Data/example.ftl"))
                .withOutputPackage("com.foo")
                .withOutputDir(getOutputDir())
                .withOutputFilePattern("${messageSpecName}.java")
                .build();

        gen.generate();
    }

    @Test
    public void testKproxyFilter() throws Exception {
        KrpcGenerator gen = KrpcGenerator.single()
                .withMessageSpecDir(getMessageSpecDir())
                .withMessageSpecFilter("*.json")
                .withTemplateDir(getTemplateDir())
                .withTemplateNames(List.of("Kproxy/Filter.ftl"))
                .withOutputPackage("com.foo")
                .withOutputDir(getOutputDir())
                .withOutputFilePattern("${messageSpecName}Filter.java")
                .build();

        gen.generate();
    }

    @Test
    public void testKproxyRequestFilter() throws Exception {
        KrpcGenerator gen = KrpcGenerator.multi()
                .withMessageSpecDir(getMessageSpecDir())
                .withMessageSpecFilter("*{Request}.json")
                .withTemplateDir(getTemplateDir())
                .withTemplateNames(List.of("Kproxy/KrpcRequestFilter.ftl"))
                .withOutputPackage("com.foo")
                .withOutputDir(getOutputDir())
                .withOutputFilePattern("${templateName}.java")
                .build();

        gen.generate();
    }

    private static File getMessageSpecDir() {
        return getBuildDir().resolve(MESSAGE_SPECS_PATH).toFile();
    }

    private static File getOutputDir() {
        return getBuildDir().resolve(OUTPUT_DIR).toFile();
    }

    private static File getTemplateDir() {
        return getBuildDir().resolve(TEST_CLASSES_DIR).toFile();
    }

    private static Path getBuildDir() {
        try {
            return Paths.get(KrpcGeneratorTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
        }
        catch (URISyntaxException e) {
            throw new RuntimeException("Couldn't resolve build directory", e);
        }
    }
}
