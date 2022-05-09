package com.github.tombentley.krpccodegen.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    @Test
    public void testHelloWorld() throws IOException {
        KrpcGenerator gen = new KrpcGenerator();

        File outputDir = new File("/tmp/krpc/");
        outputDir.mkdirs();
        gen.setOutputDir(outputDir);

        gen.setSchemaDir(new File("kafka/clients/src/main/resources/common/message"));

        gen.setTemplateDir(new File("src/test/resources/hello-world"));

        gen.setTemplateNames(List.of("example.ftl"));

        gen.setOutputFilePattern("${schemaName}.txt");

        gen.generate();
    }

    @Test
    public void testKrpcData() throws IOException {
        KrpcGenerator gen = new KrpcGenerator();

        File outputDir = new File("/tmp/krpc/");
        outputDir.mkdirs();
        gen.setOutputDir(outputDir);

        gen.setSchemaDir(new File("kafka/clients/src/main/resources/common/message"));

        gen.setTemplateDir(new File("src/test/resources/Data"));

        gen.setTemplateNames(List.of("example.ftl"));

        gen.setOutputFilePattern("${schemaName}.java");

        gen.generate();
    }

    @Test
    public void testKproxyFilter() throws IOException {
        KrpcGenerator gen = new KrpcGenerator();

        File outputDir = new File("/tmp/krpc/");
        outputDir.mkdirs();
        gen.setOutputDir(outputDir);

        gen.setSchemaDir(new File("kafka/clients/src/main/resources/common/message"));

        gen.setTemplateDir(new File("src/test/resources/Kproxy"));

        gen.setTemplateNames(List.of("Filter.ftl"));

        gen.setOutputFilePattern("${schemaName}Filter.java");

        gen.generate();
    }

}