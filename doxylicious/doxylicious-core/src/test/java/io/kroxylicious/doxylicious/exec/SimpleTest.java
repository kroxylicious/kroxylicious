/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.exec;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.kroxylicious.doxylicious.Base;
import io.kroxylicious.doxylicious.model.ProcDecl;

public class SimpleTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTest.class);

    @Test
    void simpleTest() throws IOException, ExecException {
        YAMLMapper mapper = new YAMLMapper();
        List<ProcDecl> procs;
        try (InputStream resourceAsStream = getClass().getResourceAsStream("simpleTest.yaml")) {
            JsonParser parser = mapper.createParser(resourceAsStream);
            procs = mapper.readValues(parser, new TypeReference<ProcDecl>() {
            }).readAll();
        }
        var base = Base.from(procs);
        Suite createMinimalProxy = new ProcExecutor(base).suite("create_minimal_proxy", Set.of("have_a_kubectl"));
        LOGGER.info(createMinimalProxy.describe());
        var procExecutor = new ProcExecutor(base);
        for (var procList : createMinimalProxy.cases()) {
            for (var proc : procList) {
                procExecutor.executeProcedure(proc);
            }
        }
    }
}
