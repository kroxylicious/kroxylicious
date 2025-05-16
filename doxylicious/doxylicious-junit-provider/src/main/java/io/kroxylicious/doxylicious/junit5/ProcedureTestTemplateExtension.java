/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.junit5;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import io.kroxylicious.doxylicious.Base;
import io.kroxylicious.doxylicious.exec.ProcExecutor;
import io.kroxylicious.doxylicious.exec.Suite;
import io.kroxylicious.doxylicious.model.ExecDecl;

public class ProcedureTestTemplateExtension implements
        TestTemplateInvocationContextProvider {

    private final Base base;

    public ProcedureTestTemplateExtension() {
        base = Base.fromDirectories(
                List.of(Path.of("src/main/proc"),
                        Path.of("src/test/proc")));
    }

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        TestProcedure testProcedure = context.getRequiredTestMethod().getAnnotation(TestProcedure.class);
        if (testProcedure == null) {
            return Stream.empty();
        }

        if (base.procDecl(testProcedure.value()) == null) {
            throw new RuntimeException("Proc " + testProcedure.value() + " not found");
        }

        var workingDir = Path.of(testProcedure.workingDir());
        var timeout = ExecDecl.parseDuration(testProcedure.timeout());
        var destroyTimeout = ExecDecl.parseDuration(testProcedure.destroyTimeout());

        var exec = new ProcExecutor(base, workingDir, timeout, destroyTimeout);
        Suite suite = exec.suite(testProcedure.value(),
                Arrays.stream(testProcedure.assuming()).collect(Collectors.toSet()),
                Arrays.stream(testProcedure.fixing()).collect(Collectors.toMap(TestProcedure.Fix::notional, TestProcedure.Fix::concrete)));

        List<TestTemplateInvocationContext> list = suite.cases().stream()
                .map(procsInCase -> (TestTemplateInvocationContext) new ProcedureTestTemplateInvocationContext(procsInCase, exec, testProcedure.value()))
                .toList();
        return list.stream();
    }

}
