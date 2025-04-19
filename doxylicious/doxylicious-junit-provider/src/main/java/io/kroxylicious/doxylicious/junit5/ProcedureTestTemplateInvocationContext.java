/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.junit5;

import java.util.List;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.doxylicious.exec.ProcExecutor;
import io.kroxylicious.doxylicious.model.ProcDecl;

class ProcedureTestTemplateInvocationContext implements TestTemplateInvocationContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureTestTemplateInvocationContext.class);

    private final ProcedureImpl procedure;

    ProcedureTestTemplateInvocationContext(List<ProcDecl> caseProcs,
                                           ProcExecutor exec,
                                           String procId) {

        this.procedure = new ProcedureImpl(procId, caseProcs, exec);
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        return procedure.toString();
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        return List.of(
                new BeforeTestExecutionCallback() {
                    @Override
                    public void beforeTestExecution(ExtensionContext context) throws Exception {
                        procedure.executePrereqs();
                    }
                },
                new AfterTestExecutionCallback() {
                    @Override
                    public void afterTestExecution(ExtensionContext context) throws Exception {
                        try {
                            procedure.executeTeardown(); // TODO only execute this is we executed the
                        }
                        finally {
                            procedure.executePrereqsTeardown();
                        }
                    }
                },
                new ParameterResolver() {
                    @Override
                    public boolean supportsParameter(
                                                     ParameterContext parameterContext,
                                                     ExtensionContext extensionContext)
                            throws ParameterResolutionException {
                        return parameterContext.getParameter().getType().equals(Procedure.class);
                    }

                    @Override
                    public Object resolveParameter(
                                                   ParameterContext parameterContext,
                                                   ExtensionContext extensionContext)
                            throws ParameterResolutionException {
                        return procedure;
                    }
                });
    }

}
