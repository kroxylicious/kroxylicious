/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.junit5;

import java.util.List;
import java.util.stream.Collectors;

import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.doxylicious.exec.ExecException;
import io.kroxylicious.doxylicious.exec.ProcExecutor;
import io.kroxylicious.doxylicious.model.ProcDecl;

class ProcedureImpl implements Procedure {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureImpl.class);

    private final List<ProcDecl> caseProcs;
    private final ProcExecutor executor;
    private final String procId;
    private int undoProcedureFrom = -1;
    private int undoVerificationFrom = -1;

    ProcedureImpl(String procId, List<ProcDecl> caseProcs, ProcExecutor executor) {
        if (caseProcs.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.procId = procId;
        this.caseProcs = caseProcs;
        this.executor = executor;
    }

    List<ProcDecl> caseProcs() {
        return caseProcs;
    }

    @Override
    public String toString() {
        return procId + caseProcs().stream().map(ProcDecl::id).collect(Collectors.joining(", ", "[", "]"));
    }

    private ProcDecl testProcedure() {
        return caseProcs.get(caseProcs.size() - 1);
    }

    @Override
    public void executeProcedure() {
        ProcDecl procDecl = testProcedure();
        try {
            undoProcedureFrom++;
            LOGGER.info("Executing test proc {}", procDecl);
            executor.executeProcedure(procDecl);
        }
        catch (Exception e) {
            throw new AssertionFailedError("Test procedure '%s' failed during execution".formatted(procDecl.id()), e);
        }
    }

    @Override
    public void assertVerification() {
        ProcDecl procDecl = testProcedure();
        try {
            undoVerificationFrom++;
            LOGGER.info("Verifying test proc {}", procDecl);
            executor.executeVerification(procDecl);
        }
        catch (Exception e) {
            throw new AssertionFailedError("Test procedure '%s' failed during verification".formatted(procDecl.id()), e);
        }
        try {
            executor.executeUndoVerification(procDecl);
        }
        catch (ExecException e) {
            throw new AssertionFailedError("Test procedure '%s' failed during verification undo".formatted(procDecl.id()), e);
        }
    }

    void executePrereqs() {
        for (int i = 0; i < caseProcs.size() - 1; i++) {
            ProcDecl prereqProcDecl = caseProcs.get(i);
            try {
                undoProcedureFrom++;
                LOGGER.info("Executing prerequisite {}", prereqProcDecl);
                executor.executeProcedure(prereqProcDecl);
            }
            catch (Exception e) {
                throw new AssertionFailedError("Prereq '%s' of test procedure '%s' failed during execution".formatted(prereqProcDecl.id(), testProcedure().id()), e);
            }
        }
    }

    void executeTeardown() {

        if (undoProcedureFrom < caseProcs.size() - 1) {
            LOGGER.info("Skipping undo of test procedure (never executed procedure)");
            return;
        }
        ProcDecl procDecl = testProcedure();
        try {
            undoProcedureFrom--;
            LOGGER.info("Undoing proc {}", procDecl);
            executor.executeUndoProcedure(procDecl);
        }
        catch (Exception e) {
            throw new AssertionFailedError("Test procedure '%s' errored while undoing procedure".formatted(procDecl.id()), e);
        }
    }

    void executePrereqsTeardown() {
        for (int i = undoProcedureFrom; i >= 0; i--) {
            ProcDecl prereqProcDecl = caseProcs.get(i);
            try {
                LOGGER.info("Undoing prerequisite procedure {}", prereqProcDecl);
                executor.executeUndoProcedure(prereqProcDecl);
            }
            catch (Exception e) {
                throw new AssertionFailedError("Prereq '%s' of test procedure '%s' errored while undoing procedure".formatted(prereqProcDecl.id(), testProcedure().id()),
                        e);
            }
        }
    }
}
