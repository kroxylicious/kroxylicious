/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.doc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.List;

import io.kroxylicious.doxylicious.model.Prereq;
import io.kroxylicious.doxylicious.model.ProcDecl;
import io.kroxylicious.doxylicious.model.StepDecl;
import io.kroxylicious.doxylicious.model.Unit;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * Renders a ProcDecl as asciidoc.
 */
public class ProcAsciidocGenerator {

    private final Template template;

    public ProcAsciidocGenerator() {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_34);
        cfg.setClassForTemplateLoading(this.getClass(), "");
        try {
            cfg.setObjectWrapper(new RecordWrapper());
            this.template = cfg.getTemplate("procedure.ftlh");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Validate the given proc, rendering it to an asciidoc procedure if is has no errors
     * @param unit The unit containing the proc
     * @param procDecl The proc
     * @param errorReporter The error reports
     * @param out Where to write the procedure.
     * @throws DocumentationException If the proc could not be written
     * @throws IOException If there was an IO error
     */
    public void renderProc(Unit unit,
                           ProcDecl procDecl,
                           ErrorReporter errorReporter,
                           Writer out)
            throws DocumentationException, IOException {
        int priorNumErrors = errorReporter.numErrors();
        validateProc(unit, procDecl, errorReporter);

        // Only render if there were no errors
        if (errorReporter.numErrors() == priorNumErrors) {
            try {
                this.template.process(procDecl, out);
            }
            catch (TemplateException e) {
                throw new DocumentationException(e);
            }
        }
    }

    private void validateProc(Unit unit,
                              ProcDecl procDecl,
                              ErrorReporter errorReporter) {
        if (procDecl.title() == null) {
            errorReporter.reportError(String.format("%s: proc '%s': Title must not be empty.", unit.sourceName(), procDecl.id()));
        }
        validatePrereqs(unit, procDecl, errorReporter);
        validateProcedureSteps(unit, procDecl, procDecl.procedure(), errorReporter);
        validateProcedureSteps(unit, procDecl, procDecl.verification(), errorReporter);
    }

    private static void validatePrereqs(Unit unit,
                                        ProcDecl procDecl,
                                        ErrorReporter errorReporter) {
        List<Prereq> prereqs = procDecl.prereqs();
        for (int i = 0; i < prereqs.size(); i++) {
            var prereq = prereqs.get(i);
            if (prereq.adoc() == null) {
                errorReporter.reportError(String.format("%s: proc '%s': Prereq %s must have adoc.", unit.sourceName(), procDecl.id(), i + 1));
            }
        }
    }

    private static void validateProcedureSteps(Unit unit,
                                               ProcDecl procDecl,
                                               List<StepDecl> steps,
                                               ErrorReporter errorReporter) {

        for (int i = 0; i < steps.size(); i++) {
            var step = steps.get(i);
            if (step.substeps() == null) {
                errorReporter.reportError(String.format("%s: proc '%s': Steps must have substeps.", unit.sourceName(), procDecl.id()));
            }
            else {
                validateSubsteps(unit, procDecl, errorReporter, i, step.substeps());
            }

        }
    }

    private static void validateSubsteps(Unit unit,
                                         ProcDecl procDecl,
                                         ErrorReporter errorReporter,
                                         final int stepIndex, List<StepDecl> substeps) {
        boolean prevExec = false;
        for (int j = 0; j < substeps.size(); j++) {
            var substep = substeps.get(j);
            if (substep.substeps() != null && !substep.substeps().isEmpty()) {
                errorReporter.reportError(String.format("%s: proc '%s': Substeps must not have subsubsubsteps.", unit.sourceName(), procDecl.id()));
            }
            if (j == 0 && substep.adoc() == null) {
                errorReporter.reportError(
                        String.format("%s: proc '%s': Step %s, substep %s: Substeps must start with adoc. %n%s", unit.sourceName(), procDecl.id(), stepIndex + 1, j + 1,
                                substep));
            }
            if (prevExec && substep.exec() != null) {
                errorReporter.reportError(String.format("%s: proc '%s': Step %s, substep %s: Consecutive 'exec' steps must not be used.", unit.sourceName(),
                        procDecl.id(), stepIndex + 1, j + 1));
            }
            prevExec = substep.exec() != null;
        }
    }

}
