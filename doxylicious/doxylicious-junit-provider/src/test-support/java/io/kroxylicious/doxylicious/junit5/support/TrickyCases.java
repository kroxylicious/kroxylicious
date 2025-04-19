/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.junit5.support;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.AssertionFailedError;

import io.kroxylicious.doxylicious.junit5.Expect;
import io.kroxylicious.doxylicious.junit5.Procedure;
import io.kroxylicious.doxylicious.junit5.ProcedureTestTemplateExtension;
import io.kroxylicious.doxylicious.junit5.TestProcedure;
import io.kroxylicious.doxylicious.junit5.TrickTestCasesTest;

/**
 * Testing the procedures where the execution is expected to throw during setup or teardown is
 * tricky. Such tests can't be written as normal junit tests because there's no way to
 * express in a test that we expect the test to fail outside the test method
 * (during setup or teardown).
 *
 * This class is exercised by {@link TrickTestCasesTest} using the
 * <a href="https://junit.org/junit5/docs/current/user-guide/#testkit">Junit Platform Test Kid</a>.
 */
@SuppressWarnings("java:S3577")
public class TrickyCases {

    @TestTemplate
    @ExtendWith(ProcedureTestTemplateExtension.class)
    @TestProcedure("failing_teardown")
    @Expect(value = AssertionFailedError.class,
            message="Test procedure 'failing_teardown' errored during tearDown")
    public void failing_teardownShouldThrow(Procedure procedure) {
        // given
        procedure.executeProcedure();

        procedure.assertVerification();
    }

    @TestTemplate
    @ExtendWith(ProcedureTestTemplateExtension.class)
    @TestProcedure("prereq_failing_procedure")
    @Expect(value = AssertionFailedError.class,
            message="Prereq 'failing_procedure' of test procedure 'prereq_failing_procedure' failed during execution")
    public void prereq_failing_procedureShouldThrow(Procedure procedure) {
        // given
        procedure.executeProcedure();

        procedure.assertVerification();
    }

    @TestTemplate
    @ExtendWith(ProcedureTestTemplateExtension.class)
    @TestProcedure("prereq_failing_verification")
    @Expect(value = AssertionFailedError.class,
            message="Prereq 'failing_verification' of test procedure 'prereq_failing_verification' failed during verification")
    public void prereq_failing_verificationShouldThrow(Procedure procedure) {
        // given
        procedure.executeProcedure();

        procedure.assertVerification();
    }

    @TestTemplate
    @ExtendWith(ProcedureTestTemplateExtension.class)
    @TestProcedure("prereq_failing_teardown")
    @Expect(value = AssertionFailedError.class,
            message="Prereq 'failing_teardown' of test procedure 'prereq_failing_teardown' errored during tearDown")
    public void prereq_failing_teardownShouldThrow(Procedure procedure) {
        // given
        procedure.executeProcedure();

        procedure.assertVerification();
    }
}
