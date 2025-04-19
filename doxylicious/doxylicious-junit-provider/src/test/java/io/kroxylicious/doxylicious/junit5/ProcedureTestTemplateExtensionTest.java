/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.junit5;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.doxylicious.exec.ExecException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcedureTestTemplateExtensionTest {

    @TestTemplate
    @ExtendWith(ProcedureTestTemplateExtension.class)
    @TestProcedure("notional_proc")
    void test(Procedure procedure) {
        // when
        procedure.executeProcedure();

        // then
        procedure.assertVerification();
    }

    @TestTemplate
    @ExtendWith(ProcedureTestTemplateExtension.class)
    @TestProcedure("failing_procedure")
    void failingProcedureShouldThrow(Procedure procedure) {
        assertThatThrownBy(procedure::executeProcedure)
                .isInstanceOf(AssertionError.class)
                .hasMessage("Test procedure 'failing_procedure' failed during execution")
                .cause()
                .isInstanceOf(ExecException.class)
                .hasMessage("'failing_procedure/procedure/0/steps/0/exec': Command [false] failed with code 1 and no error output");

    }

    @TestTemplate
    @ExtendWith(ProcedureTestTemplateExtension.class)
    @TestProcedure("failing_verification")
    void failingVerificationShouldThrow(Procedure procedure) {
        // given
        procedure.executeProcedure();

        // then
        assertThatThrownBy(procedure::assertVerification)
                .isInstanceOf(AssertionError.class)
                .hasMessage("Test procedure 'failing_verification' failed during verification")
                .cause()
                .isInstanceOf(ExecException.class)
                .hasMessage("'failing_verification/verification/0/steps/0/exec': Command [false] failed with code 1 and no error output");
    }

}
