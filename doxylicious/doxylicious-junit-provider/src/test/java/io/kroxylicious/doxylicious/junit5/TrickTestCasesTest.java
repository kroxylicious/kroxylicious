/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.junit5;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.MethodSelector;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Event;
import org.opentest4j.AssertionFailedError;

import io.kroxylicious.doxylicious.junit5.support.TrickyCases;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;

/**
 * See the comment on {@link TrickyCases}.
 */
public class TrickTestCasesTest {

    @Test
    void testTrickyTestCases() {

        for (var meth : TrickyCases.class.getDeclaredMethods()) {
            Expect annotation = meth.getAnnotation(Expect.class);
            if (annotation != null) {

                MethodSelector methodSelector = selectMethod(TrickyCases.class, meth.getName(), Procedure.class);

                var exceptionType = annotation.value();
                var exceptionMessage = annotation.message();

                EngineTestKit
                        .engine("junit-jupiter")
                        .selectors(methodSelector)
                        .execute()
                        .testEvents()
                        .assertStatistics(stats -> stats
                                .started(1)
                                .succeeded(0)
                                .failed(1))
                        .failed().stream().map(Event::getPayload).forEach(payload -> {
                            assertThat(payload).isPresent().get().isInstanceOf(TestExecutionResult.class);
                            var testExceptionAssert = assertThat(((TestExecutionResult) payload.get()).getThrowable()).isPresent().get()
                                    .asInstanceOf(InstanceOfAssertFactories.throwable(
                                            AssertionFailedError.class));

                            testExceptionAssert.isExactlyInstanceOf(exceptionType)
                                    .hasMessage(exceptionMessage);
                        });
            }
        }

    }

}
