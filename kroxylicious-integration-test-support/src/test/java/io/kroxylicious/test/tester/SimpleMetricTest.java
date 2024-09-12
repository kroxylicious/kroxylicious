/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimpleMetricTest {

    /**
     * Known good from <a href="https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md#text-format-example">exposition_formats</a>
     */
    static Stream<Arguments> knownGood() {
        return Stream.of(
                Arguments.of(
                        "single metric with labels, value and timestamp",
                        """
                                http_requests_total{method="post",code="200"} 1027 1395066363000""",
                        List.of(new SimpleMetric("http_requests_total", Map.of("method", "post", "code", "200"), 1027))
                ),
                Arguments.of(
                        "single metric no labels",
                        """
                                metric_without_timestamp_and_labels 12.47""",
                        List.of(new SimpleMetric("metric_without_timestamp_and_labels", Map.of(), 12.47))
                ),
                Arguments.of(
                        "many metrics",
                        """
                                rpc_duration_seconds{quantile="0.01"} 3102
                                rpc_duration_seconds{quantile="0.05"} 3272""",
                        List.of(
                                new SimpleMetric("rpc_duration_seconds", Map.of("quantile", "0.01"), 3102),
                                new SimpleMetric("rpc_duration_seconds", Map.of("quantile", "0.05"), 3272)
                        )
                ),
                Arguments.of(
                        "metric with help",
                        """
                                # HELP rpc_duration_seconds A summary of the RPC duration in seconds.
                                # TYPE rpc_duration_seconds summary
                                rpc_duration_seconds{quantile="0.05"} 3272""",
                        List.of(new SimpleMetric("rpc_duration_seconds", Map.of("quantile", "0.05"), 3272))
                ),
                Arguments.of(
                        "metric surrounded by empty lines",
                        """

                                rpc_duration_seconds{quantile="0.05"} 3272
                                """,
                        List.of(new SimpleMetric("rpc_duration_seconds", Map.of("quantile", "0.05"), 3272))
                ),
                Arguments.of(
                        "no metrics - newlines only",
                        """


                                """,
                        List.of()
                ),
                Arguments.of(
                        "no metrics - comments only",
                        """
                                #Mary had a little lamb
                                #His fleece was white as snow
                                """,
                        List.of()
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("knownGood")
    void parseKnownGood(String name, String expositionString, List<SimpleMetric> expected) {
        var actual = SimpleMetric.parse(expositionString);
        assertThat(actual)
                          .containsExactlyElementsOf(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = { "(bad)", "no_value", "invalid_value three" })
    void parseError(String malformed) {
        assertThatThrownBy(() -> SimpleMetric.parse(malformed))
                                                               .isInstanceOf(IllegalArgumentException.class)
                                                               .hasMessageContaining("Failed to parse metric");
    }
}
