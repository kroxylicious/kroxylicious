/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 * Simple representation of a Prometheus metric
 * @param name metric name
 * @param labels metric labels
 * @param value metric value
 */
public record SimpleMetric(String name, Map<String, String> labels, double value) {

    public static List<SimpleMetric> parse(String input) {
        CharStream chars = CharStreams.fromString(input);

        ErrorListener errorListener = new ErrorListener(input);
        PrometheusMetricLexer lexer = new PrometheusMetricLexer(chars);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        PrometheusMetricParser parser = new PrometheusMetricParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        var listener = new MetricListener();
        ParseTreeWalker.DEFAULT.walk(listener, parser.file());
        return listener.getResult();
    }

    private static class MetricListener extends PrometheusMetricBaseListener {

        private final List<SimpleMetric> metrics = new ArrayList<>();

        List<SimpleMetric> getResult() {
            return List.copyOf(metrics);
        }

        @Override
        public void exitMetric(PrometheusMetricParser.MetricContext ctx) {
            var labels = new HashMap<String, String>();
            if (ctx.labelSet() != null && ctx.labelSet().labelList() != null) {
                ctx.labelSet().labelList().label().forEach(label -> {
                    String labelValue = label.QUOTED_STRING().getText();
                    String unquoted = labelValue.substring(1, labelValue.length() - 1);
                    labels.put(label.IDENTIFIER().getText(), unquoted);
                });
            }

            metrics.add(new SimpleMetric(
                    ctx.metricName().getText(),
                    Map.copyOf(labels),
                    parseValue(ctx.value().getText())));
        }

        private static double parseValue(String value) {
            return switch (value) {
                case "+Inf" -> Double.POSITIVE_INFINITY;
                case "-Inf" -> Double.NEGATIVE_INFINITY;
                case "NaN" -> Double.NaN;
                default -> Double.parseDouble(value);
            };
        }
    }

    private static class ErrorListener extends BaseErrorListener {
        private final String input;

        ErrorListener(String input) {
            this.input = input;
        }

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer,
                                Object offendingSymbol,
                                int line, int charPositionInLine,
                                String msg, RecognitionException e) {
            String errorMessage = String.format("Failed to parse metric at line %d, position %d in [ %s ]: %s", line, charPositionInLine, input, msg);
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
