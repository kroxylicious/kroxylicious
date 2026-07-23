/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramSaslClientProvider;
import org.apache.kafka.common.security.scram.internals.ScramSaslServerProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.kroxylicious.sasl.credentialstore.ScramCredential;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that authentication timing is uniform regardless of whether the
 * user exists or the password is correct. This verifies the timing
 * side-channel mitigation in {@link ScramHandler}.
 * <p>
 * The test runs increasing batches in parallel and checks that the means
 * of the three scenarios (success, unknown user, wrong password) converge
 * — i.e. the relative spread between them shrinks below a tight threshold.
 * If there were a real timing difference the means would diverge, not converge.
 */
class ScramTimingTest {

    private static final String VALID_USERNAME = "alice";
    private static final String VALID_PASSWORD = "alice-secret-password";
    private static final String WRONG_PASSWORD = "wrong-password-12345";
    private static final String UNKNOWN_USERNAME = "unknown-user";

    private static final int THREADS = 8;
    private static final int BATCH_SIZE = 50;
    private static final int MAX_BATCHES = 20;
    private static final double MEAN_CONVERGENCE_THRESHOLD = 0.001;
    private static final double STDDEV_MAX_MS = 0.5;

    @BeforeAll
    static void registerProviders() {
        ScramSaslServerProvider.initialize();
        ScramSaslClientProvider.initialize();
    }

    @Test
    void authenticationTimingShouldConvergeAcrossScenarios() throws Exception {
        // Given
        ScramCredential credential = TestCredentialHelper.generateCredential(
                VALID_USERNAME, VALID_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        ScramCredentialStore credentialStore = username -> CompletableFuture.completedFuture(
                VALID_USERNAME.equals(username) ? credential : null);

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            // Warm up JIT in parallel
            List<Future<?>> warmup = new ArrayList<>();
            for (int i = 0; i < THREADS; i++) {
                warmup.add(executor.submit(() -> {
                    for (int j = 0; j < 3; j++) {
                        timeFirstRound(credentialStore, VALID_USERNAME, VALID_PASSWORD);
                        timeFirstRound(credentialStore, UNKNOWN_USERNAME, WRONG_PASSWORD);
                        timeFirstRound(credentialStore, VALID_USERNAME, WRONG_PASSWORD);
                    }
                    return null;
                }));
            }
            for (Future<?> f : warmup) {
                f.get();
            }

            // When — run batches in parallel and check for convergence
            RunningStats successTimes = new RunningStats();
            RunningStats unknownUserTimes = new RunningStats();
            RunningStats wrongPasswordTimes = new RunningStats();

            boolean converged = false;
            for (int batch = 0; batch < MAX_BATCHES; batch++) {
                List<Future<double[]>> futures = new ArrayList<>();
                for (int i = 0; i < BATCH_SIZE; i++) {
                    futures.add(executor.submit(() -> new double[]{
                            timeFirstRound(credentialStore, VALID_USERNAME, VALID_PASSWORD),
                            timeFirstRound(credentialStore, UNKNOWN_USERNAME, WRONG_PASSWORD),
                            timeFirstRound(credentialStore, VALID_USERNAME, WRONG_PASSWORD)
                    }));
                }

                for (Future<double[]> f : futures) {
                    double[] times = f.get();
                    successTimes.accept(times[0]);
                    unknownUserTimes.accept(times[1]);
                    wrongPasswordTimes.accept(times[2]);
                }

                double meanSpread = relativeSpread(
                        successTimes.mean(),
                        unknownUserTimes.mean(),
                        wrongPasswordTimes.mean());

                boolean stdDevsLow = successTimes.stdDev() < STDDEV_MAX_MS
                        && unknownUserTimes.stdDev() < STDDEV_MAX_MS
                        && wrongPasswordTimes.stdDev() < STDDEV_MAX_MS;

                if (meanSpread < MEAN_CONVERGENCE_THRESHOLD && stdDevsLow) {
                    converged = true;
                    break;
                }
            }

            // Then
            assertThat(converged)
                    .as("means should converge (<%.1f%% spread) and std devs should be low (<%.0fms) within %d iterations " +
                            "(success: mean=%.1fms stddev=%.2fms, unknown: mean=%.1fms stddev=%.2fms, wrongPw: mean=%.1fms stddev=%.2fms)",
                            MEAN_CONVERGENCE_THRESHOLD * 100, STDDEV_MAX_MS,
                            successTimes.count(),
                            successTimes.mean(), successTimes.stdDev(),
                            unknownUserTimes.mean(), unknownUserTimes.stdDev(),
                            wrongPasswordTimes.mean(), wrongPasswordTimes.stdDev())
                    .isTrue();
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static double relativeSpread(double a, double b, double c) {
        double max = Math.max(a, Math.max(b, c));
        double min = Math.min(a, Math.min(b, c));
        return (max - min) / max;
    }

    /**
     * Online mean and standard deviation using Welford's algorithm.
     */
    private static class RunningStats {
        private long n;
        private double mean;
        private double m2;

        void accept(double value) {
            n++;
            double delta = value - mean;
            mean += delta / n;
            double delta2 = value - mean;
            m2 += delta * delta2;
        }

        long count() {
            return n;
        }

        double mean() {
            return mean;
        }

        double stdDev() {
            return n < 2 ? 0.0 : Math.sqrt(m2 / (n - 1));
        }
    }

    private double timeFirstRound(ScramCredentialStore credentialStore, String username, String password) {
        ScramHandler handler = new ScramHandler(ScramMechanism.SCRAM_SHA_256, credentialStore, Clock.systemUTC());
        try {
            SaslClient client = Sasl.createSaslClient(
                    new String[]{ "SCRAM-SHA-256" },
                    null,
                    "kafka",
                    null,
                    Map.of(),
                    callbacks -> {
                        for (Callback cb : callbacks) {
                            if (cb instanceof NameCallback nc) {
                                nc.setName(username);
                            }
                            else if (cb instanceof PasswordCallback pc) {
                                pc.setPassword(password.toCharArray());
                            }
                        }
                    });

            byte[] clientFirst = client.evaluateChallenge(new byte[0]);

            long startNanos = System.nanoTime();
            handler.handleAuthenticate(clientFirst)
                    .toCompletableFuture().get();
            long elapsedNanos = System.nanoTime() - startNanos;

            client.dispose();
            return elapsedNanos / 1_000_000.0;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            handler.dispose();
        }
    }
}
