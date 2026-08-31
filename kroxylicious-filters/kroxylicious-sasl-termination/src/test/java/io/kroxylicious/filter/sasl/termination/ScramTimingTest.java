/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramSaslClientProvider;
import org.apache.kafka.common.security.scram.internals.ScramSaslServerProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.netty.channel.DefaultEventLoop;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.internal.NettyFilterDispatchExecutor;
import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramCredentialStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that authentication timing is uniform regardless of whether the
 * user exists or the password is correct. This verifies the timing
 * side-channel mitigation applied by {@link SaslTerminationFilter}.
 * <p>
 * The test runs increasing batches in parallel and checks that the means
 * of the three scenarios (success, unknown user, wrong password) converge
 * — i.e. the absolute spread between them shrinks below a tight millisecond
 * threshold — and that their standard deviations are within a bounded ratio
 * of each other (no scenario has a distinctively different noise profile).
 * If there were a real timing difference the means would diverge, not converge.
 * <p>
 * Each worker measures the three scenarios in a rotating order (rather than
 * always success, then unknown, then wrong password) so that any latency
 * introduced by the harness itself (e.g. thread scheduling, queuing on a
 * shared timer) can't correlate with scenario identity and masquerade as a
 * real timing difference.
 */
class ScramTimingTest {

    private static final String VALID_USERNAME = "alice";
    private static final String VALID_PASSWORD = "alice-secret-password";
    private static final String WRONG_PASSWORD = "wrong-password-12345";
    private static final String UNKNOWN_USERNAME = "unknown-user";

    private record Scenario(String label, String username, String password) {}

    private static final Scenario SCENARIO_SUCCESS = new Scenario("success", VALID_USERNAME, VALID_PASSWORD);
    private static final Scenario SCENARIO_UNKNOWN = new Scenario("unknown", UNKNOWN_USERNAME, WRONG_PASSWORD);
    private static final Scenario SCENARIO_WRONG_PASSWORD = new Scenario("wrongPw", VALID_USERNAME, WRONG_PASSWORD);
    private static final List<Scenario> SCENARIOS = List.of(SCENARIO_SUCCESS, SCENARIO_UNKNOWN, SCENARIO_WRONG_PASSWORD);

    private static final int THREADS = 8;
    private static final int BATCH_SIZE = 50;
    private static final int MAX_BATCHES = 20;
    private static final double MEAN_ABSOLUTE_SPREAD_THRESHOLD_MS = 1.0;
    private static final double STDDEV_RATIO_MAX = 5.0;

    private static List<Scenario> rotatedScenarios(int iteration) {
        List<Scenario> rotated = new ArrayList<>(SCENARIOS);
        Collections.rotate(rotated, iteration % SCENARIOS.size());
        return rotated;
    }

    @BeforeAll
    static void registerProviders() {
        ScramSaslServerProvider.initialize();
        ScramSaslClientProvider.initialize();
    }

    @AfterAll
    static void shutdownExecutor() throws Exception {
        for (DefaultEventLoop loop : TIMING_EVENT_LOOPS) {
            loop.shutdownGracefully().sync();
        }
    }

    @Test
    void authenticationTimingShouldConvergeAcrossScenarios() throws Exception {
        // Given
        ScramCredential credential = TestCredentialHelper.generateCredential(
                VALID_USERNAME, VALID_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        ScramCredentialStore credentialStore = testCredentialStore(credential);

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<?>> warmup = new ArrayList<>();
            for (int i = 0; i < THREADS; i++) {
                warmup.add(executor.submit(() -> {
                    for (int j = 0; j < 3; j++) {
                        for (Scenario scenario : rotatedScenarios(j)) {
                            timeFirstRound(credentialStore, scenario.username(), scenario.password());
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : warmup) {
                f.get();
            }

            // When
            RunningStats successTimes = new RunningStats();
            RunningStats unknownUserTimes = new RunningStats();
            RunningStats wrongPasswordTimes = new RunningStats();
            List<String> batchLog = new ArrayList<>();

            boolean converged = false;
            for (int batch = 0; batch < MAX_BATCHES; batch++) {
                List<Future<Map<String, Double>>> futures = new ArrayList<>();
                for (int i = 0; i < BATCH_SIZE; i++) {
                    int iteration = batch * BATCH_SIZE + i;
                    futures.add(executor.submit(() -> {
                        Map<String, Double> times = new HashMap<>();
                        for (Scenario scenario : rotatedScenarios(iteration)) {
                            times.put(scenario.label(), timeFirstRound(credentialStore, scenario.username(), scenario.password()));
                        }
                        return times;
                    }));
                }

                for (Future<Map<String, Double>> f : futures) {
                    Map<String, Double> times = f.get();
                    successTimes.accept(times.get(SCENARIO_SUCCESS.label()));
                    unknownUserTimes.accept(times.get(SCENARIO_UNKNOWN.label()));
                    wrongPasswordTimes.accept(times.get(SCENARIO_WRONG_PASSWORD.label()));
                }

                double meanSpread = absoluteSpread(
                        successTimes.mean(),
                        unknownUserTimes.mean(),
                        wrongPasswordTimes.mean());

                double maxStdDev = Math.max(successTimes.stdDev(), Math.max(unknownUserTimes.stdDev(), wrongPasswordTimes.stdDev()));
                double minStdDev = Math.min(successTimes.stdDev(), Math.min(unknownUserTimes.stdDev(), wrongPasswordTimes.stdDev()));
                boolean stdDevsConsistent = minStdDev > 0 && (maxStdDev / minStdDev) < STDDEV_RATIO_MAX;

                batchLog.add(String.format(
                        "batch %d: spread=%.3fms stddevRatio=%.2fx (success=%.2f/%.2f, unknown=%.2f/%.2f, wrongPw=%.2f/%.2f)",
                        batch, meanSpread, minStdDev > 0 ? maxStdDev / minStdDev : Double.NaN,
                        successTimes.mean(), successTimes.stdDev(),
                        unknownUserTimes.mean(), unknownUserTimes.stdDev(),
                        wrongPasswordTimes.mean(), wrongPasswordTimes.stdDev()));

                if (meanSpread < MEAN_ABSOLUTE_SPREAD_THRESHOLD_MS && stdDevsConsistent) {
                    converged = true;
                    break;
                }
            }

            // Then
            assertThat(converged)
                    .as("means should converge (<%.2fms absolute spread) and std dev ratio should be low (<%.1fx) within %d iterations " +
                            "(success: mean=%.1fms stddev=%.2fms, unknown: mean=%.1fms stddev=%.2fms, wrongPw: mean=%.1fms stddev=%.2fms)%nBatch history:%n%s",
                            MEAN_ABSOLUTE_SPREAD_THRESHOLD_MS, STDDEV_RATIO_MAX,
                            successTimes.count(),
                            successTimes.mean(), successTimes.stdDev(),
                            unknownUserTimes.mean(), unknownUserTimes.stdDev(),
                            wrongPasswordTimes.mean(), wrongPasswordTimes.stdDev(),
                            String.join("\n", batchLog))
                    .isTrue();
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    void secondRoundTimingShouldConvergeAcrossScenarios() throws Exception {
        // Given
        ScramCredential credential = TestCredentialHelper.generateCredential(
                VALID_USERNAME, VALID_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        ScramCredentialStore credentialStore = testCredentialStore(credential);

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<?>> warmup = new ArrayList<>();
            for (int i = 0; i < THREADS; i++) {
                warmup.add(executor.submit(() -> {
                    for (int j = 0; j < 3; j++) {
                        for (Scenario scenario : rotatedScenarios(j)) {
                            timeSecondRound(credentialStore, scenario.username(), scenario.password());
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : warmup) {
                f.get();
            }

            // When
            RunningStats successTimes = new RunningStats();
            RunningStats unknownUserTimes = new RunningStats();
            RunningStats wrongPasswordTimes = new RunningStats();
            List<String> batchLog = new ArrayList<>();

            boolean converged = false;
            for (int batch = 0; batch < MAX_BATCHES; batch++) {
                List<Future<Map<String, Double>>> futures = new ArrayList<>();
                for (int i = 0; i < BATCH_SIZE; i++) {
                    int iteration = batch * BATCH_SIZE + i;
                    futures.add(executor.submit(() -> {
                        Map<String, Double> times = new HashMap<>();
                        for (Scenario scenario : rotatedScenarios(iteration)) {
                            times.put(scenario.label(), timeSecondRound(credentialStore, scenario.username(), scenario.password()));
                        }
                        return times;
                    }));
                }

                for (Future<Map<String, Double>> f : futures) {
                    Map<String, Double> times = f.get();
                    successTimes.accept(times.get(SCENARIO_SUCCESS.label()));
                    unknownUserTimes.accept(times.get(SCENARIO_UNKNOWN.label()));
                    wrongPasswordTimes.accept(times.get(SCENARIO_WRONG_PASSWORD.label()));
                }

                double meanSpread = absoluteSpread(
                        successTimes.mean(),
                        unknownUserTimes.mean(),
                        wrongPasswordTimes.mean());

                double maxStdDev = Math.max(successTimes.stdDev(), Math.max(unknownUserTimes.stdDev(), wrongPasswordTimes.stdDev()));
                double minStdDev = Math.min(successTimes.stdDev(), Math.min(unknownUserTimes.stdDev(), wrongPasswordTimes.stdDev()));
                boolean stdDevsConsistent = minStdDev > 0 && (maxStdDev / minStdDev) < STDDEV_RATIO_MAX;

                batchLog.add(String.format(
                        "batch %d: spread=%.3fms stddevRatio=%.2fx (success=%.2f/%.2f, unknown=%.2f/%.2f, wrongPw=%.2f/%.2f)",
                        batch, meanSpread, minStdDev > 0 ? maxStdDev / minStdDev : Double.NaN,
                        successTimes.mean(), successTimes.stdDev(),
                        unknownUserTimes.mean(), unknownUserTimes.stdDev(),
                        wrongPasswordTimes.mean(), wrongPasswordTimes.stdDev()));

                if (meanSpread < MEAN_ABSOLUTE_SPREAD_THRESHOLD_MS && stdDevsConsistent) {
                    converged = true;
                    break;
                }
            }

            // Then
            assertThat(converged)
                    .as("second round: means should converge (<%.2fms absolute spread) and std dev ratio should be low (<%.1fx) within %d iterations " +
                            "(success: mean=%.1fms stddev=%.2fms, unknown: mean=%.1fms stddev=%.2fms, wrongPw: mean=%.1fms stddev=%.2fms)%nBatch history:%n%s",
                            MEAN_ABSOLUTE_SPREAD_THRESHOLD_MS, STDDEV_RATIO_MAX,
                            successTimes.count(),
                            successTimes.mean(), successTimes.stdDev(),
                            unknownUserTimes.mean(), unknownUserTimes.stdDev(),
                            wrongPasswordTimes.mean(), wrongPasswordTimes.stdDev(),
                            String.join("\n", batchLog))
                    .isTrue();
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static double absoluteSpread(double a, double b, double c) {
        double max = Math.max(a, Math.max(b, c));
        double min = Math.min(a, Math.min(b, c));
        return max - min;
    }

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

    private static final byte[] TEST_PHANTOM_SALT_KEY = new byte[32];

    // Each worker thread gets its own dedicated event loop so that concurrent measurement
    // threads never queue behind one another when their fixed auth delays complete, which
    // would otherwise bias whichever scenario happens to be measured first in the sequence.
    private static final Queue<DefaultEventLoop> TIMING_EVENT_LOOPS = new ConcurrentLinkedQueue<>();
    private static final ThreadLocal<FilterDispatchExecutor> TIMING_EXECUTOR = ThreadLocal.withInitial(() -> {
        DefaultEventLoop loop = new DefaultEventLoop();
        TIMING_EVENT_LOOPS.add(loop);
        return NettyFilterDispatchExecutor.eventLoopExecutor(loop);
    });

    private static ScramCredentialStore testCredentialStore(ScramCredential credential) {
        return new ScramCredentialStore() {
            @Override
            public CompletionStage<ScramCredential> lookupCredential(String username) {
                return CompletableFuture.completedFuture(VALID_USERNAME.equals(username) ? credential : null);
            }

            @Override
            public byte[] phantomSaltKey() {
                return TEST_PHANTOM_SALT_KEY.clone();
            }
        };
    }

    private double timeFirstRound(ScramCredentialStore credentialStore, String username, String password) {
        var context = new SaslTermination.SaslTerminationContext(
                null,
                OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES,
                Map.of(ScramMechanism.SCRAM_SHA_256, credentialStore),
                Map.of(ScramMechanism.SCRAM_SHA_256, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS),
                Set.of("SCRAM-SHA-256"),
                List.of(),
                null,
                Clock.systemUTC(),
                Duration.ofMillis(100),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(TIMING_EXECUTOR.get(), context);

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

            // Handshake
            var handshakeRequest = new SaslHandshakeRequestData().setMechanism("SCRAM-SHA-256");
            filter.onRequest(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(),
                    new RequestHeaderData(), handshakeRequest, mockFilterContext())
                    .toCompletableFuture().get();

            // Authenticate (timed)
            byte[] clientFirst = client.evaluateChallenge(new byte[0]);
            var authRequest = new SaslAuthenticateRequestData().setAuthBytes(clientFirst);

            long startNanos = System.nanoTime();
            filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                    new RequestHeaderData(), authRequest, mockFilterContext())
                    .toCompletableFuture().get();
            long elapsedNanos = System.nanoTime() - startNanos;

            client.dispose();
            return elapsedNanos / 1_000_000.0;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private double timeSecondRound(ScramCredentialStore credentialStore, String username, String password) {
        var context = new SaslTermination.SaslTerminationContext(
                null,
                OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES,
                Map.of(ScramMechanism.SCRAM_SHA_256, credentialStore),
                Map.of(ScramMechanism.SCRAM_SHA_256, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS),
                Set.of("SCRAM-SHA-256"),
                List.of(),
                null,
                Clock.systemUTC(),
                Duration.ofMillis(100),
                SaslTermination.DEFAULT_SUBJECT_BUILDER);
        var filter = new SaslTerminationFilter(TIMING_EXECUTOR.get(), context);

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

            // Handshake
            var handshakeRequest = new SaslHandshakeRequestData().setMechanism("SCRAM-SHA-256");
            filter.onRequest(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(),
                    new RequestHeaderData(), handshakeRequest, mockFilterContext())
                    .toCompletableFuture().get();

            // First authenticate round (not timed)
            byte[] clientFirst = client.evaluateChallenge(new byte[0]);
            var firstAuthRequest = new SaslAuthenticateRequestData().setAuthBytes(clientFirst);
            ArgumentCaptor<ApiMessage> firstRoundCaptor = ArgumentCaptor.forClass(ApiMessage.class);
            FilterContext firstRoundCtx = mockFilterContextWithCapture(firstRoundCaptor);
            filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                    new RequestHeaderData(), firstAuthRequest, firstRoundCtx)
                    .toCompletableFuture().get();

            // Extract server challenge and compute client-final-message
            SaslAuthenticateResponseData firstResponse = (SaslAuthenticateResponseData) firstRoundCaptor.getValue();
            byte[] clientFinal = client.evaluateChallenge(firstResponse.authBytes());

            // Second authenticate round (timed)
            var secondAuthRequest = new SaslAuthenticateRequestData().setAuthBytes(clientFinal);

            long startNanos = System.nanoTime();
            filter.onRequest(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                    new RequestHeaderData(), secondAuthRequest, mockFilterContext())
                    .toCompletableFuture().get();
            long elapsedNanos = System.nanoTime() - startNanos;

            client.dispose();
            return elapsedNanos / 1_000_000.0;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockFilterContext() {
        return buildMockFilterContext(null);
    }

    @SuppressWarnings("unchecked")
    private static FilterContext mockFilterContextWithCapture(ArgumentCaptor<ApiMessage> captor) {
        return buildMockFilterContext(captor);
    }

    @SuppressWarnings("unchecked")
    private static FilterContext buildMockFilterContext(@edu.umd.cs.findbugs.annotations.Nullable ArgumentCaptor<ApiMessage> captor) {
        var filterContext = mock(FilterContext.class);
        var builder = mock(RequestFilterResultBuilder.class);
        var closeOrTerminal = mock(io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage.class);
        var result = mock(RequestFilterResult.class);

        when(filterContext.requestFilterResultBuilder()).thenReturn(builder);
        if (captor != null) {
            when(builder.shortCircuitResponse(captor.capture())).thenReturn(closeOrTerminal);
        }
        else {
            when(builder.shortCircuitResponse(any())).thenReturn(closeOrTerminal);
        }
        when(closeOrTerminal.withCloseConnection()).thenReturn(closeOrTerminal);
        when(closeOrTerminal.completed()).thenReturn(CompletableFuture.completedFuture(result));

        when(filterContext.sessionId()).thenReturn("test-session");
        when(filterContext.getVirtualClusterName()).thenReturn("test-cluster");

        return filterContext;
    }
}
