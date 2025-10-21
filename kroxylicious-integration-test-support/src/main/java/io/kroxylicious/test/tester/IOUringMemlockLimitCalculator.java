/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

// Mostly based on io_uring_mlock_size() from https://github.com/axboe/liburing
public final class IOUringMemlockLimitCalculator {
    // https://en.wikipedia.org/wiki/Page_%28computer_memory%29#Multiple_page_sizes
    // https://github.com/search?q=repo%3Aaxboe%2Fliburing%20%22get_page_size(void)%22&type=code
    // https://source.android.com/docs/core/architecture/16kb-page-size/getting-page-size
    static final int DEFAULT_PAGE_SIZE = 4096;

    // https://github.com/systemd/systemd/blob/v258.1/src/core/system.conf.in#L71
    static final int DEFAULT_MEMLOCK_LIMIT = 8388608;

    // https://github.com/axboe/liburing/blob/liburing-2.12/src/setup.c#L210
    static final int KRING_SIZE = 64;

    // https://github.com/axboe/liburing/blob/liburing-2.12/man/io_uring_setup.2#L270
    static final int DEFAULT_SQE_SIZE = 64;

    // https://github.com/axboe/liburing/blob/liburing-2.12/man/io_uring_setup.2#L277
    static final int DEFAULT_CQE_SIZE = 16;

    // https://github.com/netty/netty-incubator-transport-io_uring/blob/netty-incubator-transport-parent-io_uring-0.0.26.Final/transport-classes-io_uring/src/main/java/io/netty/incubator/channel/uring/Native.java#L42
    static final int DEFAULT_NETTY_ENTRIES_COUNT = 4096;

    static final int PROCESS_TIMEOUT_SECONDS = 10;

    private static int currentMemlockLimit = -1;
    private static int currentPageSize = -1;

    private static void getCurrentPageSize() {
        if (currentPageSize != -1) {
            return;
        }

        try {
            ProcessBuilder getconfProcessBuilder = new ProcessBuilder("getconf", "PAGE_SIZE");
            Process getconfProcess = getconfProcessBuilder.start();
            boolean exited = getconfProcess.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!exited) {
                throw new RuntimeException("getconf process timed out");
            }
            if (getconfProcess.exitValue() != 0) {
                throw new RuntimeException("getconf process exited with value: " + getconfProcess.exitValue());
            }

            BufferedReader getconfOutputReader = new BufferedReader(new InputStreamReader(getconfProcess.getInputStream()));
            String getconfOutputLine = getconfOutputReader.readLine();

            currentPageSize = Integer.parseInt(getconfOutputLine);
        }
        catch (Exception e) {
            currentPageSize = DEFAULT_PAGE_SIZE;
        }
    }

    private static void getCurrentMemlockLimit() {
        if (currentMemlockLimit != -1) {
            return;
        }

        try {
            ProcessBuilder ulimitProcessBuilder = new ProcessBuilder("ulimit", "-H", "-l");
            Process ulimitProcess = ulimitProcessBuilder.start();
            boolean exited = ulimitProcess.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!exited) {
                throw new RuntimeException("ulimit process timed out");
            }
            if (ulimitProcess.exitValue() != 0) {
                throw new RuntimeException("ulimit process exited with value: " + ulimitProcess.exitValue());
            }

            BufferedReader ulimitOutputReader = new BufferedReader(new InputStreamReader(ulimitProcess.getInputStream()));
            String ulimitOutputLine = ulimitOutputReader.readLine();

            currentMemlockLimit = Integer.parseInt(ulimitOutputLine);
        }
        catch (Exception e) {
            currentMemlockLimit = DEFAULT_MEMLOCK_LIMIT;
        }
    }

    // https://github.com/axboe/liburing/blob/liburing-2.12/src/setup.c#L595
    private static int calculateRequiredMemlockLimitForOneIOUringEventLoop(int entries, int sqeSize, int cqeSize) {
        // https://github.com/axboe/liburing/blob/liburing-2.12/src/setup.c#L57
        // https://github.com/axboe/liburing/blob/liburing-2.12/src/setup.c#L100
        int paramsSqesSize = entries * sqeSize;

        // https://github.com/axboe/liburing/blob/liburing-2.12/src/setup.c#L54
        int cqEntries = 2 * entries;

        // https://github.com/axboe/liburing/blob/liburing-2.12/src/setup.c#L107
        int paramsCqSize = cqEntries * cqeSize;

        getCurrentPageSize();

        // https://github.com/axboe/liburing/blob/liburing-2.12/src/setup.c#L513-L516
        int cqSize = paramsCqSize + KRING_SIZE;
        cqSize = (int) (Math.ceil((double) cqSize / currentPageSize) * currentPageSize);
        int pages = cqSize / currentPageSize;

        // https://github.com/axboe/liburing/blob/liburing-2.12/src/setup.c#L518-L520
        int sqSize = paramsSqesSize;
        sqSize = (int) (Math.ceil((double) sqSize / currentPageSize) * currentPageSize);
        pages += sqSize / currentPageSize;

        return pages * currentPageSize;
    }

    public static boolean isMemlockLimitSufficientForMultipleIOUringEventLoops(int eventLoopCount) {
        return isMemlockLimitSufficientForMultipleIOUringEventLoops(eventLoopCount, DEFAULT_NETTY_ENTRIES_COUNT, DEFAULT_SQE_SIZE, DEFAULT_CQE_SIZE);
    }

    public static boolean isMemlockLimitSufficientForMultipleIOUringEventLoops(int eventLoopCount, int entries, int sqeSize, int cqeSize) {
        getCurrentMemlockLimit();

        int requiredMemlockLimitForOneIOUringEventLoop = calculateRequiredMemlockLimitForOneIOUringEventLoop(entries, sqeSize, cqeSize);
        int requiredMemlockLimitForMultipleIOUringEventLoops = eventLoopCount * requiredMemlockLimitForOneIOUringEventLoop;

        return requiredMemlockLimitForMultipleIOUringEventLoops <= currentMemlockLimit;
    }
}
