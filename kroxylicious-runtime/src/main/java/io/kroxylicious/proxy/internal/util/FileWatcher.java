/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.tag.ThreadSafe;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * Watches a directory for file changes and notifies registered listeners.
 * <p>
 *  The failure to watch a file is not always a terminal condition and this utility class has been designed to eliminate the need
 *  to catch multiple exceptions. If a component needs to know if it is working as intended and notifications of file changes will be
 *  received then {@link #isRunning()} can be checked.
 *
 *  The underlying watch service can raise multiple change events but has no concept of when the operation changing the file has finished.
 *  Registered notifiers <b>must</b> check the validity of the file being watched before processing it. For example, a file overwrite may
 *  consist of an initial zero byte truncation followed by writing the new content. Change events can be created for the initial truncation and
 *  periodically during the file writing as it is changing. This can be an issue where there is a slow file system.
 * </p>
 * <p>
 *  Known limitations :
 *  </p>
 *
 *  <ul>
 *  <li>The underlying watch service only returns relative paths, which means that handlers for the same file name in different directories will all be called even if the file a give handler is interested in has not changed. </li>
 *  <li>The file contents are not tracked, so notifications will be received even if the file is updated with the same content</li>
 *  </ul>
 *
 */
@ThreadSafe
public class FileWatcher implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileWatcher.class);

    /**
     * Invoked by the watcher to notify registered listeners of a file change
     */
    @FunctionalInterface
    public interface NotificationHandler {
        /**
         * Method invoked by the watcher when a file changes.
         */
        void onChange();
    }

    private final Map<Path, List<NotificationHandler>> watchList = new ConcurrentHashMap<>();
    private final Set<Path> watchedDirectories = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(Thread.ofVirtual().name("file-watcher-", 0).factory());

    private final Optional<WatchService> service;
    private final AtomicBoolean started = new AtomicBoolean(false); // true if an attempt has been made to start the watcher thread, prevents spawning multiple threads
    private final AtomicBoolean running = new AtomicBoolean(false); // true if the watcher is running without error, allows the caller to determine the severity of a failure to watch a file

    /**
     * Create a new instance of a file watcher
     */
    public FileWatcher() {
        WatchService watchService = null;
        try {
            watchService = FileSystems.getDefault().newWatchService();
        }
        catch (final Exception e) {
            LOGGER.atWarn().setCause(e).log("Error initialising file watcher, changes will not be detected");
        }
        service = Optional.ofNullable(watchService);
    }

    /**
     * Register to receive notifications when the contents of a file is changed or it is replaced.
     *
     * @param path path to the file. The path must include the parent directory but does not have to be an absolute path.
     * @param listener the handler to notify of a change
     * @return this watcher
     */
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE") // spotbugs does not recognise that path.getParent() is already checked for null before it is used
    public FileWatcher register(final Path path, final NotificationHandler listener) {
        if (path.toFile().isDirectory()) {
            throw new IllegalArgumentException("Watcher requires a file, a directory was specified " + path);
        }

        if (Objects.isNull(path.getParent())) {
            throw new IllegalArgumentException("Watched files require a parent directory, only a filename was supplied " + path);
        }

        final Path key = path.subpath(path.getNameCount() - 1, path.getNameCount());
        final List<NotificationHandler> watcher = watchList.computeIfAbsent(key, k -> new ArrayList<>());
        watcher.add(listener);

        // register the directory to watch only if it's not already being watched
        final var pathToRegister = path.getParent().toAbsolutePath();
        if (watchedDirectories.add(pathToRegister) && service.isPresent()) {
            try {
                pathToRegister.register(service.get(), ENTRY_MODIFY, ENTRY_CREATE); // have to watch the parent dir for create events to detect symlink swaps in K8s as well as direct file content
                // changes
            }
            catch (final IOException e) { // if the registration fails then just log the warning
                LOGGER.atWarn().setCause(e).addKeyValue("path", path).log("Failed to register watcher for path");
            }
        }
        return this;
    }

    /**
     * Get the running state of the watcher
     * @return True if the watcher is running and monitoring for file changes, false if it has not been started or failed due to an error.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Start watching for file changes. Files can be added to the watcher with {@link #start()} after start has been called.
     * Calling start multiple times has no effect.
     *
     * @return this watcher
     */
    public FileWatcher start() {
        if (service.isEmpty() || !started.compareAndSet(false, true)) {
            return this; // service failed to be created or is already started, so do nothing
        }

        executor.execute(() -> {
            running.set(true);
            while (running.get()) {
                try {
                    final var key = service.get().take(); // blocks if there are no notifications to process
                    final var handlers = key.pollEvents().stream()
                            .flatMap(event -> event.context() instanceof Path p ? Stream.of(p) : Stream.empty()) // check the context is not null and refers to a path
                            .filter(watchList::containsKey) // see if we are watching the path
                            .flatMap(relativePath -> watchList.get(relativePath).stream()) // extract all the listeners that need to be notified
                            .toList();
                    handlers.forEach(handler -> {
                        try {
                            handler.onChange();
                        }
                        catch (final Exception e) { // multiple handlers can be regsiterd aginst the same watcher so make sure they all get notified
                            LOGGER.atWarn().setCause(e).log("Handler threw an exception");
                        }
                    });
                    key.reset(); // all events processed, so reset the key which puts it back in the wait state
                }
                catch (final InterruptedException | ClosedWatchServiceException ignored) {
                    // These are expected when either the thread is being termintaed and/or the watcher service is being closed
                    running.set(false);
                    if (ignored instanceof InterruptedException) {
                        // the thread executor pool owns the thread so need to make sure it sees the interrupt status
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });

        return this;
    }

    /**
     * Close the watcher and release any resources
     */
    @Override
    public void close() {
        if (service.isPresent()) {
            try {
                service.get().close(); // this will cause any poll/take to throw an exception and the runnable task to exit
            }
            catch (final IOException ignored) {
                // ignore as the service is not going to be used again
            }
        }

        executor.shutdown();
    }

}
