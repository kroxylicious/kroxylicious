/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;

/**
 * Service that monitors a configuration file for changes and triggers hot-reload when detected.
 * Uses Java NIO WatchService for efficient file system monitoring.
 */
public class ConfigWatcherService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigWatcherService.class);

    private final Path configFilePath;
    private final ConfigParser configParser;
    private final Consumer<Configuration> onConfigurationChanged;
    private final Duration debounceDelay;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService watcherExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "config-watcher");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService reloadExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "config-reload");
        t.setDaemon(true);
        return t;
    });

    private WatchService watchService;
    private WatchKey watchKey;
    private Configuration currentConfiguration;
    private Instant lastModified = Instant.EPOCH;

    /**
     * Creates a new ConfigWatcherService.
     *
     * @param configFilePath the path to the configuration file to watch
     * @param pluginFactoryRegistry the plugin factory registry for parsing
     * @param onConfigurationChanged callback invoked when configuration changes
     * @param debounceDelay delay to debounce rapid file changes
     */
    public ConfigWatcherService(Path configFilePath,
                                Consumer<Configuration> onConfigurationChanged,
                                Duration debounceDelay) {
        this.configFilePath = configFilePath.toAbsolutePath();
        this.configParser = new ConfigParser();
        this.onConfigurationChanged = onConfigurationChanged;
        this.debounceDelay = debounceDelay;

        // Load initial configuration
        try {
            this.currentConfiguration = loadConfiguration();
            LOGGER.info("Loaded initial configuration from: {}", this.configFilePath);
        }
        catch (Exception e) {
            LOGGER.error("Failed to load initial configuration from: {}", this.configFilePath, e);
            throw new IllegalStateException("Cannot start ConfigWatcherService without valid initial configuration", e);
        }
    }

    /**
     * Starts watching the configuration file for changes.
     *
     * @return CompletableFuture that completes when the watcher is started
     */
    public CompletableFuture<Void> start() {
        if (running.getAndSet(true)) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.runAsync(() -> {
            try {
                initializeWatchService();
                LOGGER.info("Started watching configuration file: {}", configFilePath);
                watchForChanges();
            }
            catch (Exception e) {
                LOGGER.error("Failed to start configuration file watcher", e);
                running.set(false);
                throw new RuntimeException("Failed to start ConfigWatcherService", e);
            }
        }, watcherExecutor);
    }

    /**
     * Stops watching the configuration file.
     *
     * @return CompletableFuture that completes when the watcher is stopped
     */
    public CompletableFuture<Void> stop() {
        if (!running.getAndSet(false)) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.runAsync(() -> {
            try {
                if (watchKey != null) {
                    watchKey.cancel();
                }
                if (watchService != null) {
                    watchService.close();
                }
                watcherExecutor.shutdown();
                reloadExecutor.shutdown();
                if (!watcherExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    watcherExecutor.shutdownNow();
                }
                if (!reloadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    reloadExecutor.shutdownNow();
                }
                LOGGER.info("Stopped watching configuration file: {}", configFilePath);
            }
            catch (Exception e) {
                LOGGER.error("Error stopping configuration file watcher", e);
            }
        });
    }

    private void initializeWatchService() throws IOException {
        this.watchService = FileSystems.getDefault().newWatchService();

        Path parentDir = configFilePath.getParent();
        if (parentDir == null) {
            throw new IllegalArgumentException("Configuration file must have a parent directory: " + configFilePath);
        }

        LOGGER.info("Initializing watch service for directory: {}", parentDir);
        LOGGER.info("Watching for changes to file: {}", configFilePath.getFileName());

        // Watch for MODIFY, CREATE, and DELETE events on the parent directory
        this.watchKey = parentDir.register(watchService,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE);

        LOGGER.info("Successfully initialized watch service for directory: {}", parentDir);
    }

    private void watchForChanges() {
        while (running.get()) {
            try {
                // Wait for events (blocking call)
                WatchKey key = watchService.take();

                if (key != watchKey) {
                    continue; // Not our key
                }

                boolean configFileChanged = false;

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    // Skip overflow events
                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        LOGGER.warn("Watch event overflow - some events may have been lost");
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                    Path changedFile = pathEvent.context();

                    // Check if the changed file is our configuration file
                    if (configFilePath.getFileName().equals(changedFile)) {
                        LOGGER.debug("Configuration file {} changed: {}", changedFile, kind);

                        // We care about MODIFY and CREATE events (CREATE happens when editors create new files)
                        if (kind == StandardWatchEventKinds.ENTRY_MODIFY || kind == StandardWatchEventKinds.ENTRY_CREATE) {
                            configFileChanged = true;
                        }
                    }
                }

                // Reset the key for future events
                boolean valid = key.reset();
                if (!valid) {
                    LOGGER.warn("Watch key is no longer valid - stopping watcher");
                    break;
                }

                if (configFileChanged) {
                    scheduleConfigurationReload();
                }

            }
            catch (InterruptedException e) {
                LOGGER.warn("Configuration watcher interrupted");
                Thread.currentThread().interrupt();
                break;
            }
            catch (Exception e) {
                LOGGER.error("Error in configuration watcher", e);
                // Continue watching despite errors
            }
        }
    }

    private volatile java.util.concurrent.ScheduledFuture<?> pendingReload;

    private void scheduleConfigurationReload() {

        if (reloadExecutor.isShutdown()) {
            LOGGER.error("Reload executor is shut down! Cannot schedule configuration reload");
            return;
        }

        // Cancel any pending reload to debounce rapid file changes
        if (pendingReload != null && !pendingReload.isDone()) {
            pendingReload.cancel(false);
        }

        // Schedule new reload
        try {
            pendingReload = reloadExecutor.schedule(() -> {
                try {
                    handleConfigurationChange();
                }
                catch (Exception e) {
                    LOGGER.error("Failed to handle configuration change", e);
                }
            }, debounceDelay.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (Exception e) {
            LOGGER.error("Failed to schedule configuration reload", e);
        }
    }

    private void handleConfigurationChange() {
        try {
            LOGGER.debug("Processing configuration file change: {}", configFilePath);

            // Check if file exists and is readable
            if (!Files.exists(configFilePath)) {
                LOGGER.warn("Configuration file no longer exists: {}", configFilePath);
                return;
            }

            // Check if file was actually modified (avoid duplicate reloads)
            Instant fileModified = Files.getLastModifiedTime(configFilePath).toInstant();
            LOGGER.debug("File modification time: {}, last processed: {}", fileModified, lastModified);

            if (!fileModified.isAfter(lastModified)) {
                LOGGER.debug("Configuration file timestamp unchanged - skipping reload");
                return;
            }

            LOGGER.info("Configuration file changed, loading new configuration: {}", configFilePath);

            Configuration newConfiguration = loadConfiguration();

            // Only trigger callback if configuration actually changed
            if (!configurationEquals(currentConfiguration, newConfiguration)) {
                this.currentConfiguration = newConfiguration;
                this.lastModified = fileModified;

                try {
                    onConfigurationChanged.accept(newConfiguration);
                }
                catch (Exception callbackError) {
                    LOGGER.error("Error in configuration change callback", callbackError);
                    // Don't re-throw - we want to continue watching
                }
            }
            else {
                LOGGER.info("Configuration content unchanged - no reload needed");
                this.lastModified = fileModified;
            }

        }
        catch (Exception e) {
            LOGGER.error("Failed to reload configuration from: {}", configFilePath, e);
            // Don't update currentConfiguration on error - keep the working one
        }
    }

    private Configuration loadConfiguration() throws IOException {
        if (!Files.exists(configFilePath)) {
            throw new IOException("Configuration file does not exist: " + configFilePath);
        }

        if (!Files.isReadable(configFilePath)) {
            throw new IOException("Configuration file is not readable: " + configFilePath);
        }

        String configContent = Files.readString(configFilePath);
        return configParser.parseConfiguration(configContent);
    }

    private boolean configurationEquals(Configuration config1, Configuration config2) {
        // Simple comparison using YAML serialization
        // In a production system, you might want a more sophisticated comparison
        try {
            String yaml1 = configParser.toYaml(config1);
            String yaml2 = configParser.toYaml(config2);
            return yaml1.equals(yaml2);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to compare configurations, assuming they are different", e);
            return false;
        }
    }
}
