/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationChangeResult;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.OnFailure;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.ReloadOptions;
import io.kroxylicious.proxy.internal.ConfigurationChangeContext;
import io.kroxylicious.proxy.internal.ConfigurationChangeHandler;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Orchestrates configuration reload operations with concurrency control,
 * validation, and state tracking. This class coordinates the entire reload
 * workflow using the Template Method pattern.
 * <p>
 * Thread-safe implementation using ReentrantLock to prevent concurrent reloads.
 */
public class ConfigurationReloadOrchestrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationReloadOrchestrator.class);

    private final ConfigurationChangeHandler configurationChangeHandler;
    private final PluginFactoryRegistry pluginFactoryRegistry;
    private final Features features;
    private final ReloadStateManager stateManager;
    private final ReentrantLock reloadLock;

    // We need access to current configuration to create ConfigurationChangeContext
    private Configuration currentConfiguration;
    private final @Nullable Path configFilePath;

    // Shared mutable reference to FilterChainFactory - enables atomic swaps during hot reload
    private final AtomicReference<FilterChainFactory> filterChainFactoryRef;

    public ConfigurationReloadOrchestrator(
                                           Configuration initialConfiguration,
                                           ConfigurationChangeHandler configurationChangeHandler,
                                           PluginFactoryRegistry pluginFactoryRegistry,
                                           Features features,
                                           @Nullable Path configFilePath,
                                           AtomicReference<FilterChainFactory> filterChainFactoryRef) {
        this.currentConfiguration = Objects.requireNonNull(initialConfiguration, "initialConfiguration cannot be null");
        this.configurationChangeHandler = Objects.requireNonNull(configurationChangeHandler, "configurationChangeHandler cannot be null");
        this.pluginFactoryRegistry = Objects.requireNonNull(pluginFactoryRegistry, "pluginFactoryRegistry cannot be null");
        this.features = Objects.requireNonNull(features, "features cannot be null");
        this.filterChainFactoryRef = Objects.requireNonNull(filterChainFactoryRef, "filterChainFactoryRef cannot be null");
        this.configFilePath = configFilePath;
        this.stateManager = new ReloadStateManager();
        this.reloadLock = new ReentrantLock();
    }

    /**
     * Reload configuration with default reload options.
     * Backward-compatible overload that delegates to {@link #reload(Configuration, ReloadOptions)}.
     *
     * @param newConfig The new configuration to apply
     * @return CompletableFuture with reload result
     */
    public CompletableFuture<ReloadResult> reload(Configuration newConfig) {
        return reload(newConfig, ReloadOptions.DEFAULT);
    }

    /**
     * Reload configuration with concurrency control and configurable failure behavior.
     * This method implements the Template Method pattern - it defines the reload algorithm
     * skeleton with fixed steps.
     *
     * @param newConfig The new configuration to apply
     * @param reloadOptions Options controlling reload behavior (failure handling, persistence)
     * @return CompletableFuture with reload result
     */
    public CompletableFuture<ReloadResult> reload(Configuration newConfig, ReloadOptions reloadOptions) {
        Objects.requireNonNull(newConfig, "newConfig cannot be null");
        Objects.requireNonNull(reloadOptions, "reloadOptions cannot be null");

        // 1. Check if reload already in progress
        if (!reloadLock.tryLock()) {
            String errorMessage = "A reload operation is already in progress";
            LOGGER.warn(errorMessage);
            return CompletableFuture.failedFuture(new ConcurrentReloadException(errorMessage));
        }

        Instant startTime = Instant.now();
        OnFailure onFailure = reloadOptions.effectiveOnFailure();

        try {
            // 2. Mark reload as started
            stateManager.startReload();
            LOGGER.info("Configuration reload started (onFailure={}, persistConfigToDisk={})", onFailure, reloadOptions.persistConfigToDisk());

            // 3. Validate configuration
            Configuration validatedConfig = validateConfiguration(newConfig);
            LOGGER.debug("Configuration validation successful");

            // 4. Execute reload
            return executeReload(validatedConfig, startTime, onFailure)
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            LOGGER.error("Configuration reload failed", error);
                            stateManager.recordFailure(error);
                        }
                        else {
                            LOGGER.info("Configuration reload completed successfully");
                            stateManager.recordSuccess(result);
                            // Update current configuration on success
                            this.currentConfiguration = validatedConfig;

                            // Conditionally persist to disk
                            if (reloadOptions.persistConfigToDisk()) {
                                persistConfigurationToDisk(validatedConfig);
                            }
                            else {
                                LOGGER.debug("Skipping disk persistence (persistConfigToDisk=false)");
                            }
                        }
                    });

        }
        catch (Exception e) {
            stateManager.recordFailure(e);
            LOGGER.error("Configuration reload validation failed", e);
            return CompletableFuture.failedFuture(e);
        }
        finally {
            // CRITICAL FIX: Always unlock, even if future is pending
            // This ensures the lock is released immediately after returning the future,
            // preventing permanent lock acquisition when .get() times out
            reloadLock.unlock();
            LOGGER.debug("Reload lock released");
        }
    }

    /**
     * Validate configuration using Features framework.
     * Throws ValidationException if configuration is invalid.
     */
    private Configuration validateConfiguration(Configuration config) throws ValidationException {
        try {
            List<String> errorMessages = features.supports(config);
            if (!errorMessages.isEmpty()) {
                String message = "Configuration validation failed: " + String.join(", ", errorMessages);
                throw new ValidationException(message);
            }
            return config;
        }
        catch (IllegalConfigurationException e) {
            throw new ValidationException("Configuration validation failed: " + e.getMessage(), e);
        }
        catch (Exception e) {
            throw new ValidationException("Unexpected error during configuration validation: " + e.getMessage(), e);
        }
    }

    /**
     * Execute the configuration reload by creating a new FilterChainFactory, building a change context,
     * and delegating to the ConfigurationChangeHandler. On success, swaps to the new factory atomically.
     * On failure, behavior depends on the {@code onFailure} parameter:
     * <ul>
     *   <li>{@link OnFailure#ROLLBACK}: close new factory, keep old factory (safe default)</li>
     *   <li>{@link OnFailure#TERMINATE}: commit new factory, close old factory (proxy will shut down)</li>
     *   <li>{@link OnFailure#CONTINUE}: commit new factory, close old factory (keep partial state)</li>
     * </ul>
     */
    private CompletableFuture<ReloadResult> executeReload(Configuration newConfig, Instant startTime, OnFailure onFailure) {
        // 1. Create new FilterChainFactory with updated filter definitions
        FilterChainFactory newFactory;
        try {
            LOGGER.debug("Creating new FilterChainFactory with updated filter definitions");
            newFactory = new FilterChainFactory(pluginFactoryRegistry, newConfig.filterDefinitions());
            LOGGER.info("New FilterChainFactory created successfully");
        }
        catch (Exception e) {
            LOGGER.error("Failed to create new FilterChainFactory", e);
            throw new CompletionException("Failed to create new FilterChainFactory: " + e.getMessage(), e);
        }

        // 2. Get old factory for rollback capability
        FilterChainFactory oldFactory = filterChainFactoryRef.get();

        // 3. Build change context with both old and new factories
        ConfigurationChangeContext changeContext = getConfigurationChangeContext(newConfig, oldFactory, newFactory);

        // 4. Execute configuration changes (virtual cluster restarts, etc.)
        return configurationChangeHandler.handleConfigurationChange(changeContext, onFailure)
                .thenApply(v -> {
                    // SUCCESS: Atomically swap to new factory
                    LOGGER.info("Configuration changes applied successfully, swapping FilterChainFactory");
                    filterChainFactoryRef.set(newFactory);

                    // Close old factory to release filter resources
                    if (oldFactory != null) {
                        try {
                            oldFactory.close();
                            LOGGER.info("Old FilterChainFactory closed successfully");
                        }
                        catch (Exception e) {
                            LOGGER.warn("Exception while closing old FilterChainFactory (new factory already active)", e);
                        }
                    }

                    return buildReloadResult(changeContext, startTime);
                })
                .exceptionally(error -> {
                    if (onFailure == OnFailure.ROLLBACK) {
                        // ROLLBACK: close new factory, keep old factory
                        LOGGER.error("Configuration reload failed, rolling back FilterChainFactory", error);

                        try {
                            newFactory.close();
                            LOGGER.info("New FilterChainFactory closed successfully (rollback)");
                        }
                        catch (Exception e) {
                            LOGGER.warn("Exception while closing new FilterChainFactory during rollback", e);
                        }

                        // filterChainFactoryRef remains unchanged - still points to oldFactory
                        LOGGER.info("FilterChainFactory rollback complete, old factory remains active");
                    }
                    else {
                        // TERMINATE or CONTINUE: commit new factory, close old factory (no rollback)
                        LOGGER.warn("Configuration reload failed with onFailure={} - committing new factory (no rollback)", onFailure);
                        filterChainFactoryRef.set(newFactory);

                        if (oldFactory != null) {
                            try {
                                oldFactory.close();
                                LOGGER.info("Old FilterChainFactory closed (no rollback, onFailure={})", onFailure);
                            }
                            catch (Exception e) {
                                LOGGER.warn("Exception while closing old FilterChainFactory (onFailure={})", onFailure, e);
                            }
                        }
                    }

                    // Null-safe error message extraction
                    String errorMessage = error.getMessage();
                    if (errorMessage == null || errorMessage.isBlank()) {
                        errorMessage = error.getClass().getSimpleName();
                        if (error.getCause() != null) {
                            errorMessage += " caused by " + error.getCause().getClass().getSimpleName();
                        }
                    }
                    throw new CompletionException("Configuration reload failed: " + errorMessage, error);
                });
    }

    @NonNull
    private ConfigurationChangeContext getConfigurationChangeContext(Configuration newConfig, FilterChainFactory oldFactory, FilterChainFactory newFactory) {
        List<VirtualClusterModel> oldModels = currentConfiguration.virtualClusterModel();
        List<VirtualClusterModel> newModels = newConfig.virtualClusterModel();

        return new ConfigurationChangeContext(
                currentConfiguration,
                newConfig,
                oldModels,
                newModels,
                oldFactory,
                newFactory);
    }

    /**
     * Build a successful reload result from the change context.
     */
    private ReloadResult buildReloadResult(ConfigurationChangeContext changeContext, Instant startTime) {
        ConfigurationChangeResult changeResult = ConfigurationChangeResult.from(changeContext);
        Instant endTime = Instant.now();
        Duration duration = Duration.between(startTime, endTime);

        return ReloadResult.builder()
                .success(true)
                .message("Configuration reloaded successfully")
                .clustersModified(changeResult.modifiedCount())
                .clustersAdded(changeResult.addedCount())
                .clustersRemoved(changeResult.removedCount())
                .timestamp(endTime)
                .duration(duration)
                .build();
    }

    /**
     * Get the current reload state.
     */
    public ReloadStateManager.ReloadState getCurrentState() {
        return stateManager.getCurrentState();
    }

    /**
     * Get the last reload result.
     */
    public java.util.Optional<ReloadResult> getLastResult() {
        return stateManager.getLastResult();
    }

    /**
     * Persist the configuration to disk with backup.
     * This method never throws exceptions - failures are logged as warnings.
     *
     * @param configuration The configuration to persist
     */
    private void persistConfigurationToDisk(Configuration configuration) {
        if (configFilePath == null) {
            LOGGER.debug("Config file path not provided - skipping disk persistence");
            return;
        }

        try {
            // 1. Create backup of existing file
            Path backupPath = Path.of(configFilePath.toString() + ".bak");
            if (Files.exists(configFilePath)) {
                Files.copy(configFilePath, backupPath, StandardCopyOption.REPLACE_EXISTING);
                LOGGER.debug("Created backup: {}", backupPath);
            }

            // 2. Serialize configuration to YAML
            ConfigParser parser = new ConfigParser();
            String yamlContent = parser.toYaml(configuration);

            // 3. Write to temp file first (atomic write)
            Path tempFile = Path.of(configFilePath.toString() + ".tmp");
            Files.writeString(tempFile, yamlContent);

            // 4. Atomic rename: temp → actual file
            Files.move(tempFile, configFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            LOGGER.info("Successfully persisted configuration to disk: {}", configFilePath);
        }
        catch (Exception e) {
            // Log warning but don't fail the reload
            LOGGER.warn("Failed to persist configuration to disk (reload still succeeded in memory): {}",
                    configFilePath, e);
        }
    }
}
