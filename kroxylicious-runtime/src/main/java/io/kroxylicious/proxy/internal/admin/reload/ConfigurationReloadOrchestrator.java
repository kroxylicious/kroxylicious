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
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.ConfigurationChangeResult;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.internal.ConfigurationChangeContext;
import io.kroxylicious.proxy.internal.ConfigurationChangeHandler;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.model.VirtualClusterModel;

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

    public ConfigurationReloadOrchestrator(
            Configuration initialConfiguration,
            ConfigurationChangeHandler configurationChangeHandler,
            PluginFactoryRegistry pluginFactoryRegistry,
            Features features,
            @Nullable Path configFilePath) {
        this.currentConfiguration = Objects.requireNonNull(initialConfiguration, "initialConfiguration cannot be null");
        this.configurationChangeHandler = Objects.requireNonNull(configurationChangeHandler, "configurationChangeHandler cannot be null");
        this.pluginFactoryRegistry = Objects.requireNonNull(pluginFactoryRegistry, "pluginFactoryRegistry cannot be null");
        this.features = Objects.requireNonNull(features, "features cannot be null");
        this.configFilePath = configFilePath;
        this.stateManager = new ReloadStateManager();
        this.reloadLock = new ReentrantLock();
    }

    /**
     * Reload configuration with concurrency control.
     * This method implements the Template Method pattern - it defines the reload algorithm
     * skeleton with fixed steps.
     *
     * @param newConfig The new configuration to apply
     * @return CompletableFuture with reload result
     */
    public CompletableFuture<ReloadResult> reload(Configuration newConfig) {
        Objects.requireNonNull(newConfig, "newConfig cannot be null");

        // 1. Check if reload already in progress
        if (!reloadLock.tryLock()) {
            String errorMessage = "A reload operation is already in progress";
            LOGGER.warn(errorMessage);
            return CompletableFuture.failedFuture(new ConcurrentReloadException(errorMessage));
        }

        Instant startTime = Instant.now();

        try {
            // 2. Mark reload as started
            stateManager.startReload();
            LOGGER.info("Configuration reload started");

            // 3. Validate configuration
            Configuration validatedConfig = validateConfiguration(newConfig);
            LOGGER.debug("Configuration validation successful");

            // 4. Execute reload
            return executeReload(validatedConfig, startTime)
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

                            // Persist to disk if file path is available
                            persistConfigurationToDisk(validatedConfig);
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
     * Execute the configuration reload by creating a change context and
     * delegating to the ConfigurationChangeHandler.
     */
    private CompletableFuture<ReloadResult> executeReload(Configuration newConfig, Instant startTime) {
        // Build change context
        List<VirtualClusterModel> oldModels = currentConfiguration.virtualClusterModel(pluginFactoryRegistry);
        List<VirtualClusterModel> newModels = newConfig.virtualClusterModel(pluginFactoryRegistry);

        ConfigurationChangeContext changeContext = new ConfigurationChangeContext(
                currentConfiguration,
                newConfig,
                oldModels,
                newModels);

        // Execute change through handler
        return configurationChangeHandler.handleConfigurationChange(changeContext)
                .thenApply(v -> buildReloadResult(changeContext, startTime))
                .exceptionally(error -> {
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

