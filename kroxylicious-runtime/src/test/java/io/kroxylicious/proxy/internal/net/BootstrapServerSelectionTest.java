package io.kroxylicious.proxy.internal.net;

import java.util.List;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify that bootstrap server selection distributes across multiple servers
 * instead of always selecting the first one.
 */
class BootstrapServerSelectionTest {
    
    @Test
    void shouldSelectDifferentBootstrapServersOverTime() {
        // Arrange
        List<HostPort> bootstrapServers = List.of(
            HostPort.parse("server1:9092"),
            HostPort.parse("server2:9092"), 
            HostPort.parse("server3:9092")
        );
        
        EndpointRegistry registry = new EndpointRegistry(new TestNetworkBindingOperationProcessor());
        Set<HostPort> selectedServers = new HashSet<>();
        
        // Act - try to get different selections by varying the time
        for (int i = 0; i < 50; i++) {
            // Use reflection to call private method for testing
            try {
                java.lang.reflect.Method method = EndpointRegistry.class.getDeclaredMethod("selectBootstrapServer", List.class);
                method.setAccessible(true);
                HostPort selected = (HostPort) method.invoke(registry, bootstrapServers);
                selectedServers.add(selected);
                
                // Add small delay to change currentTimeMillis
                Thread.sleep(1);
            } catch (Exception e) {
                throw new RuntimeException("Failed to test bootstrap server selection", e);
            }
        }
        
        // Assert - should have selected more than just the first server
        assertThat(selectedServers.size()).isGreaterThan(1)
            .describedAs("Should select different bootstrap servers, not just the first one");
        assertThat(selectedServers).contains(HostPort.parse("server1:9092"));
        // Should also contain at least one other server
        assertThat(selectedServers.size()).isGreaterThanOrEqualTo(2);
    }
    
    @Test 
    void shouldReturnSingleServerWhenOnlyOneProvided() {
        // Arrange
        List<HostPort> singleServer = List.of(HostPort.parse("server1:9092"));
        EndpointRegistry registry = new EndpointRegistry(new TestNetworkBindingOperationProcessor());
        
        // Act & Assert
        try {
            java.lang.reflect.Method method = EndpointRegistry.class.getDeclaredMethod("selectBootstrapServer", List.class);
            method.setAccessible(true);
            HostPort selected = (HostPort) method.invoke(registry, singleServer);
            
            assertThat(selected).isEqualTo(HostPort.parse("server1:9092"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to test single bootstrap server selection", e);
        }
    }
    
    /**
     * Test implementation of NetworkBindingOperationProcessor for testing
     */
    private static class TestNetworkBindingOperationProcessor implements NetworkBindingOperationProcessor {
        @Override
        public void enqueueNetworkBindingEvent(NetworkBindingOperation<?> o) {
            // No-op for testing
        }

        @Override
        public void start(io.netty.bootstrap.ServerBootstrap plainServerBootstrap, 
                         io.netty.bootstrap.ServerBootstrap tlsServerBootstrap) {
            // No-op for testing  
        }

        @Override
        public void close() {
            // No-op for testing
        }
    }
}
