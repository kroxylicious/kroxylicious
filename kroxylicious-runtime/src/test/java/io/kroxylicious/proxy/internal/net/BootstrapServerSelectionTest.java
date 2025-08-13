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
    void shouldSelectDifferentBootstrapServersInRoundRobin() {
        // Arrange
        List<HostPort> bootstrapServers = List.of(
            HostPort.parse("server1:9092"),
            HostPort.parse("server2:9092"), 
            HostPort.parse("server3:9092")
        );
        
        // Act - test deterministic round-robin with specific counter values
        HostPort selected0 = EndpointRegistry.selectBootstrapServer(bootstrapServers, 0);  // 0 % 3 = 0 -> server1
        HostPort selected1 = EndpointRegistry.selectBootstrapServer(bootstrapServers, 1);  // 1 % 3 = 1 -> server2
        HostPort selected2 = EndpointRegistry.selectBootstrapServer(bootstrapServers, 2);  // 2 % 3 = 2 -> server3
        HostPort selected3 = EndpointRegistry.selectBootstrapServer(bootstrapServers, 3);  // 3 % 3 = 0 -> server1
        HostPort selected4 = EndpointRegistry.selectBootstrapServer(bootstrapServers, 4);  // 4 % 3 = 1 -> server2
        
        // Assert - should follow round-robin pattern
        assertThat(selected0).isEqualTo(HostPort.parse("server1:9092"));
        assertThat(selected1).isEqualTo(HostPort.parse("server2:9092"));
        assertThat(selected2).isEqualTo(HostPort.parse("server3:9092"));
        assertThat(selected3).isEqualTo(HostPort.parse("server1:9092"));
        assertThat(selected4).isEqualTo(HostPort.parse("server2:9092"));
    }
    
    @Test 
    void shouldReturnSingleServerWhenOnlyOneProvided() {
        // Arrange
        List<HostPort> singleServer = List.of(HostPort.parse("server1:9092"));
        
        // Act - test with different counter values, should always return the same server
        HostPort selected1 = EndpointRegistry.selectBootstrapServer(singleServer, 0);
        HostPort selected2 = EndpointRegistry.selectBootstrapServer(singleServer, 12345);
        HostPort selected3 = EndpointRegistry.selectBootstrapServer(singleServer, 999);
        
        // Assert
        assertThat(selected1).isEqualTo(HostPort.parse("server1:9092"));
        assertThat(selected2).isEqualTo(HostPort.parse("server1:9092"));
        assertThat(selected3).isEqualTo(HostPort.parse("server1:9092"));
    }
    
    @Test
    void shouldDistributeEvenlyAcrossServers() {
        // Arrange
        List<HostPort> bootstrapServers = List.of(
            HostPort.parse("server1:9092"),
            HostPort.parse("server2:9092")
        );
        
        // Act - test specific counter values for 2 servers
        HostPort selected0 = EndpointRegistry.selectBootstrapServer(bootstrapServers, 0);  // 0 % 2 = 0 -> server1
        HostPort selected1 = EndpointRegistry.selectBootstrapServer(bootstrapServers, 1);  // 1 % 2 = 1 -> server2
        HostPort selected2 = EndpointRegistry.selectBootstrapServer(bootstrapServers, 2);  // 2 % 2 = 0 -> server1
        HostPort selected3 = EndpointRegistry.selectBootstrapServer(bootstrapServers, 3);  // 3 % 2 = 1 -> server2
        
        // Assert
        assertThat(selected0).isEqualTo(HostPort.parse("server1:9092"));
        assertThat(selected1).isEqualTo(HostPort.parse("server2:9092"));
        assertThat(selected2).isEqualTo(HostPort.parse("server1:9092"));
        assertThat(selected3).isEqualTo(HostPort.parse("server2:9092"));
    }
    
    @Test
    void shouldTestActualRoundRobinBehavior() {
        // Arrange
        List<HostPort> bootstrapServers = List.of(
            HostPort.parse("server1:9092"),
            HostPort.parse("server2:9092"), 
            HostPort.parse("server3:9092")
        );
        
        Set<HostPort> selectedServers = new HashSet<>();
        
        // Act - use the actual round-robin method (without counter parameter)
        // This tests the real behavior with the internal counter
        for (int i = 0; i < 10; i++) {
            HostPort selected = EndpointRegistry.selectBootstrapServer(bootstrapServers);
            selectedServers.add(selected);
        }
        
        // Assert - should have used all servers
        assertThat(selectedServers).containsExactlyInAnyOrder(
            HostPort.parse("server1:9092"),
            HostPort.parse("server2:9092"),
            HostPort.parse("server3:9092")
        );
    }
}
