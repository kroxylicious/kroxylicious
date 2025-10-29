/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.enforcer;

import org.apache.maven.enforcer.rule.api.EnforcerRuleException;
import org.apache.maven.model.Model;
import org.apache.maven.project.MavenProject;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RequireSonatypeCentralMetadataTest {

    @Mock
    MavenProject mavenProject;

    @Mock
    Model originalModel;

    @InjectMocks
    RequireSonatypeCentralMetadata requireSonatypeCentralMetadata = new RequireSonatypeCentralMetadata();

    @BeforeEach
    void setup() {
        when(mavenProject.getOriginalModel()).thenReturn(originalModel);
    }

    @Test
    void projectNameNotSpecified() {
        // given
        when(originalModel.getName()).thenReturn(null);
        // when
        Assertions.assertThatThrownBy(() -> requireSonatypeCentralMetadata.execute())
                // then
                .isInstanceOf(EnforcerRuleException.class)
                .hasMessage("Project name is not explicitly specified, please add a <name> element to the pom.xml.");
    }

    @Test
    void descriptionNotSpecified() {
        // given
        when(originalModel.getName()).thenReturn("projectname");
        when(originalModel.getDescription()).thenReturn(null);
        // when
        Assertions.assertThatThrownBy(() -> requireSonatypeCentralMetadata.execute())
                // then
                .isInstanceOf(EnforcerRuleException.class).hasMessage(
                        "Project description is not explicitly specified, please add a <description> element to the pom.xml.");
    }

    @Test
    void procectAndDescriptionSpecified() {
        // given
        when(originalModel.getName()).thenReturn("projectname");
        when(originalModel.getDescription()).thenReturn("description");
        // when
        Assertions.assertThatCode(() -> requireSonatypeCentralMetadata.execute())
                // then
                .doesNotThrowAnyException();
    }

}
