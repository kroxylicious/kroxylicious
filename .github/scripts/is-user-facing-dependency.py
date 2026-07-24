#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""
Determine whether a Dependabot Maven dependency bump is user-facing.

Exit 0 (CREATE) when a changelog entry should be created.
Exit 1 (SKIP)   when the dependency is test-only or a build plugin.

Usage:
  python3 is-user-facing-dependency.py <package>   # normal operation
  python3 is-user-facing-dependency.py --test       # run self-tests

<package> is the bumped package extracted from the Dependabot PR title:
  - Artifact form: "groupId:artifactId"  (e.g. "com.fasterxml.jackson:jackson-bom")
  - Property form: "property.name"        (e.g. "lowkey.version")
"""

import glob
import sys
import unittest
import xml.etree.ElementTree as ET

# Maven POM XML namespace used in all pom.xml files
MAVEN_NS = 'http://maven.apache.org/POM/4.0.0'


# ---------------------------------------------------------------------------
# Low-level XML helpers
# ---------------------------------------------------------------------------

def _tag(local_name):
    """Return the fully-qualified Maven namespace tag, e.g. _tag('plugin')."""
    return f'{{{MAVEN_NS}}}{local_name}'


def _text(element, local_name):
    """Return stripped text of a child element, or empty string if absent."""
    return (element.findtext(_tag(local_name)) or '').strip()


def _build_parent_map(root):
    """
    Return a dict mapping each element to its direct parent.

    ElementTree does not expose parent references natively, so we build
    this by iterating the whole tree once.
    """
    parent_map = {}
    for parent in root.iter():
        for child in parent:
            parent_map[child] = parent
    return parent_map


def _is_inside_plugin(element, parent_map):
    """
    Return True if element is nested anywhere inside a <plugin> element.

    This is used to distinguish project-level <dependency> elements from
    dependencies declared inside a <plugin>'s own <dependencies> block,
    which are build-time only and not user-facing.
    """
    ancestor = parent_map.get(element)
    while ancestor is not None:
        if ancestor.tag in {_tag('plugin'), 'plugin'}:
            return True
        ancestor = parent_map.get(ancestor)
    return False


def _scope_of(dependency):
    """Return the Maven scope of a <dependency> element, defaulting to 'compile'."""
    return _text(dependency, 'scope') or 'compile'


# ---------------------------------------------------------------------------
# Per-pom classification helpers
# ---------------------------------------------------------------------------

def _artifact_is_a_build_plugin(group_id, artifact_id, root):
    """Return True if this pom declares the artifact as a <plugin>."""
    for plugin in root.iter(_tag('plugin')):
        if _text(plugin, 'groupId') == group_id and _text(plugin, 'artifactId') == artifact_id:
            return True
    return False


def _find_dependency_scope(group_id, artifact_id, root, parent_map):
    """
    Search for a <dependency> matching group_id:artifact_id in this pom.

    Returns:
      True  if the dependency is found with a non-test, non-plugin-internal scope.
      False if the dependency is found but is test-only or plugin-internal.
      None  if no matching dependency is declared in this pom.
    """
    found_non_production = False
    for dep in root.iter(_tag('dependency')):
        if _text(dep, 'groupId') != group_id or _text(dep, 'artifactId') != artifact_id:
            continue

        if _is_inside_plugin(dep, parent_map):
            # Build-time dependency of a Maven plugin — not a project dependency.
            found_non_production = True
            continue

        if _scope_of(dep) == 'test':
            found_non_production = True
        else:
            # compile / runtime / provided / import - all user-facing
            return True

    return False if found_non_production else None


def _property_version_is_user_facing(prop_ref, root, parent_map):
    """
    Search for <version> elements containing prop_ref (e.g. '${lowkey.version}').

    Returns:
      True  if any matching version belongs to a user-facing dependency.
      False if all matches are test-scope, plugin versions, or plugin-internal.
      None  if prop_ref is not used as a version in this pom.
    """
    found_non_production = False
    for version_elem in root.iter(_tag('version')):
        if not (version_elem.text and prop_ref in version_elem.text):
            continue

        parent = parent_map.get(version_elem)
        if parent is None:
            continue

        if parent.tag in {_tag('plugin'), 'plugin'}:
            found_non_production = True
        elif parent.tag in {_tag('dependency'), 'dependency'}:
            if _is_inside_plugin(parent, parent_map):
                found_non_production = True
            elif _scope_of(parent) == 'test':
                found_non_production = True
            else:
                return True
        # Any other parent (e.g. <parent>, <project>) is not a dependency
        # version - skip it without affecting the result.

    return False if found_non_production else None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def check_poms(pkg, poms):
    """
    Classify a Dependabot package against a collection of parsed pom.xml files.

    poms    - iterable of (path, root_element) pairs.
    pkg     - either "groupId:artifactId" or "property.name".

    Returns True  if a changelog entry should be created (user-facing dependency).
    Returns False if the entry should be skipped (test-only or build plugin).
    """
    is_artifact_form = ':' in pkg
    if is_artifact_form:
        group_id, artifact_id = pkg.split(':', 1)
        prop_ref = None
    else:
        group_id = artifact_id = None
        prop_ref = f'${{{pkg}}}'  # e.g. "${lowkey.version}"

    found_anything = False

    for _path, root in poms:
        parent_map = _build_parent_map(root)

        if is_artifact_form:
            if _artifact_is_a_build_plugin(group_id, artifact_id, root):
                found_anything = True
                # Do not skip dependency check — an artifact can be both a plugin
                # in one context and a project dependency in another within the same pom.

            scope_result = _find_dependency_scope(group_id, artifact_id, root, parent_map)
            if scope_result:
                return True   # found as a user-facing dep - no need to look further
            if scope_result is not None:
                found_anything = True

        else:
            scope_result = _property_version_is_user_facing(prop_ref, root, parent_map)
            if scope_result:
                return True   # found as a user-facing dep - no need to look further
            if scope_result is not None:
                found_anything = True

    # If we never found this artifact in any pom, we cannot determine its scope.
    # Default to CREATE so user-facing deps are never silently omitted.
    return not found_anything


def check(pkg):
    """Scan the filesystem for pom.xml files and classify pkg."""
    poms = []
    for path in sorted(glob.glob('**/pom.xml', recursive=True)):
        try:
            poms.append((path, ET.parse(path).getroot()))
        except Exception as e:
            print(f'Warning: could not parse {path}: {e}', file=sys.stderr)
    return check_poms(pkg, poms)


# ---------------------------------------------------------------------------
# Self-tests  (python3 is-user-facing-dependency.py --test)
# ---------------------------------------------------------------------------

def _pom(content):
    """Parse a minimal pom.xml fragment for use in tests."""
    xml = f'<project xmlns="http://maven.apache.org/POM/4.0.0">{content}</project>'
    return ('test-pom.xml', ET.fromstring(xml))


class CheckPomsTests(unittest.TestCase):

    def test_compile_scope_dep_is_user_facing(self):
        pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-lib</artifactId>
                    <version>1.0</version>
                </dependency>
            </dependencies>''')
        self.assertTrue(check_poms('com.example:my-lib', [pom]))

    def test_provided_scope_dep_is_user_facing(self):
        pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-lib</artifactId>
                    <version>1.0</version>
                    <scope>provided</scope>
                </dependency>
            </dependencies>''')
        self.assertTrue(check_poms('com.example:my-lib', [pom]))

    def test_runtime_scope_dep_is_user_facing(self):
        pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-lib</artifactId>
                    <version>1.0</version>
                    <scope>runtime</scope>
                </dependency>
            </dependencies>''')
        self.assertTrue(check_poms('com.example:my-lib', [pom]))

    def test_test_scope_dep_is_skipped(self):
        pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-test-lib</artifactId>
                    <version>1.0</version>
                    <scope>test</scope>
                </dependency>
            </dependencies>''')
        self.assertFalse(check_poms('com.example:my-test-lib', [pom]))

    def test_plugin_is_skipped(self):
        pom = _pom('''
            <build>
                <pluginManagement>
                    <plugins>
                        <plugin>
                            <groupId>com.example</groupId>
                            <artifactId>my-maven-plugin</artifactId>
                            <version>1.0</version>
                        </plugin>
                    </plugins>
                </pluginManagement>
            </build>''')
        self.assertFalse(check_poms('com.example:my-maven-plugin', [pom]))

    def test_plugin_internal_dep_is_skipped(self):
        pom = _pom('''
            <build>
                <plugins>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-dependency-plugin</artifactId>
                        <dependencies>
                            <dependency>
                                <groupId>com.example</groupId>
                                <artifactId>my-lib</artifactId>
                                <version>1.0</version>
                            </dependency>
                        </dependencies>
                    </plugin>
                </plugins>
            </build>''')
        self.assertFalse(check_poms('com.example:my-lib', [pom]))

    def test_not_found_defaults_to_create(self):
        pom = _pom('<dependencies/>')
        self.assertTrue(check_poms('com.example:unknown', [pom]))

    def test_empty_poms_defaults_to_create(self):
        self.assertTrue(check_poms('com.example:unknown', []))

    def test_different_groupid_does_not_match(self):
        pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>org.other</groupId>
                    <artifactId>my-lib</artifactId>
                    <version>1.0</version>
                    <scope>test</scope>
                </dependency>
            </dependencies>''')
        # com.example:my-lib not found - defaults to CREATE
        self.assertTrue(check_poms('com.example:my-lib', [pom]))

    def test_property_reference_to_test_dep_is_skipped(self):
        pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-test-lib</artifactId>
                    <version>${my-test-lib.version}</version>
                    <scope>test</scope>
                </dependency>
            </dependencies>''')
        self.assertFalse(check_poms('my-test-lib.version', [pom]))

    def test_property_reference_to_compile_dep_is_user_facing(self):
        pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-lib</artifactId>
                    <version>${my-lib.version}</version>
                </dependency>
            </dependencies>''')
        self.assertTrue(check_poms('my-lib.version', [pom]))

    def test_property_reference_to_plugin_is_skipped(self):
        pom = _pom('''
            <build>
                <plugins>
                    <plugin>
                        <groupId>com.example</groupId>
                        <artifactId>my-maven-plugin</artifactId>
                        <version>${my-plugin.version}</version>
                    </plugin>
                </plugins>
            </build>''')
        self.assertFalse(check_poms('my-plugin.version', [pom]))

    def test_property_in_parent_pom_declaration_does_not_affect_result(self):
        """A property used only in <parent><version> is not a dependency - defaults to CREATE."""
        pom = _pom('''
            <parent>
                <groupId>com.example</groupId>
                <artifactId>parent</artifactId>
                <version>${some.version}</version>
            </parent>''')
        self.assertTrue(check_poms('some.version', [pom]))

    def test_property_name_not_matched_as_substring_of_longer_name(self):
        """${foo.version} must not match ${notfoo.version}."""
        pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-test-lib</artifactId>
                    <version>${notfoo.version}</version>
                    <scope>test</scope>
                </dependency>
            </dependencies>''')
        # foo.version not found - defaults to CREATE
        self.assertTrue(check_poms('foo.version', [pom]))

    def test_dep_in_profile_is_user_facing(self):
        pom = _pom('''
            <profiles>
                <profile>
                    <id>my-profile</id>
                    <dependencies>
                        <dependency>
                            <groupId>com.example</groupId>
                            <artifactId>my-lib</artifactId>
                            <version>1.0</version>
                        </dependency>
                    </dependencies>
                </profile>
            </profiles>''')
        self.assertTrue(check_poms('com.example:my-lib', [pom]))

    def test_empty_scope_element_treated_as_compile(self):
        pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-lib</artifactId>
                    <version>1.0</version>
                    <scope/>
                </dependency>
            </dependencies>''')
        self.assertTrue(check_poms('com.example:my-lib', [pom]))

    def test_artifact_that_is_both_plugin_and_dep_in_same_pom(self):
        """Artifact declared as both a plugin and a production dep — should CREATE."""
        pom = _pom('''
            <build>
                <plugins>
                    <plugin>
                        <groupId>com.example</groupId>
                        <artifactId>my-lib</artifactId>
                        <version>1.0</version>
                    </plugin>
                </plugins>
            </build>
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-lib</artifactId>
                    <version>1.0</version>
                </dependency>
            </dependencies>''')
        self.assertTrue(check_poms('com.example:my-lib', [pom]))

    def test_mixed_scopes_creates_entry_if_any_production(self):
        """When same artifact is test-scope in one module but compile in another, CREATE."""
        test_pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-lib</artifactId>
                    <scope>test</scope>
                </dependency>
            </dependencies>''')
        compile_pom = _pom('''
            <dependencies>
                <dependency>
                    <groupId>com.example</groupId>
                    <artifactId>my-lib</artifactId>
                    <scope>compile</scope>
                </dependency>
            </dependencies>''')
        self.assertTrue(check_poms('com.example:my-lib', [test_pom, compile_pom]))


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == '--test':
        sys.argv.pop()  # prevent unittest from interpreting '--test'
        unittest.main(verbosity=2)
    elif len(sys.argv) == 2:
        pkg = sys.argv[1]
        if check(pkg):
            print(f'{pkg}: user-facing production dependency - CREATE entry')
            sys.exit(0)
        else:
            print(f'{pkg}: test-only or build plugin - SKIP entry')
            sys.exit(1)
    else:
        print(f'Usage: {sys.argv[0]} <groupId:artifactId | property.name>', file=sys.stderr)
        print(f'       {sys.argv[0]} --test', file=sys.stderr)
        sys.exit(2)