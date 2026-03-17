#!/usr/bin/env python3
"""
Add expectedAuditActions to authorization test YAML files.

This script analyzes each test scenario and determines what audit actions should be logged
based on the request type, resources accessed, and authorization rules.
"""

import yaml
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

# Mapping of API keys to resource operations (based on enforcement classes)
# These map to TopicResource enum values
API_TO_OPERATION = {
    'DELETE_RECORDS': 'DELETE',
    'FETCH': 'READ',
    'PRODUCE': 'WRITE',
    'LIST_OFFSETS': 'DESCRIBE',
    'OFFSET_COMMIT': 'READ',
    'OFFSET_FETCH': 'READ',
    'OFFSET_DELETE': 'READ',
    'OFFSET_FOR_LEADER_EPOCH': 'DESCRIBE',
    'FIND_COORDINATOR': None,  # Not topic-based
    'JOIN_GROUP': 'READ',
    'SYNC_GROUP': 'READ',
    'HEARTBEAT': 'READ',
    'LEAVE_GROUP': 'READ',
    'DELETE_GROUPS': 'DELETE',
    'CREATE_TOPICS': 'CREATE',
    'DELETE_TOPICS': 'DELETE',
    'METADATA': 'DESCRIBE',
    'ALTER_CONFIGS': 'ALTER_CONFIGS',
    'DESCRIBE_CONFIGS': 'DESCRIBE_CONFIGS',
    'INCREMENTAL_ALTER_CONFIGS': 'ALTER_CONFIGS',
    'CREATE_PARTITIONS': 'ALTER',  # Note: uses ALTER not CREATE
    'CONSUMER_GROUP_HEARTBEAT': 'READ',
    'CONSUMER_GROUP_DESCRIBE': 'DESCRIBE',
    'DESCRIBE_PRODUCERS': 'READ',
    'DESCRIBE_TOPIC_PARTITIONS': 'DESCRIBE',
    'TXN_OFFSET_COMMIT': 'READ',
    'API_VERSIONS': None,  # No authorization
    'INIT_PRODUCER_ID': None,  # TransactionalId resource, special handling
    'ADD_PARTITIONS_TO_TXN': None,  # Mixed topic + transactionalId, special handling
    'ADD_OFFSETS_TO_TXN': None,  # TransactionalId resource, special handling
    'END_TXN': None,  # TransactionalId resource, special handling
    'DESCRIBE_TRANSACTIONS': None,  # TransactionalId resource, special handling
    'LIST_TRANSACTIONS': None,  # TransactionalId resource, special handling
}

# Resource class names
TOPIC_RESOURCE_CLASS = 'io.kroxylicious.filter.authorization.TopicResource'
TRANSACTIONAL_ID_RESOURCE_CLASS = 'io.kroxylicious.filter.authorization.TransactionalIdResource'

def get_topics_from_request(api_key: str, request: Dict) -> List[str]:
    """Extract topic names from a request."""
    topics = []

    # Handle config APIs that use 'resources' with resourceType
    if api_key in ('ALTER_CONFIGS', 'DESCRIBE_CONFIGS', 'INCREMENTAL_ALTER_CONFIGS'):
        if 'resources' in request:
            for resource in request['resources']:
                # resourceType 2 = TOPIC (from Kafka ConfigResource.Type)
                if isinstance(resource, dict) and resource.get('resourceType') == 2:
                    if 'resourceName' in resource:
                        topics.append(resource['resourceName'])
        return topics

    if 'topics' in request:
        topics_list = request['topics']
        if isinstance(topics_list, list):
            for topic in topics_list:
                if isinstance(topic, dict):
                    # Most APIs use 'name', but FETCH and some others use 'topic'
                    if 'name' in topic:
                        topics.append(topic['name'])
                    elif 'topic' in topic:
                        topics.append(topic['topic'])
                elif isinstance(topic, str):
                    topics.append(topic)

    # Special case for CONSUMER_GROUP_HEARTBEAT
    if api_key == 'CONSUMER_GROUP_HEARTBEAT' and 'topicPartitions' in request:
        for tp in request['topicPartitions']:
            if 'topic' in tp:
                topics.append(tp['topic'])

    return topics

def is_allowed(subject: str, resource_name: str, resource_type: str, authorizer_rules: Dict, resource_class: str = 'TOPIC') -> bool:
    """Check if a subject is allowed to perform an operation on a resource."""
    if 'allowed' not in authorizer_rules:
        return False

    for rule in authorizer_rules['allowed']:
        if (rule.get('subject') == subject and
            rule.get('resourceName') == resource_name and
            rule.get('resourceClass') == resource_class and
            rule.get('resourceType') == resource_type):
            return True

    return False

def should_skip_audit(scenario: Dict) -> bool:
    """Determine if this scenario should have no audit actions."""

    # Check expectAuthorizationOutcomeLog flag
    if scenario.get('then', {}).get('expectAuthorizationOutcomeLog') == False:
        return True

    api_key = scenario['metadata']['apiKeys']

    # API_VERSIONS and other non-authorizable requests
    # But don't skip transactional APIs - they may not have topic operations but still check transactional IDs
    if API_TO_OPERATION.get(api_key) is None and api_key not in ('TXN_OFFSET_COMMIT', 'ADD_PARTITIONS_TO_TXN',
                                                                    'ADD_OFFSETS_TO_TXN', 'INIT_PRODUCER_ID', 'END_TXN'):
        return True

    # METADATA with includeTopicAuthorizedOperations or includeClusterAuthorizedOperations
    if api_key == 'METADATA':
        request = scenario['when']['request']
        if request.get('includeTopicAuthorizedOperations') or request.get('includeClusterAuthorizedOperations'):
            return True

    # DESCRIBE_TOPIC_PARTITIONS with empty topics list is an all-topics discovery request - not audited
    if api_key == 'DESCRIBE_TOPIC_PARTITIONS':
        request = scenario['when']['request']
        if 'topics' in request and (request['topics'] is None or len(request['topics']) == 0):
            return True

    return False

def get_transactional_id(api_key: str, request: Dict) -> Optional[str]:
    """Extract transactional ID from a request."""
    if 'transactionalId' in request and request['transactionalId'] is not None:
        return request['transactionalId']
    # Some APIs use v3AndBelowTransactionalId
    if 'v3AndBelowTransactionalId' in request and request['v3AndBelowTransactionalId'] is not None:
        return request['v3AndBelowTransactionalId']
    return None

def get_topics_from_response(api_key: str, response: Dict) -> List[str]:
    """Extract topic names from a response (for response-side authorization checks)."""
    topics = []

    if api_key == 'CONSUMER_GROUP_DESCRIBE':
        # ConsumerGroupDescribe checks topics from response data
        if 'groups' in response:
            for group in response['groups']:
                if 'members' in group and isinstance(group['members'], list):
                    for member in group['members']:
                        # Check both assignment and targetAssignment
                        for assignment_field in ['assignment', 'targetAssignment']:
                            if assignment_field in member and member[assignment_field]:
                                assignment = member[assignment_field]
                                if 'topicPartitions' in assignment:
                                    for tp in assignment['topicPartitions']:
                                        if 'topicName' in tp:
                                            topics.append(tp['topicName'])

    return list(set(topics))  # Remove duplicates

def generate_audit_actions(scenario: Dict) -> List[Dict[str, Any]]:
    """Generate the expectedAuditActions for a scenario."""

    api_key = scenario['metadata']['apiKeys']

    if should_skip_audit(scenario):
        return []

    operation = API_TO_OPERATION.get(api_key)

    subject = scenario['when']['subject']
    request = scenario['when']['request']
    authorizer_rules = scenario['given']['authorizerRules']

    audit_actions = []

    # Generate audit actions for topic resources (only if operation is defined)
    if operation is not None:
        # Try to get topics from request first
        topics = get_topics_from_request(api_key, request)

        # For some APIs, authorization is done on the response - use mocked upstream response
        if not topics and 'mockedUpstreamResponses' in scenario['given']:
            for mock_response in scenario['given']['mockedUpstreamResponses']:
                if 'upstreamResponse' in mock_response and mock_response['upstreamResponse']:
                    response_topics = get_topics_from_response(api_key, mock_response['upstreamResponse'])
                    topics.extend(response_topics)

        # Generate audit actions for each topic
        for topic in topics:
            allowed = is_allowed(subject, topic, operation, authorizer_rules)

            audit_action = {
                'action': operation,
                'objectRef': {TOPIC_RESOURCE_CLASS: topic},
                'status': None if allowed else 'denied'
            }
            audit_actions.append(audit_action)

    # Handle transactional ID for APIs that use it
    if api_key in ('TXN_OFFSET_COMMIT', 'ADD_PARTITIONS_TO_TXN', 'ADD_OFFSETS_TO_TXN',
                   'INIT_PRODUCER_ID', 'END_TXN'):
        transactional_id = get_transactional_id(api_key, request)
        if transactional_id:
            # Determine the operation for transactional ID
            txn_operation = 'WRITE'  # Most transactional APIs use WRITE
            if api_key == 'INIT_PRODUCER_ID':
                txn_operation = 'WRITE'  # INIT_PRODUCER_ID uses WRITE

            allowed = is_allowed(subject, transactional_id, txn_operation, authorizer_rules, 'TRANSACTIONAL_ID')

            audit_action = {
                'action': txn_operation,
                'objectRef': {TRANSACTIONAL_ID_RESOURCE_CLASS: transactional_id},
                'status': None if allowed else 'denied'
            }
            audit_actions.append(audit_action)

    return audit_actions

def process_yaml_file(file_path: Path) -> bool:
    """Process a single YAML file, adding expectedAuditActions if needed."""

    try:
        with open(file_path, 'r') as f:
            content = f.read()

        # Parse YAML
        scenario = yaml.safe_load(content)

        # Check if expectedAuditActions already exists
        if scenario.get('then', {}).get('expectedAuditActions') is not None:
            print(f"Skipping {file_path} - already has expectedAuditActions")
            return False

        # Generate audit actions
        audit_actions = generate_audit_actions(scenario)

        # Add to scenario
        if 'then' not in scenario:
            scenario['then'] = {}
        scenario['then']['expectedAuditActions'] = audit_actions

        # Write back with preserved structure
        # We'll manually insert it to preserve formatting
        lines = content.split('\n')

        # Find the 'then:' section
        then_line_idx = None
        for i, line in enumerate(lines):
            if line.strip().startswith('then:'):
                then_line_idx = i
                break

        if then_line_idx is None:
            print(f"Warning: Could not find 'then:' section in {file_path}")
            return False

        # Find the indentation level
        then_indent = len(lines[then_line_idx]) - len(lines[then_line_idx].lstrip())
        field_indent = then_indent + 2

        # Find where to insert (at the end of the 'then' section)
        insert_idx = len(lines)
        for i in range(then_line_idx + 1, len(lines)):
            line = lines[i]
            if line.strip() and not line.startswith(' ' * (then_indent + 1)):
                # Found a line that's not part of 'then' section
                insert_idx = i
                break

        # Generate YAML for expectedAuditActions
        if not audit_actions:
            audit_yaml = f"{' ' * field_indent}expectedAuditActions: []"
            lines.insert(insert_idx, audit_yaml)
        else:
            audit_lines = [f"{' ' * field_indent}expectedAuditActions:"]
            for action in audit_actions:
                audit_lines.append(f"{' ' * (field_indent + 2)}- action: \"{action['action']}\"")
                audit_lines.append(f"{' ' * (field_indent + 4)}objectRef:")
                for key, value in action['objectRef'].items():
                    audit_lines.append(f"{' ' * (field_indent + 6)}\"{key}\": \"{value}\"")
                if action['status'] is None:
                    audit_lines.append(f"{' ' * (field_indent + 4)}status: null")
                else:
                    audit_lines.append(f"{' ' * (field_indent + 4)}status: \"{action['status']}\"")

            for line in reversed(audit_lines):
                lines.insert(insert_idx, line)

        # Write back
        with open(file_path, 'w') as f:
            f.write('\n'.join(lines))

        print(f"Updated {file_path} - added {len(audit_actions)} audit action(s)")
        return True

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    # Find all YAML files in scenarios directory
    base_dir = Path(__file__).parent / 'kroxylicious-filters' / 'kroxylicious-authorization' / 'src' / 'test' / 'resources' / 'scenarios'

    if not base_dir.exists():
        print(f"Error: Directory not found: {base_dir}")
        return 1

    yaml_files = list(base_dir.glob('**/*.yaml'))
    print(f"Found {len(yaml_files)} YAML files")

    updated = 0
    for yaml_file in sorted(yaml_files):
        if process_yaml_file(yaml_file):
            updated += 1

    print(f"\nUpdated {updated} files")
    return 0

if __name__ == '__main__':
    sys.exit(main())
