# Policy that supports regex-based resource matching
# Users can be configured with regex patterns for resources they own

package kroxylicious.abac

import rego.v1

default allow := false

# METADATA
# entrypoint: true
allow if {
	user_is_admin
	action_allowed_for_admin
}

allow if {
	user_is_owner_by_regex
	action_allowed_for_owner
}

allow if {
	user_is_subscriber_by_regex
	action_allowed_for_subscriber
}

user_is_admin if {
	count([p |
		some p in input.subject.principals
		p.type == "User"
		p.name == "admin"
	]) > 0
}

# Check if user owns a resource by regex matching
user_is_owner_by_regex if {
	some pattern in data.patterns
	some p in input.subject.principals
	p.type == "User"
	p.name == pattern.owner
	regex.match(pattern.pattern, input.resourceName)
}

# Check if user is subscriber by regex matching
user_is_subscriber_by_regex if {
	some pattern in data.patterns
	some p in input.subject.principals
	p.type == "User"
	count([s |
		some s in pattern.subscribers
		s == p.name
	]) > 0
	regex.match(pattern.pattern, input.resourceName)
}

action_allowed_for_admin if input.action in {"create", "delete"}
action_allowed_for_owner if input.action in {"read", "write", "describe", "describe_configs", "alter", "alter_configs"}
action_allowed_for_subscriber if input.action in {"read", "describe"}

