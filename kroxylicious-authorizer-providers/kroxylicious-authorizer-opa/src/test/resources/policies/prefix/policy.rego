# Policy that supports prefix-based resource matching
# Users can be configured with resource name prefixes they own

package kroxylicious.abac

default allow := false

# METADATA
# entrypoint: true
allow if {
	user_is_admin
	action_allowed_for_admin
}

allow if {
	user_is_owner_by_prefix
	action_allowed_for_owner
}

allow if {
	user_is_subscriber_by_prefix
	action_allowed_for_subscriber
}

user_is_admin if {
	count([p |
		some p in input.subject.principals
		p.type == "User"
		p.name == "admin"
	]) > 0
}

# Check if user owns a resource by prefix matching
user_is_owner_by_prefix if {
	some prefix in data.prefixes
	some p in input.subject.principals
	p.type == "User"
	p.name == prefix.owner
	startswith(input.resourceName, prefix.prefix)
}

# Check if user is subscriber by prefix matching
user_is_subscriber_by_prefix if {
	some prefix in data.prefixes
	some p in input.subject.principals
	p.type == "User"
	count([s |
		some s in prefix.subscribers
		s == p.name
	]) > 0
	startswith(input.resourceName, prefix.prefix)
}

action_allowed_for_admin if input.action in {"create", "delete"}
action_allowed_for_owner if input.action in {"read", "write", "describe", "describe_configs", "alter", "alter_configs"}
action_allowed_for_subscriber if input.action in {"read", "describe"}

