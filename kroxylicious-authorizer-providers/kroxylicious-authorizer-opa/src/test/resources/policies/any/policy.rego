# Policy that supports any resource access
# Users can be configured to access any resource

package kroxylicious.abac

default allow := false

# METADATA
# entrypoint: true
allow if {
	user_is_admin
	action_allowed_for_admin
}

allow if {
	user_can_access_any_resource
	action_allowed_for_owner
}

allow if {
	user_is_subscriber_any
	action_allowed_for_subscriber
}

user_is_admin if {
	count([p |
		some p in input.subject.principals
		p.type == "User"
		p.name == "admin"
	]) > 0
}

# Check if user can access any resource
user_can_access_any_resource if {
	some p in input.subject.principals
	p.type == "User"
	count([u |
		some u in data.anyResourceUsers
		u == p.name
	]) > 0
}

# Check if user is subscriber for any resource
user_is_subscriber_any if {
	some p in input.subject.principals
	p.type == "User"
	count([u |
		some u in data.anyResourceSubscribers
		u == p.name
	]) > 0
}

action_allowed_for_admin if input.action in {"create", "delete"}
action_allowed_for_owner if input.action in {"read", "write", "describe", "describe_configs", "alter", "alter_configs"}
action_allowed_for_subscriber if input.action in {"read", "describe"}

