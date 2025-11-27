package main

default allow := false

# TODO: the OPA rule should have a specific shape
# we should define the convention to be used
# this is an initial simplistic approach
permissions = {
    "alice": {
        "my-topic": {},
        "your-topic": {},
    },
    "bob": {
        "my-topic": {
            "describe": true,
            "read": true,
        },
    },
}

action_allowed(action) if {
    permissions[input.subject][action.resource][action.operation]
}

# Build the output structure: lists of allowed and denied actions
# METADATA
# entrypoint: true
result := {
    "subject": input.subject,
    "allowed": [action.operation |
        input.actions[_] = action;
        action_allowed(action)
    ],
    "denied": [action.operation |
        input.actions[_] = action;
        not action_allowed(action)
    ],
}
