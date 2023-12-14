#!/usr/local/bin/expect
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#


log_file out
set prompt ".*(%|#|\\$) $"          ;# default prompt
catch {set prompt $env(EXPECT_PROMPT)}

spawn bash -i


expect -re $prompt

set send_human {.1 .3 1 .05 2}
send -h "echo hello got prompt\n"

expect -re $prompt
send -h "echo hello got another prompt\n"

expect -re $prompt
send -h "echo hello got another prompt\n"

expect -re $prompt
send -h "echo hello got another prompt\n"


expect -re $prompt
send -h "echo hello got another prompt\n"

send -- "exit\n"
expect eof



