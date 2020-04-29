#!/bin/bash

a="mate-terminal --window -t 'Test 1' -e \"bash -c 'echo 1; bash'\""
b="echo yes"
a+=" --tab -t 'TEST 2' -e \"bash -c 'echo 2; bash'\""
#c="$a$b"
eval $a

mate-terminal --window -t 'Test 1' -e "bash -c 'echo 1; bash'" --tab -t 'Test 2' -e "bash -c 'echo 2; bash'"

