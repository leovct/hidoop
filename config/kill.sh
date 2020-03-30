#!/bin/bash

# Kill un processus

pid=$(netstat -tlp | grep $1 | awk {'print $7'} | cut -d / -f 1)
kill -9 $pid
