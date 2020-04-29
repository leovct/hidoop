#!/bin/bash
display_usage() {  
	echo -e "usage : /config/kill.sh <serversloginID>" 
	}

# check whether user had supplied -h or --help, if yes display usage 
if [[ ( $# == "--help") ||  $# == "-h" ]] 
then 
	display_usage
	exit 0
fi

# if no arguments supplied, display usage 
if [ $# -le 0 ] 
then 
	display_usage
	exit 1
fi

id=$1
namenode=true
input=$PWD/config/servers
while IFS= read -r line
do
	if [ "$namenode" = true ]
	then
		#Kill NameNodeImpl java programs
  		ssh -n $id@$line pkill -9 -f NameNodeImpl
  		namenode=false
  	else
		#Kill DataNodeImpl and DaemonImpl java programs
  		ssh -n $id@$line pkill -9 -f DataNodeImpl
  		ssh -n $id@$line pkill -9 -f DaemonImpl
	fi
done < "$input"