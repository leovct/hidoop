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
i=1
nbrKill=$(< config/servers wc -l)
while IFS= read -r line
do
	if [ "$namenode" = true ]
	then
		#Kill NameNodeImpl java programs
		echo -e "\033[1;31m[$i/$nbrKill]\033[0m\033[1m killing NameNode java process on \e[4m$line\033[0m"
  		ssh -n $id@$line pkill -9 -f NameNodeImpl
  		namenode=false
  	else
		#Kill DataNodeImpl and DaemonImpl java programs
		echo -e "\033[1;31m[$i/$nbrKill]\033[0m\033[1m killing DataNode & Daemon java processes on \e[4m$line\033[0m"
  		ssh -n $id@$line pkill -9 -f DataNodeImpl
  		ssh -n $id@$line pkill -9 -f DaemonImpl
	fi
	i=$((i+1))
done < "$input"
