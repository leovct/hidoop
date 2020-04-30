#!/bin/bash
display_usage() {  
	echo -e "usage : /config/ssh.sh <serversloginID>" 
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

input=$PWD/config/servers
i=1
nbrAdd=$(< config/servers wc -l)
while IFS= read -r line
do
	#add computer @line to the fingerprint
	echo -e "\033[1;32m[$i/$nbrAdd]\033[0m\033[1m adding \e[4m$line\033[0m\033[1m to .ssh/known_hosts\033[0m"
	ssh-keyscan -H $line >> ~/.ssh/known_hosts
	# ssh -o StrictHostKeyChecking=no ?
	i=$((i+1))
done < "$input"
