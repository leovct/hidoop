#!/bin/bash
display_usage() { 
	echo -e "\033[1musage: ./config/ssh.sh \033[1;31m<serversloginID>\033[0m"
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

input=$PWD/config/servers.config
i=1
nbrAdd=$(< $input wc -l)
while IFS= read -r line
do
	#add computer @line to the fingerprint
	echo -e "\033[1;32m[$i/$nbrAdd]\033[0m\033[1m adding public key to \e[4m$line\033[0m\033[1m\033[0m"
	#ssh-keyscan -H $line >> ~/.ssh/known_hosts
	ssh-copy-id $id@$line #add the public key to all the servers in the cluster
	i=$((i+1))
done < "$input"
