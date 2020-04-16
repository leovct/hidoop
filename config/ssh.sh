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

#generate
ssh-keygen -t rsa

input=$PWD/config/servers
while IFS= read -r line
do
	#copy
	ssh-copy-id $id@$line
done < "$input"