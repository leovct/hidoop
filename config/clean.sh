#!/bin/bash
display_usage() {  
	echo -e "usage : /config/clean.sh <serversloginID>" 
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

if [ $# -le 1 ] 
then 
	DATA_FOLDER="/work/hidoop-fgvb"
else
	DATA_FOLDER=$2
fi 

id=$1
input=$PWD/config/servers.config
i=1
nbrDeploy=$(< $input wc -l)
while IFS= read -r line
do
	#Delete all files on server
	#Directory to delete is the one specified in 
	#src/config/Project.java, variable DATA_FOLDER
	echo -e "\033[1;31m[$i/$nbrDeploy]\033[0m\033[1m deleting files on \e[4m$line\033[0m"
  	ssh -n $id@$line rm -r $DATA_FOLDER
	i=$((i+1))
done < "$input"
