#!/bin/bash
display_usage() {  
	echo -e "usage : /config/clean.sh <serversloginID> [dataPathOnServers]" 
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
input=$PWD/config/servers
while IFS= read -r line
do
	#Delete all files on server
	#Directory to delete is the one specified in 
	#src/config/Project.java, variable DATA_FOLDER
  	ssh -n $id@$line rm -r $DATA_FOLDER
done < "$input"
