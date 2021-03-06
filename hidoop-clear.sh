#!/bin/bash
display_usage() {
	echo -e "\033[1musage: ./hidoop-clear.sh \033[1;31m<serversloginID>\033[0m" 
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

chmod u+x config/*.sh
id=$1
config/kill.sh $id
config/clean.sh $id $DATA_FOLDER
