#!/bin/bash
display_usage() {  
	echo -e "usage : /config/deploy.sh <serversloginID> [dataPathOnServers]" 
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
rm */*/*.class
javac -d bin */*/*.java
input=config/servers
while IFS= read -r line
do
	#copy compiled .class files on server
  	scp -r bin $id@$line:$DATA_FOLDER
done < "$input"
