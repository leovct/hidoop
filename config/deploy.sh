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
[ -d "bin" ] || mkdir bin
javac -d bin */*/*.java
cp config/servers.config bin/config/servers.config
input=config/servers
i=1
nbrDeploy=$(< config/servers wc -l)
while IFS= read -r line
do
	# copy compiled .class files on server
	# use java 1.8 !!!
	# sudo update-alternatives --config javac 
	# sudo update-alternatives --config java
	echo -e "\033[1;32m[$i/$nbrDeploy]\033[0m\033[1m deploying on \e[4m$line\033[0m"
  	scp -r bin $id@$line:$DATA_FOLDER
	i=$((i+1))
done < "$input"
