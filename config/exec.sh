#!/bin/bash
display_usage() {  
	echo -e "usage : /config/exec.sh <serversloginID> [dataPathOnServers]" 
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
namenode=true
input=config/servers
cmd=""
while IFS= read -r line
do
	if [ "$namenode" = true ]
	then
		#Run NameNode
	        #mate-terminal --window -t "NameNode $line" -e "ssh $id@$line java -classpath $DATA_FOLDER hdfs.NameNodeImpl"
		cmd+="mate-terminal --hide-menubar --window -t \"Namenode $line\" -e \"ssh $id@$line 'java -cp $DATA_FOLDER hdfs.NameNodeImpl'\""
		namenode=false
	else
		#Run DataNode
 	        #mate-terminal --tab -t "DataNode $line" -e "ssh $id@$line java -classpath $DATA_FOLDER hdfs.DataNodeImpl $line"
		cmd+=" --tab -t \"DataNode $line\" -e \"ssh $id@$line 'java -cp $DATA_FOLDER hdfs.DataNodeImpl $line'\""
	fi
done < "$input"

index=0
while IFS= read -r line
do
if [ $index = 1 ]
then
		#Run Daemon (new window)
	        #mate-terminal --window -t "Daemon $line" -e "ssh $id@$line java -classpath $DATA_FOLDER ordo.DaemonImpl $line"
		cmd+=" --window --hide-menubar -t \"Daemon $line\" -e \"ssh $id@$line 'java -cp $DATA_FOLDER ordo.DaemonImpl $line'\""
elif [ $index -ge 2 ]
then
		#Run Daemon (new tab)
	        #mate-terminal --tab -t "Daemon $line" -e "ssh $id@$line java -classpath $DATA_FOLDER ordo.DaemonImpl $line"
		cmd+=" --tab -t \"Daemon $line\" -e \"ssh $id@$line 'java -cp $DATA_FOLDER ordo.DaemonImpl $line'\""
fi
	index=$(($index+1))
done < "$input"
eval $cmd
