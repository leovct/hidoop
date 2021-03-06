#!/bin/bash
display_usage() {  
	echo -e "usage : /config/exec.sh <serversloginID>" 
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

time=4 #time DataNode and Daemons wait before starting (wait for the NameNode to start)
id=$1
namenode=true
input=config/servers.config
cmd=""
i=1
nbrStart=$(< $input wc -l)
nbrStart=$((nbrStart*2))
while IFS= read -r line
do
	if [ "$namenode" = true ]
	then
		#Run NameNode
	        #mate-terminal --window -t "NameNode $line" -e "ssh $id@$line java -classpath $DATA_FOLDER hdfs.NameNodeImpl"
		echo -e "\033[1;32m[$i/$nbrStart]\033[0m\033[1m starting NameNode on \e[4m$line\033[0m"
		cmd+="mate-terminal --window --hide-menubar -t \"Namenode $line\" -e \"ssh $id@$line 'java -cp $DATA_FOLDER hdfs.NameNodeImpl'\""
		namenode=false
	else
		#Run DataNode
 	        #mate-terminal --tab -t "DataNode $line" -e "ssh $id@$line java -classpath $DATA_FOLDER hdfs.DataNodeImpl $line"
		echo -e "\033[1;32m[$i/$nbrStart]\033[0m\033[1m starting DataNode on \e[4m$line\033[0m"
		cmd+=" --tab -t \"DataNode $line\" -e \"ssh $id@$line 'sleep $time; java -cp $DATA_FOLDER hdfs.DataNodeImpl $line'\""
	fi
	i=$((i+1))
done < "$input"

jobmanager=true
while IFS= read -r line
do
	if [ "$jobmanager" = true ]
	then
		#Run Daemon (new window)
	        #mate-terminal --window -t "Daemon $line" -e "ssh $id@$line java -classpath $DATA_FOLDER ordo.DaemonImpl $line"
		echo -e "\033[1;32m[$i/$nbrStart]\033[0m\033[1m starting JobManager on \e[4m$line\033[0m"
		cmd+=" --window --hide-menubar -t \"JobManager $line\" -e \"ssh $id@$line 'java -cp $DATA_FOLDER ordo.JobManagerImpl'\""
		jobmanager=false
	else
		#Run Daemon (new tab)
	        #mate-terminal --tab -t "Daemon $line" -e "ssh $id@$line java -classpath $DATA_FOLDER ordo.DaemonImpl $line"
		echo -e "\033[1;32m[$i/$nbrStart]\033[0m\033[1m starting Daemon on \e[4m$line\033[0m"
		cmd+=" --tab -t \"Daemon $line\" -e \"ssh $id@$line 'sleep $time; java -cp $DATA_FOLDER ordo.DaemonImpl $line'\""
	fi	
	i=$((i+1))
done < "$input"
eval $cmd
