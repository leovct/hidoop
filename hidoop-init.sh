#!/bin/bash
display_usage() {  
	echo -e "\033[1musage: ./hidoop-init.sh \033[1;31m<serversloginID>\033[0m [optional:dataPathOnServers]" 
}

display_logo() {
	echo -e '\n\033[34m            __  __  ______   ____    _____   _____   ____    
           /\ \/\ \/\__  _\ /\  _`\ /\  __`\/\  __`\/\  _`\  
           \ \ \_\ \/_/\ \/ \ \ \/\ \ \ \/\ \ \ \/\ \ \ \L\ \
            \ \  _  \ \ \ \  \ \ \ \ \ \ \ \ \ \ \ \ \ \ ,__/
             \ \ \ \ \ \_\ \__\ \ \_\ \ \ \_\ \ \ \_\ \ \ \/ 
              \ \_\ \_\/\_____\\\ \____/\ \_____\ \_____\ \_\ 
               \/_/\/_/\/_____/ \/___/  \/_____/\/_____/\/_/ \n\033[0m'
	echo -e "              \033[1mValentin FLAGEAT, Baptiste GREAUD, Léo VINCENT\n"\
		"                          \033[34mINP ENSEEIHT 2019-2020\033[0m\n\n"
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

#run scripts through command line
chmod u+x config/*.sh hidoop-clear.sh hidoop-run.sh
#run scripts
id=$1
display_logo
config/ssh.sh $id
config/clean.sh $id $DATA_FOLDER
config/deploy.sh $id $DATA_FOLDER
