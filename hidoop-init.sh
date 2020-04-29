#!/bin/bash
display_usage() {  
	echo -e "usage : ./hidoop-init.sh <serversloginID> [optional:dataPathOnServers]" 
}

display_logo() {
	echo -e "\n ####################################################\n"\
		"#                                                  #\n"\
		"#     \033[1;34m*   *  *****  ***    *****  *****  *****     \033[0m#\n"\
		"#     \033[1;34m*   *    *    *  *   *   *  *   *  *   *     \033[0m#\n"\
		"#     \033[1;34m*****    *    *   *  *   *  *   *  *****     \033[0m#\n"\
		"#     \033[1;34m*   *    *    *  *   *   *  *   *  *         \033[0m#\n"\
		"#     \033[1;34m*   *  *****  ***    *****  *****  *         \033[0m#\n"\
		"#                                                  #\n"\
		"####################################################"
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
display_logo
config/clean.sh $id $DATA_FOLDER
config/deploy.sh $id $DATA_FOLDER
