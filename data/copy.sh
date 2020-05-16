#!/bin/bash
display_usage() {  
	echo -e "\033[1musage: /data/copy.sh \033[1;31m<file> <nbrTimes>\033[0m" 
}

# check whether user had supplied -h or --help, if yes display usage 
if [[ ( $# == "--help") ||  $# == "-h" ]] 
then 
	display_usage
	exit 0
fi

# if no (or only one) argument(s) supplied, display usage 
if [ $# -le 1 ] 
then 
	display_usage
	exit 1
fi

# copy file n times into file$n
> $1$(($2))
for (( i=0; i<$2; i++ ))
do
	cat $1 >> $1$(($2))
done
echo -e "File $1 copied \033[1;32m$2 times\033[0m into \033[1;32m$1$2\033[0m"
