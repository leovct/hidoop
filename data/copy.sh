#clear
> filesample$(($1)).txt
#copy i times
for (( i=0; i<$1; i++ ))
do 
	cat filesample.txt >> filesample$(($1)).txt
done
echo -e "\033[1;32m[copy done]\033[0m\033[1m filesample$1.txt generated (contains $1 times filesample.txt)"
