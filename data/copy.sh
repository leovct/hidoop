#clear
> filesample$(($1)).txt
#copy i times
for (( i=0; i<$1; i++ ))
do 
	echo "$i"
	cat filesample.txt >> filesample$(($1)).txt
done
