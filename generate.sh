cp filesample.txt filesample$((2**$1)).txt
for ((n=0;n<$1;n++))
do
cat filesample$($1**$1)$((2**$1)).txt filesample$((2**$1)).txt > temp
mv temp filesample$((2**$1)).txt
done
