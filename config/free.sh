#!/bin/bash

# Chemin vers le projet
path="/work/version_courante"

# Arrêt du NameNode
ssh -t silure "cd /; cd $path; \
	./config/kill.sh 4023" &

# Arrêt des démons sur Truite
ssh truite "cd /; cd $path; \
	./config/kill.sh 4698; \
	./config/kill.sh 4321" &

# Arrêt des démons sur Carpe
ssh carpe "cd /; cd $path; \
	./config/kill.sh 4698; \
	./config/kill.sh 4321" &

# Arrêt des démons sur Tanche
ssh tanche "cd /; cd $path; \
	./config/kill.sh 4698; \
	./config/kill.sh 4321" &

# Arrêt des démons sur Omble
ssh omble "cd /; cd $path; \
	./config/kill.sh 4698; \
	./config/kill.sh 4321" 

echo "Les ports 4023, 4698 et 4321 sont libres"
