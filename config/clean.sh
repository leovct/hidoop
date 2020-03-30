#!/bin/bash

# Chemin vers le projet
path="/work/version_courante/src"

# Clean sur Truite
ssh truite "cd /; cd $path; \
	rm -f filesample*" &


# Clean sur Carpe
ssh carpe "cd /; cd $path; \
	rm -f filesample*" &


# Clean sur Tanche
ssh tanche "cd /; cd $path; \
	rm -f filesample*" &


# Clean sur Omble
ssh omble "cd /; cd $path; \
	rm -f filesample*" &

echo "Les fichiers commençant par filesample ont été supprimés !!"
