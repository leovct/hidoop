#!/bin/bash

# ajout des cles pour les ssh
ssh-keygen -t rsa

# 1 namenode (catalogue)
ssh-copy-id silure 

# 3 datanode (demon+hdfsServeur)
ssh-copy-id truite 
ssh-copy-id carpe 
ssh-copy-id omble 
ssh-copy-id tanche
