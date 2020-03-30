#!/bin/bash

# On recompile le projet
javac */*/*.java

# On déploie le projet sur les serveurs
scp -r -p bgreaud@ohm:/work/version_courante bgreaud@truite:/work
scp -r -p bgreaud@ohm:/work/version_courante bgreaud@tanche:/work
scp -r -p bgreaud@ohm:/work/version_courante bgreaud@carpe:/work
scp -r -p bgreaud@ohm:/work/version_courante bgreaud@omble:/work
scp -r -p bgreaud@ohm:/work/version_courante bgreaud@silure:/work
