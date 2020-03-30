#!/bin/bash

# Chemin vers le dossier src/ de l'archive decompressee de notre projet (a modifier)
path="/work/version_courante/src"

# Lancement des serveurs hdfs
mate-terminal --window -t "Truite/HdfsServeur" -e "/bin/bash -c \"ssh truite 'cd $path; java hdfs/HdfsServeur';exec /bin/bash\"" \
               --tab -t "Carpe/HdfsServeur" -e  "/bin/bash -c \"ssh carpe 'cd $path; java hdfs/HdfsServeur';exec /bin/bash\"" \
		--tab -t "Omble/HdfsServeur" -e  "/bin/bash -c \"ssh omble 'cd $path; java hdfs/HdfsServeur';exec /bin/bash\"" \
		 --tab -t "Tanche/HdfsServeur" -e  "/bin/bash -c \"ssh tanche 'cd $path; java hdfs/HdfsServeur';exec /bin/bash\""



# Lancement des démons
mate-terminal --window -t "Truite/DaemonImpl1" -e "/bin/bash -c \"ssh truite 'cd $path; java ordo/DaemonImpl 1';exec /bin/bash\"" \
               --tab -t "Carpe/DaemonImpl2" -e "/bin/bash -c \"ssh carpe 'cd $path; java ordo/DaemonImpl 2';exec /bin/bash\"" \
	        --tab -t "Omble/DaemonImpl3" -e "/bin/bash -c \"ssh omble 'cd $path; java ordo/DaemonImpl 3';exec /bin/bash\"" \
		 --tab -t "Tanche/DaemonImpl4" -e "/bin/bash -c \"ssh tanche 'cd $path; java ordo/DaemonImpl 4';exec /bin/bash\""



# Lancement du NameNode
mate-terminal --window -t "Silure/NameNode" -e "/bin/bash -c \"ssh silure 'cd $path; java hdfs/NameNodeImpl';exec /bin/bash\""

