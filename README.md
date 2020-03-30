# HIDOOP
> Contributeurs : Clément Barioz, Valentin Flageat, Baptiste Gréaud, Léo Vincent

Ce projet propose une première expérience sur le thème des applications concurrentes pour le calcul intensif et le traitement de masses de données. Pour cela, il s’agira :
* De réaliser une plateforme comportant un système de fichiers adapté au traitement concurrent de masses de données, ainsi qu’un noyau d’exécution axé sur l’ordonnancement et la gestion de tâches selon le schéma « diviser pour régner » (map-reduce). L’architecture et les fonctionnalités de cette plateforme reprendront (de manière simplifiée) celles de la plateforme Hadoop.
* De permettre de tester et évaluer cette plateforme
* De développer des outils d’instrumentation et de supervision de l’exécution des applications tournant sur la plateforme.
* Au travers de son utilisation par des applications concurrentes utilisant de manière intensive les ressources mémoire et/ou de calcul

## Utilisation
**A compléter**

## Fonctionnement
**A détailler?**  
Exemple avec l'application MyMapReduce
* Une fois l'environnement initialisé, l'application est lancée en éxécutant le main de sa classe Java en ligne de commande (java MyMapReduce <nomfichier>).
* Le main instancie un objet de la classe Job, paramètre son _InputFormat_ et son _InputFname_ puis éxécute sa méthode _jobStart(m)_ en passant en paramètre une instance de la classe de l'application : _j.startJob(new MyMapReduce())_
*  

## Recensement des commandes principales de GIT utiles pour ce projet
> Auteur : Valentin Flageat, v1

### git add <path>
Ajoute les fichiers spécifiés au "pre-commit"

### git commit -m "message"
Enregistre les fichiers ajoutés au "pre-commit", le message doit stipuler les changements apportés par cette version

### git push
Envoie les données sur le repository (serveur distant GitLab)

### git pull
Récupère les données du repository (serveur distant GitLab)

### git branch <branchname>
Crée d'une branche dans le projet

### git branch <branchname>
Affiche la liste des branches du projet

### git checkout <branchname>
Change la branche courante

### git merge <branchname>
Rapatrie la branche spécifiée sur la branche courante, les deux branches sont fusionnées

### git merge -d <branchname>
Supprime la branche spécifiée

### git reset HEAD
Revient en arrière et calque le "pre-commit" sur le HEAD commit (dernier commit), annule donc tous les "git add"

### git status
Affiche une comparaison du projet avec le HEAD commit (dernier commit)

### git checkout <path>
Remplace les fichiers spécifiés par ceux contenus dans le HEAD commit (dernier commit)

### git log
Affiche l'historique des "commit"

### git pull origin <branch>
Récupère les fichiers de la branche spécifiée (saisir master pour la première utilisation) du répertoire distant

### git clone git@gitlab.enseeiht.fr:2SN/Hidoop.git
Récupère une copie du projet complet

### #InitialisationSeulement# git remote add origin git@gitlab.enseeiht.fr:2SN/Hidoop.git
Connecte le projet local au repository (répertoire du projet) présent sur Gitlab

### #InitialisationSeulement# git remote
Affiche les dépôts distants auxquels le projet est connecté (devrait contenir "origin" uniquement)

### #InitialisationSeulement# git push origin master
Dépose le projet sur le repository (répertoire distant du projet) 


## Architecture HDFS
http://lowemarius.over-blog.com/2017/08/comprendre-l-architecture-hdfs.html  
https://www.edureka.co/blog/apache-hadoop-hdfs-architecture/  
https://data-flair.training/blogs/hadoop-architecture/  

## Template pour README.md
https://gist.github.com/PurpleBooth/109311bb0361f32d87a2