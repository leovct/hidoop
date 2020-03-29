# Hidoop
> Contributeurs : Clément Barioz, Valentin Flageat, Bptiste Gréaud, Léo Vincent
Ce projet propose une première expérience sur le thème des applications concurrentes pour le calcul intensif et le traitement de masses de données. Pour cela, il s’agira :
* De réaliser une plateforme comportant un système de fichiers adapté au traitement concurrent de masses de données, ainsi qu’un noyau d’exécution axé sur l’ordonnancement et la gestion de tâches selon le schéma « diviser pour régner » (map-reduce). L’architecture et les fonctionnalités de cette plateforme reprendront (de manière simplifiée) celles de la plateforme Hadoop.
* De permettre de tester et évaluer cette plateforme
* De développer des outils d’instrumentation et de supervision de l’exécution des applications tournant sur la plateforme.
* Au travers de son utilisation par des applications concurrentes utilisant de manière intensive les ressources mémoire et/ou de calcul

## Utilisation
** A compléter**



## Recensement des commandes principales de GIT utiles pour ce projet
> Valentin Flageat, v1

### git add <path>
Ajoute les fichiers spécifiés au "pre-commit"

### git reset HEAD
Revient en arrière et calque le "pre-commit" sur le HEAD commit (dernier commit), annule donc tous les "git add"

### git status
Affiche une comparaison du projet avec le HEAD commit (dernier commit)

### git checkout <path>
Remplace les fichiers spécifiés par ceux contenus dans le HEAD commit (dernier commit)

### git commit -m "message"
Enregistre les fichiers ajoutés au "pre-commit", le message doit stipuler les changements apportés par cette version

### git push
Envoie les données sur le repository (serveur distant GitLab)

### git pull
Récupère les données du repository (serveur distant GitLab)

### git log
Affiche l'historique des "commit"

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

### git pull origin <branch>
Récupérer les fichiers de la branche spécifiée (saisir master pour la première utilisation) du répertoire distant

### #InitialisationSeulement# git remote add origin https://gitlab.enseeiht.fr/vflageat/Hidoop.git
Connecte le projet local au repository (répertoire du projet) présent sur Gitlab

### #InitialisationSeulement# git remote
Affiche les dépôts distants auxquels le projet est connecté (devrait contenir "origin" uniquement)

### #InitialisationSeulement# git push origin master
Dépose le projet sur le repository (répertoire distant du projet) 



## Template pour README.md
https://gist.github.com/PurpleBooth/109311bb0361f32d87a2