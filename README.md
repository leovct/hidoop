<div align="center">
    <img src="img/header.png" />
</div>

![Language Java](https://img.shields.io/badge/Language-Java-B07219 "Language") ![Status Under Development](https://img.shields.io/badge/Status-Under%20Development-brightgreen "Status")

**Hidoop is a platform developed in Java, allowing the execution of applications based on MapReduce programming model on a computer cluster.**  

This project is our very first work on the theme of competing applications for intensive computing and mass data processing.  
It consists in a lite version of [Hadoop](https://hadoop.apache.org/) (developed by Apache) and it is composed of two modules :
* :file_folder: **A distributed file-system** managing data storing on a cluster - _inspired by **H**adoop **D**istributed **F**ile **S**ystem (HDFS)_
* :diamond_shape_with_a_dot_inside: **An implementation of the MapReduce programming model** for large-scale data processing - _inspired by Hadoop MapReduce_

## Table of contents
<details>
<summary>Click to expand</summary>

- [**Overview**](#overview)
- [**Getting started** :pushpin:](#getting-started-pushpin)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
    - [Configuration](#configuration)
- [**Run Hidoop** :rocket:](#run-hidoop-rocket)
    - [Deployment](#deployment)
    - [Launching](#launching)
    - [Shutdown](#shutdown)
- [**Spread data on the cluster using HDFS** :file_folder:](#spread-data-on-the-cluster-using-hdfs-file_folder)
    - [Data Format](#data-format)
    - [Write a file on HDFS](#write-a-file-on-hdfs)
    - [Read a file from HDFS](#read-a-file-from-hdfs)
    - [Delete a file from HDFS](#delete-a-file-from-hdfs)
- [**Run a MapReduce application on the cluster** :diamond_shape_with_a_dot_inside:](#run-a-mapreduce-application-on-the-cluster-diamond_shape_with_a_dot_inside)
    - [WordCount application](#wordcount-application)
    - [QuasiMonteCarlo application](#quasimontecarlo-application)
    - [PageRank application](#pagerank-application)
- [**How Hidoop works :gear:**](#how-hidoop-works-gear)
    - [Details on HDFS](#details-on-hdfs)
    - [Details on the implementation of the MapReduce concept](#details-on-the-implementation-of-the-mapreduce-concept)
- [**Next development stages :bulb:**](#next-development-stages-bulb)
- [**Contributors** :busts_in_silhouette:](#contributors-busts_in_silhouette)
</details>

## Overview

This application allows its user to **process large data sets across multiple servers** using the MapReduce programming model.  
This programming model is used to **parallelize the processing of large-scale data** among a cluster.
Each server from the cluster processes a small part of data.

In this project, processing data takes place in 3 steps :  
* Data (provided as a file) is cut into small chunks and spread over the servers (process managed by __HDFS__)
* Each server processes a small part amount data (__Map__)
* Server results are collected and aggregated (__Reduce__)

__Map__ and __Reduce__ processes depend on the purpose of the MapReduce application, and can be entirely designed by user.
The strength of this model lies in the parallelization of processes.  
For example, MapReduce programming model can be use to parallelize the counting of the number of occurrences of a specific word in a large dataset.

## Getting started :pushpin:

### Prerequisites

* Current version must be run on a **Linux** system.
* Current version also needs [MATE Terminal](https://mate-desktop.org/) to be installed on the system.  
Indeed, starting the current version causes the opening of different terminals allowing the monitoring of the activity of the different services.
* All servers (or machine) must be in a same **network**, **accessible by the machine running the project**.
* Servers need to be accessible via **SSH**.
* All servers must have a **recent version of Java** (1.8 or more recent).
* Java compiler used must produce code that is **executable by all servers**.

### Installation

Project must be cloned on a **Linux** system, which will be responsible for the deployment of the platform within the cluster.

### Configuration

#### Servers addresses

Servers that are part of the cluster must be filled in file _config/servs.config_.
Addresses must be written line by line as in the example below :

>__salameche.enseeiht.fr__  
>carapuce.enseeiht.fr  
>bulbizarre.enseeiht.fr  
>nidoran.enseeiht.fr  
>magicarpe.enseeiht.fr  

Server **written first** (at the top of the list) will automatically be set as **Master Server**, i.e. the server running central processes (NameNode and JobManager).

#### Other settings

Following settings can be configured in file _congif/settings.config_ ([KV format](#kv-format) file).  
These setting have to be written key-value pairs (watch out for white spaces), in any order, according to the following indications :

* **Chunk size** (___chunksize___ key, _strictly positive integer_) : size of chunks (Mo) when files are written on HDFS
* **Data path on servers** (___datapath___ key, _string_) : path to folder where data will be stored (Hidoop executables and data processed) **on each server**
* **NameNode's port** (___portnamenode___ key, _strictly positive integer_) : port used by NameNode
* **DataNodes' port** (___portdatanode___ key, _strictly positive integer_) : port used by DataNodes
* **JobMaster's port** (___portjobmaster___ key, _strictly positive integer_) : port used by JobMaster (also called JobManager)
* **Daemons' port** (___portdaemon___ key, _strictly positive integer_) : port used by Daemons

See the example below :

>chunksize<->128  
>datapath<->/work/hidoop/  
>portnamenode<->4023  
>portdatanode<->4027  
>portjobmaster<->4985  
>portdaemon<->4321

## Run Hidoop :rocket:

Once the project is configured (see section above), running the platform is a **2-step procedure**.  
First executable files have to be **deployed** on the cluster, then the platform has to be **launched**.

### Deployment

Open a terminal in project's **root folder** and execute _hidoop-init.sh_ bash script by typing the following command :  
```
./hidoop-init.sh <username>
```
**\<username\>** has to be replaced with the username to be used for SSH connection on servers.  
_Note : SSH command used within bash scripts is `ssh <username>@<serveraddress>`, e.g. `ssh lvincent@salameche.enseeiht.fr`_.

A logo signalling the begining of the deployment should appear.  
**Deployment might take a few time** : executable files are copied on each machine of the cluster.

<br/>
<div align="center">
  <img src="img/hidoop-init.png" alt="Screenshot showing execution of hidoop-init.sh script" width=500/>
</div>
<br/>

### Launching

In a terminal in project's **root folder**, execute _hidoop-run.sh_ bash script by typing the following command :  
```
./hidoop-run.sh <username>
```
Same remark as for the deployment, **\<username\>** has to be replaced with the username to be used for SSH connection on servers.

After a few seconds, **two terminals** with tabs should show-up as in the picture below, **displaying the status of the different entities of Hidoop** running on servers.  
On the first terminal, you can track the status of the **NameNode** and the four **DataNodes**.  
On the second terminal, you can track the status of the **JobManager** and the four **Daemons**.   
The role and functioning of these entities is explained in more detail in [this section](#how-the-hidoop-application-works-gears) for the more curious.

<br/>
<div align="center">
  <img src="img/hidoop-run.png" alt="Screenshot showing execution of hidoop-run.sh script" width=1000/>
</div>
<br/>

**Hidoop is now ready for use !**

## Shutdown

To clean up everything related to Hidoop from the cluster (binaries, data and Java processes), execute _hidoop-clear.sh__ bash script from project's root folder :  

```
./hidoop-clear.sh <username>
```
Same remark as for the deployment, **\<username\>** has to be replaced with the username to be used for SSH connection on servers.
The Java processes of NameNode, DataNodes, JobManager and Daemons should be killed and the data stored on each server should be erased.


## Spread data on the cluster using HDFS :file_folder:

To run MapReduce applications processing data, **the data must first be distributed on the cluster** using **HDFS**. Data is cut into small chunks and spread over the servers.  

In this project, HDFS provides 3 main functionnalities :
* **Write data :** cut a provided file into chunk and spread them over the servers
* **Read data :** retrieve data that has been written on servers by _write_ process in order to rebuild the original file
* **Delete data :** delete data stored by servers concerning a specified file

The following instructions can only work if Hidoop platform is running on the cluster (see section above).  

:warning: ___This project is still under development, please do not try to proceed any sensitive data without making a copy.___


### Data Format

Data to process must be stored as a file. Current version of the project supports two types of format : Line and KV.

#### Line format
In Line format, text is written **line by line**. A line of text is considered a unit of data, so lines' length should not be too disparate.  

Here is an example of Line format file content :  
> This is the content of the file.  
> It should be written line by line,  
> lines' length should not be too disparate,  
> file can be as large as desired.  

#### KV format
In KV format, text file is composed of key-value pairs. Each pair is written **on a line**, separator is *\<-\>* symbol.  

Here is an example of KV format file content :  
>movie<->inception  
>series<->game-of-thrones  
>dish<->pizza  
>colour<->blue  

### Write a file on HDFS

To write a large file in HDFS (corresponds to **spreading data among servers**), open a terminal in project's **root folder** and execute following command :
```
java -cp bin hdfs.HdfsClient write <line|kv> <sourcefilename> [replicationfactor]
```
* *\<line|kv\>* corresponds to input file format, line or KV format.  
* *\<sourcefilename\>* is the name of the file to proceed.
* *\[replicationfactor\]* is an **optional** argument. It corresponds to the replication factor of the file, i.e. the number of time each chunk is duplicated on the cluster, in order to anticipate server failures. **Default value is 1**.

_Note : -cp is a shortcut for -classpath._

### Read a file from HDFS

To read a file from HDFS (corresponds to **retrieving data from servers**), open a terminal in project's **root folder** and execute following command :
```
java -cp bin hdfs.HdfsClient read <filename> <destfilename>
```
* *\<sourcefilename\>* is the name of the file (written on HDFS previously) to read from the servers.
* *\<destfilename\>* is the name of the file to store data retrieved by process (rebuilt file).

### Delete a file from HDFS

To delete a file from HDFS (corresponds to **deleting data from servers**), open a terminal in project's **root folder** and execute following command :
```
java -cp bin hdfs.HdfsClient delete <sourcefilename>
```
* *\<sourcefilename\>* is the name of the file (written on HDFS previously) to delete from HDFS.

## Run a MapReduce application on the cluster :diamond_shape_with_a_dot_inside:

MapReduce application models are given in _src/application_ package.  

:warning: **The following instructions can only work if Hidoop platform is running on the cluster and the data is spread within the cluster.**

_Note : It is also possible to run MapReduce applications that do not take a file as a parameter_
_(for example QuasiMonteCarlo application that generates a number of points in a unitary square and which calculates the number of points inside the circle inscribed in the square in order to approach the value of pi)_.

### WordCount application

**Wordcount** is a MapReduce application runnable on Hiddop platform.  
This application counts the number of occurrences of each word in a large text file in Line format.  
The application is located in package _src/application/_.

_Note : We have also implemented an iterative version to compare the differences of performance on very large files (> 10 GB)._

Make sure you have written the file in HDFS before launching the application. Then, execute following code from a terminal opened in project's **root folder** :
```
java -cp bin application.WordCount_MapReduce <filename>
```
* *\<filename\>* is the name of the file (written on HDFS previously) to process.

> Example of use : 
>
> Let's say you would want to count the number of occurrences of all every word in a large 50GB text file, stored on your system at data/filesample.txt.  
> The first step is to write the file in HDFS :  
> `java -cp bin hdfs.HdfsClient write line data/filesample.txt 1`  
> The next step is to run the WordCount application by specifying the file name (without the path because HDFS is a flat hierarchy) :  
> `java -cp bin application.WordCount_MapReduce filesample.txt`  
> The result of the process is written in the file _results/resf-filesample.txt_, in KV format.  

_Note : It is possible to compare the performances of MapReduce applications with their iterative versions, also present in the package *src/application*._  
_Keep in mind that *time savings will only be noticeable on very large files (> 10 Go)*, not taking into account time spent writing files on HDFS._
_Indeed, the MapReduce process is quite expensive and is only useful on large data sets._  
***:warning: This project is still under development, the results may not be very significant at this time.***

### QuasiMonteCarlo application

We have developed a MapReduce version of the **QuasiMonteCarlo** algorithm.  
It generates a number of X points in a unit square during the Map operation and then calculates the number of points inside the circle inscribed in this square during the Reduce operation.  
It then derives an approximation of 𝝅.

This application is **complementary to the WordCount application** because it does not take a file as a parameter on which to execute operations.  
Moreover, the application does not manage the distribution of Map and Reduce operations on the cluster in the same way.  
Indeed, since no Map operation is performed on chunks because the application does not take a file in parameter, a Map and Reduce operation is launched on each available daemon.

To launch the QuasiMonteCarlo application, simply and run the following command from project's root folder :
```
java -cp bin application.QuasiMonteCarlo_MapReduce
```
_Note : By default, the application generates 10⁶ points per Daemon._

### PageRank application

The **PageRank** application is based on Google's page ranking algorithm developed by Larry Page. This algorithm allows to rank web pages in Google's search results. 

The principle is to assign to each page a value (or page rank) proportional to the number of times a user would pass by this page while surfing on the Web (by clicking on links on other pages). For more details on this algorithm, don't hesitate to watch this [Youtube video series](https://www.youtube.com/watch?v=9e3geIYFOF4&loop=0).

_Note : The performance gain is visible only if you run the algorithm on **large graphs** (the web is huge, hence the usefulness of the MapReduce version)._

In the data/ folder, we have provided you with two files small-network and big-network which represent small and medium size networks (4 pages for the first one and 26 for the second one).
The application runs on files in KV format, for example :

>A<->B,C  
>B<->A  
>C<->A,B  

This file represents a graph consisting of 3 nodes (pages). A page A which contains links to pages B and C. A page B which redirects to page A and finally a page C which leads to pages A and B. We did not use jsoup to analyze existing web pages but it could be an idea for improvement.

To launch the PageRank application, simply go to the prject's root folder and run the following command : 
```
java -cp bin application.PageRank_MapReduce <filename> <numberiterations>
```
* *\<filename\>* is the network on which you want to calculate page rank values.
* *\<numberiterations\>* is the number of iterations you want to run. The higher the number, the more the algorithm converges.

> Example of use: 
>
> To run the PageRank algorithm on the small network, execute the command :  
> ```java -cp bin application.PageRank_MapReduce data/small-network 3```  
> Note: Very few iterations are needed to converge in such a simple case (here, 3 iterations is enough to observe convergence).

## How Hidoop works :gear:

### Details on HDFS

The proposed implementation of the HDFS service is composed of 2 main entities, *NameNode* and *DataNode*.  
It also provides a class with static methods to perform all possible operations on the file system, *HdfsClient*.  

The ***NameNode*** is the Java process running on the **master server of the cluster**.  
It supervises the entire network, stores information about the files stored on the file system (metadata and file chunks locations), and provides an interface to make queries on the data.  

The ***DataNode*** is the Java process running on each of the cluster's **slave servers**.  
It supervises the data stored on the server (chunks), performing the operations controlled by the NameNode (receiving/sending/deleting chunks).

The processes are inter-connected by **RMI**, and exchange data via **Socket in TCP mode**.  
The 3 possible operations on the HDFS file system are those performed by the static methods of the *HdfsClient* class, namely :

* **HdfsWrite** : writes a file on the file system. The file is split into chunks, and each chunk is sent to one or more *DataNode* (according to the replication factor).
The choice of the *DataNode* receiving the chunk is determined by the *NameNode*.  

* **HdfsRead** : reads a file on HDFS. The NameNode provides locations of the different chunks composing the file.
For each chunk, the client tries to retrieve data from at least one of the servers containing a copy.
A connection is established with the DataNode of the server, which provides the content of the chunk to the client.  

* **HdfsDelete** : deletes a file from HDFS. The NameNode provides locations of the different chunks composing the file. 
For each chunk, the client sends a request to the corresponding DataNode, asking for data deletion.
The DataNode deletes the chunk and notifies the NameNode.  

### Details on the implementation of the MapReduce concept

The implementation of the MapReduce concept is composed of 2 major entities: **JobManager** and **Daemon**.

The **JobManager** is the Java process, that has a similar role to the *NameNode* but on the application side.
It supervises all the *Daemons* Map and Reduce's ongoing operations within the cluster.
It also keeps track of the *Jobs* launched and would allow, in a future version, to manage failures during a Map operation.

The **Daemon** is the Java process that execute an action defined by the **Map** operation.
The results of each map will be aggregated and returned to the client thanks to the **Reduce** operation.

A **Job** executes the Map and Reduce methods of a MapReduce application (*i.e. WordCount_MapReduce*) within the cluster.
It will retrieve the list of *Daemons* available thanks to the *JobManager*.
It will also retrieve the list of fragments if the application requires an input file, written in *HDFS*. 
Then the Job will execute the Map operations and ask the *JobManager* for the progress of operations until all the maps are done.
At the very end, it will read the result file of the map thanks to *HDFS* and start the Reduce operation.

## Next development stages :bulb:

### HDFS service
* **Parallelization of client-side operations** *(HdfsClient)*.
* **Cluster monitoring** improvement *(NameNode)*.
* **Implementation of an HDFS Shell**, facilitating the use of the file system.
* **Global architecture improvement**.
* **Implementation of new file formats**.

### MapReduce concept implementation 
* **Parallelization of the Reduce** operation.
* **Manage breakdowns** during the course of a MapReduce application.
* **Optimize the distribution of resources** according to the CPU used for example (currently distributed according to the number of maps per server).
* **Make sure that the Job is executed on a cluster node** and not on the client machine (to get closer to the real *Hadoop* architecture).
* **Improve the Job interface** to make it more generic and accessible to other types of MapReduce applications.

### MapReduce applications developed
* In the PageRank application, **add a whole part allowing to analyze a web page with [Jsoup](https://jsoup.org/)** in order to find all the links (existing web pages and web pages which do not exist any more).
* **Develop more applications** based on the MapReduce concept (i.e. algorithms on the exact coverage like *Donald Knuth's DLX* algorithm allowing to solve sudokus or other puzzles of that kind).

## Contributors :busts_in_silhouette:

[Valentin Flageat](https://github.com/vaflag) - [Baptiste Gréaud](https://github.com/bgreaud) - [Léo Vincent](https://github.com/leovct)