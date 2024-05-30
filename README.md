# PROJET FUN D'ETUDE



## Introduction

Projet fin d'etude de mise en place d'un system d'optimisation de location

## Tool to install

- [ ] Java 8 => [Direct link](https://download.oracle.com/otn/java/jdk/8u202-b08/1961070e4c9b4e26a04e7f5a083f551e/jdk-8u202-windows-x64.exe) + [How to install and setup](https://codewitharjun.medium.com/install-hadoop-on-macos-m1-m2-6f6a01820cc9)
- [ ] Hadoop => [Direct link](https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz) + [How to install and setup](https://www.youtube.com/watch?v=knAS0w-jiUk)
- [ ] Spark => [Direct link](https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz) + [How to install and setup](https://www.youtube.com/watch?v=OmcSTQVkrvo)
- [ ] Kafka => [Direct link](https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz) + [How to install and setup](https://www.youtube.com/watch?v=BwYFuhVhshI)
- [ ] Microsoft Sql Server 2022 => [Direct link](https://download.microsoft.com/download/c/c/9/cc9c6797-383c-4b24-8920-dc057c1de9d3/SQL2022-SSEI-Dev.exe)


## System Environment settings
### Environment Variables
- Create JAVA_HOME variable
- Add the following part to your path:
  - %JAVA_HOME%/bin
- Create HADOOP_HOME Variable
- Add the following part to your path:
  - %HADOOP_HOME%/bin
  - %HADOOP_HOME%/sbin
- Create SPARK_HOME variable
- Add the following part to your path:
  - %SPARK_HOME%/bin

### Workspace settings
```` shell
pip install pipenv
````
Now in your workspace run this command to initiate pipenv and install the libraries needed for the project.(found in Pipfile.lock)
````shell
pipenv install
````
Activate pipenv shell
````shell
pipenv shell
````
To add a package use the following structure
````shell
pipenv install <package name>
````
## How to run the code
### Kafka
#### How to run kafka
* Run 3 CMD as administrator and go to Kafaka folder
  ````shell
  cd <path to your kafka folder>
  ```` 
* Run Zookeeper server
  ````shell
  ./bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties
  ````
* Run Kafka server
  ````shell
  ./bin/windows/kafka-server-start.bat ./config/server.properties
  ````
* Create Topic
  ````shell
  ./bin/windows/kafka-topics.bat --create --bootstrap-server localhost:9092 --topic <Topic Name>
  ````
#### How to stop kafka
* CMD as administrator in kafka folder
* Stop zookeeper server
```` shell
./bin/windows/zookeeper-server-stop.bat ./config/zookeeper.properties
````
* Stop Kafka server
```` shell
./bin/windows/kafka-server-stop.bat ./config/server.properties
````
### Hadoop Distributed File System (HDFS)
#### How to run HDFS
* Each CMD is run as administrator
* Run Namenode and Datanode
```` shell
start-dfs.cmd
````
* Run Yarn
```` shell
start-yarn.cmd
````
* You can check your hadoop cluster on 
`` localhost:9870 `` or `` localhost:50070 ``
* You can check your hadoop ressource manager on
`` localhost:8088 ``
#### How to stop HDFS
```` shell
stop-all.cmd
````

## Useful References
- [Hadoop & spark Tutorial](https://youtube.com/playlist?list=PLJlKGwy-7Ac6ASmzZPjonzYsV4vPELf0x&si=Y-F3MRxEUnhnHfIs)
- [PySpark Tutorial](https://data-flair.training/blogs/pyspark-tutorials-home/)
- [Apache Kafka Tutorial with project](https://data-flair.training/blogs/apache-kafka-tutorial/)
- [Apache Kafka Tutorial step by step](https://www.tutorialspoint.com/apache_kafka/apache_kafka_installation_steps.htm)