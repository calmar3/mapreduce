## Quickstart

`docker pull calmar3/sabd-mapred`

## Manual Configuration

### Download

Run `git clone https://github.com/calmar3/mapreduce`

Download hbase release 1.2.6 from [HBase download section](http://www.apache.org/dyn/closer.cgi/hbase/)

Download flume release 1.7.0 from [Flume download section](https://flume.apache.org/download.html)

Rename downloaded folders respectively to `hbase` and `flume` and move this folders to `docker` folder in project directory

### HBase Configuration

Set `export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre` in `hbase/conf/hbase-env.sh`

Add configuration in  `hbase/conf/hbase-site.xml`

---------------------------------------------------------------------------------------

	<configuration>
		<property>
	    <name>hbase.rootdir</name>
	    <value>/hbase/hbase-log</value>
	  </property>
	  <property>
	    <name>hbase.zookeeper.property.dataDir</name>
	    <value>/hbase/zookeeper-log</value>
	  </property>
	</configuration>

------------------------------------------------------------------------------------------

 
### Flume Configuration

Set `export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre` and `FLUME_CLASSPATH="/flume/lib/"` in `flume/conf/flume-env.sh`

Add sources, channels and sinks in `flume/conf/flume-conf.properties`

------------------------------------------------------------------------------------------

    #DECLARATION
    agent1.sinks =  hdfs-sink-1 hdfs-sink-2
    agent1.sources = source-1 source-2
    agent1.channels = fileChannel-1 fileChannel-2
    
    
    #MOVIES
    
    agent1.channels.fileChannel-1.type = file 
    agent1.channels.fileChannel-1.capacity = 200000
    agent1.channels.fileChannel-1.transactionCapacity = 1000
    agent1.channels.fileChannel-1.checkpointDir = /flume/logs/f1
    agent1.channels.fileChannel-1.dataDirs = /flume/logs/data/f1
    
    agent1.sources.source-1.type = spooldir
    agent1.sources.source-1.spoolDir = /data/input/movies
    agent1.sources.source-1.fileHeader = false
    agent1.sources.source-1.fileSuffix = .COMPLETED
    
    agent1.sinks.hdfs-sink-1.type = hdfs
    agent1.sinks.hdfs-sink-1.hdfs.path = hdfs://master:54310/movies
    agent1.sinks.hdfs-sink-1.hdfs.batchSize = 1000
    agent1.sinks.hdfs-sink-1.hdfs.rollSize = 268435456
    agent1.sinks.hdfs-sink-1.hdfs.rollInterval = 0
    agent1.sinks.hdfs-sink-1.hdfs.rollCount = 50000000
    agent1.sinks.hdfs-sink-1.hdfs.writeFormat=Text
    agent1.sinks.hdfs-sink-1.hdfs.fileType = DataStream
    
    agent1.sources.source-1.channels = fileChannel-1
    agent1.sinks.hdfs-sink-1.channel = fileChannel-1
    
    
    #RATINGS
    
    agent1.channels.fileChannel-2.type = file 
    agent1.channels.fileChannel-2.capacity = 200000
    agent1.channels.fileChannel-2.transactionCapacity = 1000
    agent1.channels.fileChannel-2.checkpointDir = /flume/logs/f2
    agent1.channels.fileChannel-2.dataDirs = /flume/logs/data/f2
    
    agent1.sources.source-2.type = spooldir
    agent1.sources.source-2.spoolDir = /data/input/ratings
    agent1.sources.source-2.fileHeader = false
    agent1.sources.source-2.fileSuffix = .COMPLETED
    
    agent1.sinks.hdfs-sink-2.type = hdfs
    agent1.sinks.hdfs-sink-2.hdfs.path = hdfs://master:54310/ratings
    agent1.sinks.hdfs-sink-2.hdfs.batchSize = 1000
    agent1.sinks.hdfs-sink-2.hdfs.rollSize = 268435456
    agent1.sinks.hdfs-sink-2.hdfs.rollInterval = 0
    agent1.sinks.hdfs-sink-2.hdfs.rollCount = 50000000
    agent1.sinks.hdfs-sink-2.hdfs.writeFormat=Text
    agent1.sinks.hdfs-sink-2.hdfs.fileType = DataStream
    
    agent1.sources.source-2.channels = fileChannel-2
    agent1.sinks.hdfs-sink-2.channel = fileChannel-2

------------------------------------------------------------------------------------------



### Build Image

Open new terminal in `docker` directory and RUN: `docker build -t sabd-mapred:latest .`


## Run

Create folders `ratings` and `movies` in `dist/hddata/input` directory and copy the 
input files `ratings.csv` and `movies.csv` (download `ml-20m.zip` from https://grouplens.org/datasets/movielens/ and ) in the corresponding directory

Open new terminal in `dist` directory of project and run script `create.sh`

You're now logged in a docker container which is the namenode of the hadoop cluster 

Run `/data/script/initialize.sh` to initialize hadoop environment and hbase daemon

Inject files in hadoop file system with one of this two scripts

    - /data/flume-inject.sh : inject data in hdfs with flume ( ̴  90 minutes ). The script does not end, stop it when
                              input files are marked as .COMPLETED  
    - /data/put-inject.sh : inject data in hdfs with hdsf put command ( ̴  1 minutes ) 
    

Run `hadoop jar /data/mapreduce.jar core.QueryOne` ( or `core.QueryTwo` or `core.QueryThree`)
to execute a query

## Stop & Delete

To stop container run exit and run `delete.sh` script in `dist` directory
    
    
                                                         


