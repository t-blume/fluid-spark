## Setup working environment
 - install java oracle jdk 8 or later (preferred 11.03)
 - install scala `sudo apt-get install scala`
    - check version `scala -version`
 - install sbt
    - `echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list`
    - `sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823`
    - `sudo apt-get update`
    - `sudo apt-get install sbt`
 - download and install spark
    - download spark `wget http://archive.apache.org/dist/spark/spark-2.4.2/spark-2.4.2-bin-hadoop2.7.tgz`
    - create spark directory `mkdir /usr/local/spark`
    - extract spark `tar xvf spark-2.4.2-bin-hadoop2.7.tgz -C /usr/local/spark/`
    - you may need to change user permission so non-root users can read spark
    - configure local environment for spark and scala. Add the following variables to your bashrc, adapt if necessary (Max RAM size).
        - `export SCALA_HOME=/usr/bin/scala`
        - `export SPARK_HOME=/usr/local/spark/spark-2.4.3-bin-hadoop2.7`
        - `export SBT_OPTS="-Xmx300G -XX:MaxPermSize=300G"`
    - update bashrc `source ~/.bashrc`
        
## Build project

 - build executable (optional): `sbt package` This steps shows that everything (except spark) is correctly installed.
 - setup OrientDB database (https://orientdb.com/)
    - for convenience, we provide to pre-configured docker images in `resources/docker`, one used for development and one used for experiments using different memory configurations.
    - see this manual to install docker-compose: https://docs.docker.com/compose/install/   
    - make sure read and write access is set properly to the data directory of OrientDB (`sudo chmod -R 777 orientdb`)

 - run program from source: `sbt "runMain Main resources/configs/tests/manual-test-1.conf"`
 - the program runs using a small sample file, creates a database, and logs some performance data. For more details, see the configuration files provided.
 - for configurations using larger datasets, one can observe the progress via the Web UI (localhost:4040). When you run this on a server, you may want to consider using ssh port forwarding `ssh -NL <local port>:<server>:4040 <server>` 

## Monitoring UI (history server)

 - start the history server with the following command (change location if spark is installed somewhere else)
`/./usr/local/spark/spark-2.4.2-bin-hadoop2.7/sbin/start-history-server.sh`
 - view the history of all executed spark programs here, default location is `localhost:18080`
 - parse execution results by running the python script `collectPerformance.py`. The script collects all execution times of for a given configuration and aggregates them in a csv file. 
 - by default, the history files are stored in `/tmp/spark-events/`. You may change this to avoid data loss.
## Run Tests
- make sure OrientDB is running.
- run JUnits individually (closing the db connections is error prone so for now tests cannot be run automated)
