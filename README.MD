# Prequisitions
install scala
`sudo apt-get install scala`
Verify
`scala -version`

install sbt
`echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list`
`sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823`
`sudo apt-get update`
`sudo apt-get install sbt`

download and install spark
`https://www.apache.org/dyn/closer.lua/spark/spark-2.4.2/spark-2.4.2-bin-hadoop2.7.tgz`

Build jar
`sbt package`

Run program from source
`sbt "runMain Main resources/configs/experiments/random-graph-own-test.conf"`



## Monitoring UI

Start the history server with the following command (change location if spark is installed somewhere else)
`/./usr/local/spark/spark-2.4.2-bin-hadoop2.7/sbin/start-history-server.sh`
View the history in the browser, default is: `localhost:18080`


## Run Tests
- start orient db with docker-compose up
- make sure read write access is set properly (`sudo chmod -R 777 orientdb`)
- run JUnits individually (closing the db connections is error prone so for now tests cannot be run automated)
