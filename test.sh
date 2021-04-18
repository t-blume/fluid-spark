myd=$(pwd)
cd resources/docker/develop/ && docker-compose up -d

if [ ! -d /tmp/spark-events/ ]; then
    mkdir -p /tmp/spark-events/
    echo "created /tmp/spark-events/"
fi

if [ ! -d /tmp/spark-events/ ]; then
    mkdir -p /tmp/spark-events/spark-memory
    echo "created /tmp/spark-events/spark-memory"
fi

cd $myd && SBT_OPTS="-Xms4G -Xmx4G" sbt clean coverage test && sbt coverageReport coveralls
