FROM bde2020/spark-master:2.4.0-hadoop2.7

MAINTAINER Till Blume <tbl@informatik.uni-kiel.de>

ENV SPARK_APPLICATION_MAIN_CLASS Main
ENV SPARK_APPLICATION_ARGS "resources/configs/experiments/random-graph-own-test.conf"