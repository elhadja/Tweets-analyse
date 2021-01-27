#!/bin/bash
hdfs dfs -rm -r /user/elbah/projetPrototype
mvn package &&
	#spark-submit ./target/TPSpark-0.0.1.jar
	#spark-submit ./target/TPSpark-0.0.1.jar
	#spark-submit --master yarn ./target/TPSpark-0.0.1.jar
	spark-submit --master yarn --num-executors 16 --executor-cores 4 --executor-memory 2G ./target/TPSpark-0.0.1.jar NUMBER_TWEETS_BY_LANG
