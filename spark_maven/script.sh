#!/bin/bash
hdfs dfs -rm -r /user/elbah/projetPrototype
mvn package &&
	spark-submit --master yarn  ./target/TPSpark-0.0.1.jar /raw_data/tweet_01_03_2020_first10000.nljson /user/elbah/projetPrototype
	#spark-submit --master yarn --num-executors 16 --executor-memory 2G --executor-cores 4 ./target/TPSpark-0.0.1.jar /raw_data/tweet_01_03_2020.nljson /user/elbah/projetPrototype
