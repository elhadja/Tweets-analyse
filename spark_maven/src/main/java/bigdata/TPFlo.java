package bigdata;

import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TPFlo {
    public static class HBaseProg extends Configured implements Tool {
        private static final byte[] TWEET_CREATED_AT_FAMILY = Bytes.toBytes("created_at");
        private static final byte[] TWEET_ID_FAMILLY = Bytes.toBytes("id");
        private static final byte[] TWEET_ID_STR_FAMILLY = Bytes.toBytes("id_str");
        private static final byte[] TWEET_TEXT_FAMILLY = Bytes.toBytes("text");
        private static final byte[] TWEET_LANG_FAMILLY = Bytes.toBytes("lang");
        private static final byte[] TWEET_USER_FAMILLY = Bytes.toBytes("user");
        private static final byte[] TWEET_ENTITY_FAMILLY = Bytes.toBytes("entity");
        private static final byte[] USER_HASHTAGS_FAMILLY = Bytes.toBytes("hashtags");
        private static final byte[] HASHTAGS_USERS_FAMILLY = Bytes.toBytes("users");
        private static final byte[] HASHTAGS_FAMILLY = Bytes.toBytes("hashtag");
        private static final byte[] COUNT_FAMILLY = Bytes.toBytes("count");


        private static final byte[] USER_ID_FAMILLY = Bytes.toBytes("idUser");
        private static final byte[] USER_NAME_FAMILLY = Bytes.toBytes("name");
        private static final byte[] NUMBER_TWEETS_FAMILLY = Bytes.toBytes("numberTweets");


        public static JavaRDD<Tweet> loadAndParseFileFromHDFS(JavaSparkContext context, String path) {
            JavaRDD<String> fileRDD = context.textFile(path);
            JavaRDD<Tweet> jsonRDD = fileRDD.map(
                    line -> {
                        GsonBuilder gson = new GsonBuilder();
                        Tweet tweet = null;
                        try {
                            tweet = gson.create().fromJson(line, Tweet.class);
                        } catch (Exception e) {}
                        return tweet;
                    }
            ).filter(tweet -> tweet != null && tweet.user != null);
            return jsonRDD;
        }

        public static Configuration getHbaseConfiguration(String localTable) {
            Configuration myConfig = null;
            try {
                myConfig =   HBaseConfiguration.create();
                myConfig.set("hbase.mapred.outputtable", localTable);
                myConfig.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");
                HBaseAdmin.checkHBaseAvailable(myConfig);
                System.out.println("===> Hbase is running");
            } catch (MasterNotRunningException e) {
                System.out.println("===> Hbase is not running");
                System.exit(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            myConfig.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");

            return myConfig;
        }

        public static Job getNewAPIJobConfiguration(String localTable, Configuration myConfig) {
            Job newAPIJobConfiguration1 = null;
            try {
                newAPIJobConfiguration1 = Job.getInstance(myConfig);
            } catch(Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
            return newAPIJobConfiguration1;
        }

        public static String getPath(int i) {
            return (i < 10) ? "/raw_data/tweet_0" + i + "_03_2020.nljson" : "/raw_data/tweet_" + i + "_03_2020.nljson";
        }

        public static void computeUsersW3Hashtags(Connection connection, JavaSparkContext context) {
            final String localTable = "bah-simba_users_hashtags";
//            createTableUsersHashtags(connection);

            JavaPairRDD<String, User> unionRdds = JavaPairRDD.fromJavaRDD(context.emptyRDD());
//            for (int i=1; i<=21; i++) {
//                String path = getPath(i);
                String path = "/raw_data/tweet_01_03_2020_first10000.nljson";
                JavaRDD<Tweet> jsonRDD = loadAndParseFileFromHDFS(context, path);
                unionRdds = jsonRDD
                        .filter(tweet -> tweet.entities != null && tweet.entities.hashtags.size() == 3)
                        .mapToPair(tweet ->{
                            tweet.user.addHashtag(tweet.entities.hashtagsToString());
                            return new Tuple2<>(tweet.user.getId(), tweet.user);
                        })
                        .reduceByKey((userId, user) -> {
                            userId.mergeHashtags(user);
                            return userId;
                        });
//            }

//            JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = unionRdds.mapToPair(
//                    tuple -> {
//                        Put put = new Put(Bytes.toBytes(tuple._1()));
//                        put.addColumn(USER_HASHTAGS_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(String.valueOf(tuple._2().getHashtags())));
//
//                        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
//                    }
//            );

//            Configuration myConfig = getHbaseConfiguration(localTable);
//            Job newAPIJobConfiguration1 = getNewAPIJobConfiguration(localTable, myConfig);
//
//            hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
        }

        public static void computeTopK3Hashtags(Connection connection, JavaRDD<Tweet> jsonRDD, JavaSparkContext context) {
            final String localTable = "bah-simba_topK_hashtags";
//            createTableTopKHashtags(connection);
            JavaRDD<HashTags> pairRdd  = jsonRDD
                    .filter(tweet -> tweet.entities != null && tweet.entities.hashtags.size() == 3)
                    .flatMapToPair(tweet -> {
                        List<Tuple2<String, HashTags>> list = new ArrayList<>();
                        HashTags hashTags = new HashTags(tweet.entities.hashtags);
                        list.add(new Tuple2<>(hashTags.getText(), hashTags));
                        return list.iterator();
                    })
                    .reduceByKey((h1, h2) -> {
                        h1.mergeCounters(h2);
                        return h1;
                    }).values();
            JavaRDD<HashTags> hehe = context.parallelize(pairRdd.top(1000));
//
//            //*
//
//            AtomicInteger id = new AtomicInteger();
//            JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = hehe.zipWithIndex().mapToPair(
//                    hashtag -> {
//                        Put put = new Put(Bytes.toBytes(String.valueOf(hashtag._2())));
//                        put.addColumn(HASHTAGS_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(hashtag._1().getText()));
//                        put.addColumn(COUNT_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(String.valueOf(hashtag._1.getCounter())));
//                        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
//                    }
//            );
//            //*/
//
//            Configuration myConfig = null;
//            try {
//                myConfig =   HBaseConfiguration.create();
//                myConfig.set("hbase.mapred.outputtable", localTable);
//                myConfig.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");
//                HBaseAdmin.checkHBaseAvailable(myConfig);
//                System.out.println("===> Hbase is running");
//            } catch (MasterNotRunningException e) {
//                System.out.println("===> Hbase is not running");
//                System.exit(1);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            myConfig.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
//
//            Job newAPIJobConfiguration1 = null;
//            try {
//                newAPIJobConfiguration1 = Job.getInstance(myConfig);
//            } catch(Exception e) {
//                e.printStackTrace();
//                System.exit(-1);
//            }
//            newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, localTable);
//            newAPIJobConfiguration1.setOutputFormatClass(TableOutputFormat.class);
//
//            hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
        }


        public int run(String[] args) throws Exception {
//            final Connection connection = ConnectionFactory.createConnection(getConf());

            SparkConf conf = new SparkConf().setAppName("BAH_SIMBA_PROJECT");
            conf.set("spark.hadoop.validateOutputSpecs", "false");
            JavaSparkContext context = new JavaSparkContext(conf);

			/*
			JavaRDD<String> fileRDD = context.textFile(args[0]);
			JavaRDD<Tweet> jsonRDD = fileRDD.map(
				line -> {
					GsonBuilder gson = new GsonBuilder();
					Tweet tweet = null;
					try {
						tweet = gson.create().fromJson(line, Tweet.class);
					} catch (Exception e) {}
					return tweet;
				}
			).filter(tweet -> tweet != null && tweet.user != null);
			//*/

            //computeNumberTweetsByUser(connection, context);
            //computeNumberTweetsByLang(connection, context);
//            computeUsersHashtags(connection, context);
            computeUsersW3Hashtags(null, context);
            //computeTopK3Hashtags(connection, jsonRDD, context);
            //computeUsersByHashtag(connection, jsonRDD);
            //computeTopKHashtags(connection, jsonRDD, context);
            //computeTopKHashtagsByDay(connection, context, jsonRDD);

            return 0;
        }
    }
    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new TPSpark.HBaseProg(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
