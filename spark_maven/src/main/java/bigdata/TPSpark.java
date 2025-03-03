package bigdata;


import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.util.StatCounter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import sun.security.krb5.Config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.MasterNotRunningException;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Iterator;

import com.google.gson.GsonBuilder;
import org.json.JSONObject;

import java.nio.charset.Charset;


import scala.Tuple2;

public class TPSpark {

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
		

		private static final byte[] TABLE_NAME = Bytes.toBytes("elhadj_tweet");
		private static final byte[] TABLE_NUMBER_TWEETS_BY_USER = Bytes.toBytes("bah-simba_tweets_by_user");
		private static final byte[] TABLE_USER_HASHTAGS = Bytes.toBytes("bah-simba_users_hashtags");

		public static final int TOP_K = 10000;
		public static final int NUMBER_FILES = 21;

		public int run(String[] args) throws Exception {
			final Connection connection = ConnectionFactory.createConnection(getConf());

			SparkConf conf = new SparkConf().setAppName("BAH_SIMBA_PROJECT");
			conf.set("spark.hadoop.validateOutputSpecs", "false");
			JavaSparkContext context = new JavaSparkContext(conf);

			if (args.length != 1)
				usage();

			switch (args[0]) {
				case "NUMBER_TWEETS_BY_USER":
					computeNumberTweetsByUser(connection, context);
					break;
				case "NUMBER_TWEETS_BY_LANG":
					computeNumberTweetsByLang(connection, context);
					break;
				case "USER_HASHTAGS":
					computeUsersHashtags(connection, context);
					break;
				case "TOPK_HASHTAGS":
					computeTopKHashtags(connection, context);
					break;
				case "TOPK_HASHTAGS_BY_DAY":
					computeTopKHashtagsByDay(connection, context);
					break;
				case "HASHTAG_USERS":
					computeUsersByHashtag(connection, context);
					break;
				case "ALL":
					computeNumberTweetsByUser(connection, context);
					computeNumberTweetsByLang(connection, context);
					computeUsersHashtags(connection, context);
					computeTopKHashtags(connection, context);
					computeTopKHashtagsByDay(connection, context);
					computeUsersByHashtag(connection, context);
					break;
				default:
					usage();
			}

			return 0;
		}

		public static void usage() {
			System.out.println("\n\n========================= usage ============================\n" + 
					"spark-submit --master yarn --num-executors 16 --executor-cores 4 --executor-memory 2G ./target/TPSpark-0.0.1.jar <REQUEST>\n" +
					"Where request: \n" +
						"\tNUMBER_TWEETS_BY_USER: compute the number tweets by user \n" +
						"\tNUMBER_TWEETS_BY_LANG: compute the number tweets by language \n" +
						"\tUSER_HASHTAGS: compute hashtags for each user\n" +
						"\tTOPK_HASHTAGS: compute the top k (1<= k <= 10000) hashtags \n" +
						"\tTOPK_HASHTAGS_BY_DAY: compute the top k hashtags for each day \n" +
						"\tHASHTAG_USERS: for each hastag, count it and get its users \n" +
						"\tALL: run all request\n\n");
			System.exit(-1);
		}

		public static void computeNumberTweetsByUser(Connection connection, JavaSparkContext context) {
			final String localTable = "bah-simba_tweets_by_user";
			createTableTweetsByUser(connection);

			JavaPairRDD<String, User> unionRdds = JavaPairRDD.fromJavaRDD(context.emptyRDD());
			for (int i=1; i<=NUMBER_FILES; i++) {
				String path = getPath(i);
				JavaRDD<Tweet> jsonRDD = loadAndParseFileFromHDFS(context, path);
				unionRdds = jsonRDD
						.mapToPair(tweet -> new Tuple2<>(tweet.getUser().getId(), tweet.getUser()))
						.reduceByKey((user1, user2) -> {
							user1.mergeNumberTweets(user2);
							return user1;
						})
						.union(unionRdds)
						.reduceByKey((user1, user2) -> {
							user1.mergeNumberTweets(user2);
							return user1;
						});
			}

			JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = unionRdds.mapToPair(
				tuple -> {
					Put put = new Put(Bytes.toBytes(tuple._1()));
					put.addColumn(USER_NAME_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(tuple._2().getName()));
					put.addColumn(NUMBER_TWEETS_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(String.valueOf(tuple._2().getNumberTweets())));

					return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);    
				}
			);

			Configuration myConfig = getHbaseConfiguration(localTable);
			Job newAPIJobConfiguration1 = getNewAPIJobConfiguration(localTable, myConfig);

			hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		}

		public static String getPath(int i) {
			return (i < 10) ? "/raw_data/tweet_0" + i + "_03_2020.nljson" : "/raw_data/tweet_" + i + "_03_2020.nljson";
		}

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
			).filter(tweet -> tweet != null && tweet.getUser() != null);
			return jsonRDD;
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

		public static void computeNumberTweetsByLang(Connection connection, JavaSparkContext context) {
			final String localTable = "bah-simba_tweets_by_lang";
			createTableTweetsByLang(connection);

			JavaPairRDD<String, Integer> unionRdds = JavaPairRDD.fromJavaRDD(context.emptyRDD());


			for (int i=1; i<=NUMBER_FILES; i++) {
				String path = getPath(i);
				JavaRDD<Tweet> jsonRDD = loadAndParseFileFromHDFS(context, path);
				unionRdds = jsonRDD
						.mapToPair(tweet -> new Tuple2<>(tweet.getLang(), 1))
						.reduceByKey((n1, n2) -> n1 + n2)
						.union(unionRdds)
						.reduceByKey((n1, n2) -> n1 + n2);
			}

			JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = unionRdds.mapToPair(tuple -> {
							Put put = new Put(Bytes.toBytes(tuple._1()));
							put.addColumn(NUMBER_TWEETS_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(String.valueOf(tuple._2())));
							return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);    
						});

			Configuration myConfig = getHbaseConfiguration(localTable);
			Job newAPIJobConfiguration1 = getNewAPIJobConfiguration(localTable, myConfig);

			hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		}

		public static void computeUsersHashtags(Connection connection, JavaSparkContext context) {
			final String localTable = "bah-simba_users_hashtags";
			createTableUsersHashtags(connection);

			JavaPairRDD<String, User> unionRdds = JavaPairRDD.fromJavaRDD(context.emptyRDD());
			for (int i=1; i<=21; i++) {
				String path = getPath(i);
				JavaRDD<Tweet> jsonRDD = loadAndParseFileFromHDFS(context, path);
				unionRdds = jsonRDD
						.filter(tweet -> tweet.getEntity() != null && !tweet.getEntity().hastagsToString().equals(""))
						.mapToPair(tweet ->{
							tweet.getUser().addHashtag(tweet.getEntity().hastagsToString());
							return new Tuple2<>(tweet.getUser().getId(), tweet.getUser());
						}) 
						.reduceByKey((user1, user2) -> {
							user1.mergeHashtags(user2);
							return user1;
						})
						.union(unionRdds)
						.reduceByKey((user1, user2) -> {
							user1.mergeHashtags(user2);
							return user1;
						});
			}
	
			JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = unionRdds.mapToPair(
				tuple -> {
					Put put = new Put(Bytes.toBytes(tuple._1()));
					put.addColumn(USER_HASHTAGS_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(String.valueOf(tuple._2().getHashtags())));

					return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);    
				}
			);

			Configuration myConfig = getHbaseConfiguration(localTable);
			Job newAPIJobConfiguration1 = getNewAPIJobConfiguration(localTable, myConfig);

			hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		}

		public static void computeUsersByHashtag(Connection connection, JavaSparkContext context) {
			final String localTable = "bah-simba_users_by_hashtag";
			createTableUsersByHashtag(connection);

			JavaPairRDD<String, HashTag> unionRdds = JavaPairRDD.fromJavaRDD(context.emptyRDD());
			for (int i=1; i<=NUMBER_FILES; i++) {
				String path = getPath(i);
				JavaRDD<Tweet> jsonRDD = loadAndParseFileFromHDFS(context, path);
				unionRdds = jsonRDD
						.filter(tweet -> tweet.getEntity() != null && !tweet.getEntity().hastagsToString().equals(""))
						.flatMapToPair(tweet -> {
							List<Tuple2<String, HashTag>> list = new ArrayList<>();
							for (HashTag h : tweet.getEntity().getHashtags()) {
								h.setUsersrNames(tweet.getUser().getName());
								list.add(new Tuple2<>(h.getText(), h));
							}
							return list.iterator();
						})
						.reduceByKey((h1, h2) -> {
							h1.mergeCounters(h2);
							h1.mergeUsersNames(h2);
							return h1;
						})
						.union(unionRdds)
						.reduceByKey((h1, h2) -> {
							h1.mergeCounters(h2);
							h1.mergeUsersNames(h2);
							return h1;
						});
			}
	
			JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = unionRdds.mapToPair(
				tuple -> {
					Put put = new Put(Bytes.toBytes(tuple._1()));
					put.addColumn(HASHTAGS_USERS_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(tuple._2().getUsersNames()));
					put.addColumn(COUNT_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(String.valueOf(tuple._2().getCounter())));

					return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);    
				}
			);

			Configuration myConfig = getHbaseConfiguration(localTable);
			Job newAPIJobConfiguration1 = getNewAPIJobConfiguration(localTable, myConfig);

			hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		}


		public static void computeTopKHashtags(Connection connection, JavaSparkContext context) {
			final String localTable = "bah-simba_topK_hashtags";
			createTableTopKHashtags(connection);

			JavaPairRDD<String, HashTag> unionRdds = JavaPairRDD.fromJavaRDD(context.emptyRDD());
			for (int i=1; i<=NUMBER_FILES; i++) {
				String path = getPath(i);
				JavaRDD<Tweet> jsonRDD = loadAndParseFileFromHDFS(context, path);
				unionRdds = jsonRDD
						.filter(tweet -> tweet.getEntity() != null && !tweet.getEntity().hastagsToString().equals(""))
						.flatMapToPair(tweet -> {
							List<Tuple2<String, HashTag>> list = new ArrayList<>();
							for (HashTag h : tweet.getEntity().getHashtags()) {
								list.add(new Tuple2<>(h.getText(), h));
							}
							return list.iterator();
						})
						.reduceByKey((h1, h2) -> {
							h1.mergeCounters(h2);
							return h1;
						})
						.union(unionRdds)
						.reduceByKey((h1, h2) -> {
							h1.mergeCounters(h2);
							return h1;
						});
			}

			JavaRDD<HashTag> hehe = context.parallelize(unionRdds.values().top(TOP_K));
	
			JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = hehe.zipWithIndex().mapToPair(
				hashtag -> {
					Put put = new Put(Bytes.toBytes(String.valueOf(hashtag._2())));
					put.addColumn(HASHTAGS_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(hashtag._1().getText()));
					put.addColumn(COUNT_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(String.valueOf(hashtag._1.getCounter())));
					return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);    
				}
			);

			Configuration myConfig = getHbaseConfiguration(localTable);
			Job newAPIJobConfiguration1 = getNewAPIJobConfiguration(localTable, myConfig);

			hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		}


		public static void computeTopKHashtagsByDay(Connection connection, JavaSparkContext context) {
			final String localTable = "bah-simba_topK_hashtags_by_day";
			createTableTopKHashtagsByDay(connection);

			for (int i=1; i<=NUMBER_FILES; i++) {
				final AtomicInteger atomicI = new AtomicInteger(i);
				String path = getPath(i);
				JavaRDD<Tweet> jsonRDD = loadAndParseFileFromHDFS(context, path);
				JavaPairRDD<String, HashTag> unionRdds = jsonRDD
						.filter(tweet -> tweet.getEntity() != null && !tweet.getEntity().hastagsToString().equals(""))
						.flatMapToPair(tweet -> {
							List<Tuple2<String, HashTag>> list = new ArrayList<>();
							for (HashTag h : tweet.getEntity().getHashtags()) {
								h.setNumDay(atomicI.get());
								list.add(new Tuple2<>(h.getText(), h));
							}
							return list.iterator();
						})
						.reduceByKey((h1, h2) -> {
							h1.mergeCounters(h2);
							return h1;
						});

				JavaRDD<HashTag> hehe = context.parallelize(unionRdds.values().top(TOP_K));
				JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = hehe.zipWithIndex().mapToPair(
					hashtag -> {
						String key = String.valueOf(hashtag._1().getNumDay()) + "-" + String.valueOf(hashtag._2());
						Put put = new Put(Bytes.toBytes(key));
						put.addColumn(HASHTAGS_FAMILLY, Bytes.toBytes("name"), Bytes.toBytes(hashtag._1().getText()));
						put.addColumn(HASHTAGS_FAMILLY, Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(hashtag._1.getCounter())));
						return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);    
					}
				);

				Configuration myConfig = getHbaseConfiguration(localTable);
				Job newAPIJobConfiguration1 = getNewAPIJobConfiguration(localTable, myConfig);

				hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
			}

		}



		public static void createTableTweetsByUser(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NUMBER_TWEETS_BY_USER));
				tableDescriptor.addFamily(new HColumnDescriptor(USER_ID_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(USER_NAME_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(NUMBER_TWEETS_FAMILLY));
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
		}


		public static void createTableTweetsByLang(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("bah-simba_tweets_by_lang"));
				tableDescriptor.addFamily(new HColumnDescriptor(TWEET_LANG_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(NUMBER_TWEETS_FAMILLY));
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		public static void createTableUsersHashtags(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("bah-simba_users_hashtags"));
				tableDescriptor.addFamily(new HColumnDescriptor(USER_HASHTAGS_FAMILLY));
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		public static void createTableUsersByHashtag(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("bah-simba_users_by_hashtag"));
				tableDescriptor.addFamily(new HColumnDescriptor(HASHTAGS_USERS_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(COUNT_FAMILLY));
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		public static void createTableTopKHashtags(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("bah-simba_topK_hashtags"));
				tableDescriptor.addFamily(new HColumnDescriptor(HASHTAGS_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(COUNT_FAMILLY));
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		public static void createTableTopKHashtagsByDay(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("bah-simba_topK_hashtags_by_day"));
				tableDescriptor.addFamily(new HColumnDescriptor(HASHTAGS_FAMILLY));
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		public static <T> void printRDD(JavaRDD<T> rdd) {
			List<T> list = rdd.take(11);
			for (T t : list)
				System.out.println("--> " + t);
		}
	}


	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(HBaseConfiguration.create(), new TPSpark.HBaseProg(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
