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
import java.util.Iterator;

import com.google.gson.GsonBuilder;
import org.json.JSONObject;


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


		private static final byte[] USER_ID_FAMILLY = Bytes.toBytes("idUser");
		private static final byte[] USER_NAME_FAMILLY = Bytes.toBytes("name");
		private static final byte[] NUMBER_TWEETS_FAMILLY = Bytes.toBytes("numberTweets");
		

		private static final byte[] TABLE_NAME = Bytes.toBytes("elhadj_tweet");
		private static final byte[] TABLE_NUMBER_TWEETS_BY_USER = Bytes.toBytes("bah_simba");

		public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
		}

		public static void createTable(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
				HColumnDescriptor famLoc = new HColumnDescriptor(TWEET_CREATED_AT_FAMILY); 
				tableDescriptor.addFamily(famLoc);
				tableDescriptor.addFamily(new HColumnDescriptor(TWEET_ID_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(TWEET_ID_STR_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(TWEET_TEXT_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(TWEET_LANG_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(TWEET_USER_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(TWEET_ENTITY_FAMILLY));
				
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
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

		public static void computeNumberTweetsByUser(Connection connection, JavaRDD<Tweet> jsonRDD) {
			final String localTable = "bah_simba";
			createTableTweetsByUser(connection);
			JavaRDD<Iterable<Tweet>> newJsonRdd = jsonRDD
						.groupBy(tweet -> tweet.user.getId())
						.values();
			System.out.println("===> counttt: " + newJsonRdd.count());
			JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = newJsonRdd.mapToPair(
				iterable -> {
					Iterator<Tweet> it = iterable.iterator();
					long count = 0;
					Tweet firstTweet = null;
					if (it.hasNext()) {
						firstTweet = it.next();
						count += 1;
					}
					while(it.hasNext()) {
						count += 1;
						it.next();
					}

					Put put = new Put(Bytes.toBytes(firstTweet.user.getId()));
					put.addColumn(USER_NAME_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(firstTweet.user.getName()));
					put.addColumn(NUMBER_TWEETS_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(String.valueOf(count)));

					return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);    
				}
			);

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
		
			Job newAPIJobConfiguration1 = null;
			try {
				newAPIJobConfiguration1 = Job.getInstance(myConfig);
			} catch(Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
			newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, localTable);
			newAPIJobConfiguration1.setOutputFormatClass(TableOutputFormat.class);

			hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
		}

		public int run(String[] args) throws Exception {
			final Connection connection = ConnectionFactory.createConnection(getConf());

			SparkConf conf = new SparkConf().setAppName("BAH_SIMBA_PROJECT");
			conf.set("spark.hadoop.validateOutputSpecs", "false");
			JavaSparkContext context = new JavaSparkContext(conf);

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

			computeNumberTweetsByUser(connection, jsonRDD);

			/*
			JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = jsonRDD.mapToPair(
				tweet -> {
					Put put = new Put(Bytes.toBytes("" + tweet.row));
					put.addColumn(TWEET_CREATED_AT_FAMILY, Bytes.toBytes(""), Bytes.toBytes(tweet.created_at));
					put.addColumn(TWEET_ID_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(tweet.id));
					put.addColumn(TWEET_ID_STR_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(tweet.id_str));
					put.addColumn(TWEET_TEXT_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(tweet.text));
					put.addColumn(TWEET_LANG_FAMILLY, Bytes.toBytes(""), Bytes.toBytes(tweet.lang));
					if (tweet.user != null) {
						put.addColumn(TWEET_USER_FAMILLY, Bytes.toBytes("id"), Bytes.toBytes(tweet.user.id));
						put.addColumn(TWEET_USER_FAMILLY, Bytes.toBytes("name"), Bytes.toBytes(tweet.user.name));
					}
					if (tweet.entities != null) {
						put.addColumn(TWEET_ENTITY_FAMILLY, Bytes.toBytes("hastags"), Bytes.toBytes(tweet.entities.hastagsToString()));
					}
					return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);    
				}
			);
			*/
			// hbase config
			/*
			Configuration myConfig = null;
			try {
				myConfig =   HBaseConfiguration.create();
				//conf.set(ZOOKEEPER_QUORUM, "comma seperated list of zookeeper quorum");
				myConfig.set("hbase.mapred.outputtable", "elhadj_tweet");
				myConfig.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");
				HBaseAdmin.checkHBaseAvailable(myConfig);
				System.out.println("===> Hbase is running");
			} catch (MasterNotRunningException e) {
				System.out.println("===> Hbase is not running");
				System.exit(1);
			} catch (Exception e) {
				e.printStackTrace();
			}
			//myConfig.set(TableInputFormat.INPUT_TABLE, "elhadj_tweet");
			myConfig.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
		
			// new Hadoop API configuration
			
			Job newAPIJobConfiguration1 = Job.getInstance(myConfig);
			newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "elhadj_tweet");
			newAPIJobConfiguration1.setOutputFormatClass(TableOutputFormat.class);
			System.out.println("===> count: " + hbasePuts.count());
			hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());

			// count the number of tweet for a user
			final String filterId = "767354852";
			long res = jsonRDD.filter(tweet -> (tweet.user != null && tweet.user.id.equals("767354852"))).count();
			System.out.println("===> number of tweets for user " + filterId + ": " + res);

			// group tweet by country
			JavaPairRDD<String, Iterable<Tweet>> tweetsByLang = jsonRDD.filter(tweet -> !tweet.lang.equals("")).groupBy(tweet -> tweet.lang);

			System.out.println("=> number of valid countries: " + tweetsByLang.count());
			tweetsByLang.foreach(tuple -> {
				int count = 0;
				for (Tweet tweet : tuple._2()) {
					count+=1;
				}
				System.out.println("=> " + tuple._1() + ": " + count);
			});

			// count user's hashtags
			jsonRDD = jsonRDD.filter(tweet -> {
				return (tweet.entities != null) && (tweet.entities.hashtags.size() > 0);
			});

			System.out.println("tweets with hastags: " + jsonRDD.count());

			// just for debug
			try {
				System.out.println();
				System.out.println(jsonRDD.first().toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("==> first line" + fileRDD.first());
			//*/

			System.out.println("==> first line" + fileRDD.first());
			return 0;
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
