package bigdata;


import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.GsonBuilder;
import org.json.JSONObject;


import scala.Tuple2;

public class TPSpark1 {

	public static class HBaseProg extends Configured implements Tool {
		private static final byte[] TWEET_CREATED_AT_FAMILY = Bytes.toBytes("created_at");
		private static final byte[] TWEET_ID_FAMILLY = Bytes.toBytes("id");
		private static final byte[] TWEET_ID_STR_FAMILLY = Bytes.toBytes("id_str");
		private static final byte[] TWEET_TEXT_FAMILLY = Bytes.toBytes("text");
		private static final byte[] TWEET_SOURCE_FAMILLY = Bytes.toBytes("source");
		private static final byte[] TWEET_TRUNCATED_FAMILLY = Bytes.toBytes("truncated");
		private static final byte[] ROW    = Bytes.toBytes("0");
		private static final byte[] TABLE_NAME = Bytes.toBytes("elhadj_tweet");

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
				tableDescriptor.addFamily(new HColumnDescriptor(TWEET_SOURCE_FAMILLY));
				tableDescriptor.addFamily(new HColumnDescriptor(TWEET_TRUNCATED_FAMILLY));
				
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		public int run(String[] args) throws Exception {
			final Connection connection = ConnectionFactory.createConnection(getConf());
			createTable(connection);
			final Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

			SparkConf conf = new SparkConf().setAppName("TP Spark");
			JavaSparkContext context = new JavaSparkContext(conf);
			JavaRDD<String> fileRDD = context.textFile(args[0]);
			//*
			JavaRDD<Tweet> jsonRDD = fileRDD.map(
				line -> {
					GsonBuilder gson = new GsonBuilder();
					Tweet tweet = gson.create().fromJson(line, Tweet.class);
					return tweet;
				}
			);
			try {
				System.out.println();
				System.out.println(jsonRDD.first().toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("==> " + fileRDD.first());
			//*/

			//*
			Put put = new Put(Bytes.toBytes("0"));
			put.addColumn(TWEET_CREATED_AT_FAMILY, Bytes.toBytes("x"), Bytes.toBytes("SAM 9 janv 2021"));
			put.addColumn(TWEET_ID_FAMILLY, Bytes.toBytes("x"), Bytes.toBytes("1234565"));
			put.addColumn(TWEET_ID_STR_FAMILLY, Bytes.toBytes("x"), Bytes.toBytes("1234565"));
			put.addColumn(TWEET_TEXT_FAMILLY, Bytes.toBytes("x"), Bytes.toBytes("1234565"));
			put.addColumn(TWEET_SOURCE_FAMILLY, Bytes.toBytes("x"), Bytes.toBytes("1234565"));
			put.addColumn(TWEET_TRUNCATED_FAMILLY, Bytes.toBytes("x"), Bytes.toBytes("1234565"));
			table.put(put);
			//*/

			return 0;
		}
	}


	public static void main(String[] args) {
		/*
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> fileRDD = context.textFile(args[0]);
		JavaRDD<Tweet> jsonRDD = fileRDD.map(
			line -> {
				GsonBuilder gson = new GsonBuilder();
				return gson.create().fromJson(line, Tweet.class);
			}
		);
		try {
			System.out.println();
			System.out.println(jsonRDD.first().toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("==> " + fileRDD.first());
		*/

		try {
			int exitCode = ToolRunner.run(HBaseConfiguration.create(), new TPSpark.HBaseProg(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
