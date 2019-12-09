package com.labs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

public class KafkaToHDFS {

	public static void main(String[] args) throws StreamingQueryException {

		SparkSession spark = SparkSession.builder().config("spark.sql.shuffle.partitions", "3")
				.appName("KafkaToHDFS").master("local[*]").getOrCreate();

		Encoder<User> userEncoder = Encoders.bean(User.class);

		Dataset<Row> df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9091")
				.option("subscribe", "test").load();

		df.printSchema();

		Dataset<Row> lines = df.selectExpr("CAST(value AS STRING)");

		// Split the lines into user objects
		Dataset<User> words = lines.as(Encoders.STRING()).map(x -> {
			String[] values = x.split(",");
			return new User(values[0], Integer.parseInt(values[1]), Integer.parseInt(values[2]));
		}, userEncoder);

		words.printSchema();

		StreamingQuery query = words.writeStream().trigger(Trigger.ProcessingTime("5 seconds")).outputMode("append")
				.option("checkpointLocation", "/tmp/checkpoint2").foreach(new HDFSTransactionalWriter()).start();
		query.awaitTermination();
		
	}
	
	private static final class HDFSTransactionalWriter extends ForeachWriter<User>{

		private static final long serialVersionUID = 1L;
		transient FileSystem fs = null;
		transient String filenameComitted ;
		transient String filenameUnComitted ;
		
		transient FSDataOutputStream fsStream ;
		
		transient boolean exists = false;;

		
		@Override
		public void close(Throwable th) {
			if(th!=null || exists) {
				System.out.println("File already exists...nothing to do");
				return;
			}
			
			try {
				fsStream.hflush();
				fsStream.flush();
				fsStream.close();
				
				//Atomically rename the file, making it available for consumption
				fs.rename(new Path(filenameUnComitted), new Path(filenameComitted));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
		}

		@Override
		public boolean open(long partitionid, long epoch) {
			
			filenameComitted = "/user/hadoop/testdir/exactlyonce_"+partitionid+"_"+epoch+".comitted";
			filenameUnComitted = "/user/hadoop/testdir/exactlyonce_"+partitionid+"_"+epoch+".uncomitted";
			try {
				fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());

				exists = fs.exists(new Path(filenameComitted));
				System.out.println("file exists :"+exists);

				if(exists) {
					return false;
				}
				
				fsStream = fs.create(new Path(filenameUnComitted), true);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
				return false;
			}
			
			return true;
		}

		@Override
		public void process(User user) {
			System.out.println("USER RECEIVED : " + user.getName() + "   " + user.getAge());
			try {
				fsStream.writeUTF(user.toString());
			} catch (IllegalArgumentException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			
		}
		
	}
}
