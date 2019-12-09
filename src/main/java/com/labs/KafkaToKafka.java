package com.labs;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * 
 * @author sbanerje
 *
 */
public class KafkaToKafka {

	public static void main(String[] args) throws StreamingQueryException {

		SparkSession spark = SparkSession.builder().config("spark.sql.shuffle.partitions", "3").appName("KafkaToKafka")
				.master("local[*]").getOrCreate();

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
				.option("checkpointLocation", "/tmp/checkpoint").foreach(new KafkaTransactionlWriter()).start();

		query.awaitTermination();

	}

	private static final class KafkaTransactionlWriter extends ForeachWriter<User> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void close(Throwable arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean open(long partitionId, long epoch) {

			Properties props = new Properties();
			props.put("bootstrap.servers", "localhost:9091");
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("enable.idempotence", true);
			 props.put("transactional.id", String.valueOf(epoch) + partitionId );
			props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
			
			
			 props.put("group.id", String.valueOf(epoch) + partitionId);
		     props.put("enable.auto.commit", "true");
		     props.put("auto.commit.interval.ms", "1000");

			Producer<String, String> producer = new KafkaProducer<>(props);

			return false;
		}

		@Override
		public void process(User arg0) {

		}

	}
}
