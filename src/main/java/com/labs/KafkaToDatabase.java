package com.labs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
 * Exactly once from Kafka to database leveraging transactional writes.
 * @author sbanerje
 *
 */
public class KafkaToDatabase {

	public static void main(String[] args) throws StreamingQueryException {

		SparkSession spark = SparkSession.builder().config("spark.sql.shuffle.partitions", "3")
				.appName("KafkaToDatabase").master("local[*]").getOrCreate();

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
				.option("checkpointLocation", "/tmp/checkpoint").foreach(new JDBCTransactionalWriter()).start();

		query.awaitTermination();

	}

	private static final class JDBCTransactionalWriter extends ForeachWriter<User> {

		private static final long serialVersionUID = 1L;

		private static final String CHECK_DUPLICATE = "select max(EPOCH) as EPOCHVAL from TXTABLE where PARTITIONID=";
		private static final String INSERT_INTO_TXTABLE = "INSERT INTO TXTABLE(\"PARTITIONID\",\"EPOCH\") values(?,?)";
		private static final String INSERT_INTO_USER = "INSERT INTO  USER(\"NAME\",\"AGE\", \"ID\") values(?,?,?)";

		transient Connection conn;
		transient PreparedStatement stmtTable;
		transient PreparedStatement stmtTx;
		transient boolean isResult = false;

		@Override
		public void process(User user) {
			System.out.println("USER RECEIVED : " + user.getName() + "   " + user.getAge());
			try {
				stmtTable.setString(1, user.getName());
				stmtTable.setInt(2, user.getAge());
				stmtTable.setInt(3, user.getId());
				stmtTable.addBatch();
			} catch (SQLException e) {

				e.printStackTrace();
				throw new RuntimeException(e);
			}

		}

		@Override
		public boolean open(long partitionid, long epoch) {
			try {
				Class.forName("org.h2.Driver");
				conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/test", "", "");

				String sql = CHECK_DUPLICATE + partitionid;
				Statement queryStatement = conn.createStatement();
				ResultSet result = queryStatement.executeQuery(sql);
				while (result.next()) {
					long epocfromdb = result.getLong("EPOCHVAL");
					if (epocfromdb >= epoch) {
						isResult = true;
						break;
					}
				}
				if (isResult) {
					System.out.println("result already in db, exiting");
					return false;
				}

				System.out.println("result not in db, continuing");
				
				stmtTable = conn.prepareStatement(INSERT_INTO_USER);
				stmtTx = conn.prepareStatement(INSERT_INTO_TXTABLE);
				stmtTx.setLong(1, partitionid);
				stmtTx.setLong(2, epoch);

				conn.setAutoCommit(false);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
			return true;
		}

		@Override
		public void close(Throwable th) {

			try {
				if (!isResult) {
					stmtTable.executeBatch();
					stmtTx.execute();
					conn.commit();
				}
			} catch (SQLException e) {
				try {
					conn.rollback();
					e.printStackTrace();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

			}finally {
				if (conn != null) {
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		}
	}

}
