package kafka
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.sql.Row
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.Properties
import scala.util.Try

object Start{
	case class Structure(
		CRASH_DATE:String,
		CRASH_TIME:String,
		BOROUGH:String,
		ZIP_CODE:String,
		LATITUDE:String,
		LONGITUDE:String,
		LOCATION:String,
		ON_STREET_NAME:String,
		CROSS_STREET_NAME:String,
		OFF_STREET_NAME:String,
		NUMBER_OF_PERSONS_INJURED:String,
		NUMBER_OF_PERSONS_KILLED:String,
		NUMBER_OF_PEDESTRIANS_INJURED:String,
		NUMBER_OF_PEDESTRIANS_KILLED:String,
		NUMBER_OF_CYCLIST_INJURED:String,
		NUMBER_OF_CYCLIST_KILLED:String,
		NUMBER_OF_MOTORIST_INJURED:String,
		NUMBER_OF_MOTORIST_KILLED:String,
		CONTRIBUTING_FACTOR_VEHICLE_1:String,
		CONTRIBUTING_FACTOR_VEHICLE_2:String,
		CONTRIBUTING_FACTOR_VEHICLE_3:String,
		CONTRIBUTING_FACTOR_VEHICLE_4:String,
		CONTRIBUTING_FACTOR_VEHICLE_5:String,
		COLLISION_ID:String,
		VEHICLE_TYPE_CODE_1:String,
		VEHICLE_TYPE_CODE_2:String,
		VEHICLE_TYPE_CODE_3:String,
		VEHICLE_TYPE_CODE_4:String,
		VEHICLE_TYPE_CODE_5:String)

        def main(arg: Array[String]): Unit = {
		val spark = SparkSession.builder()
                     .master("local[*]")
                     .appName("data")
                     .getOrCreate()
		val n0 = arg(0)
		val n = scala.util.Try(n0.toInt).getOrElse(0)
                val ssc = new StreamingContext(spark.sparkContext, Seconds(60 * n))
		val sqlContext = new SQLContext(spark.sparkContext)
		import sqlContext.implicits._
				
		val par = Map("bootstrap.servers" -> "localhost:9092",
				"key.deserializer" -> classOf[StringDeserializer],
				"value.deserializer" -> classOf[StringDeserializer],
				"group.id" -> "test-consumer-group",
				"auto.offset.reset" -> "latest",
				"enable.auto.commit" -> (false: java.lang.Boolean)
		)
		val topics = Set("crash")
                val kafkaStream = KafkaUtils.createDirectStream
		[String, String](
                        ssc, 
                        PreferConsistent,
                        Subscribe[String, String](topics, par)
                )
		val topicData = kafkaStream.map(x => x.value)
		val structuredData = topicData.map(record => Structure(
			record.split(",", -1)(0),
			record.split(",", -1)(1),
			record.split(",", -1)(2),
			record.split(",", -1)(3),
			record.split(",", -1)(4),
			record.split(",", -1)(5),
			record.split(",", -1)(6),
			record.split(",", -1)(7),
			record.split(",", -1)(8),
			record.split(",", -1)(9),
			record.split(",", -1)(10),
			record.split(",", -1)(11),
			record.split(",", -1)(12),
			record.split(",", -1)(13),
			record.split(",", -1)(14),
			record.split(",", -1)(15),
			record.split(",", -1)(16),
			record.split(",", -1)(17),
			record.split(",", -1)(18),
			record.split(",", -1)(19),
			record.split(",", -1)(20),
			record.split(",", -1)(21),
			record.split(",", -1)(22),
			record.split(",", -1)(23),
			record.split(",", -1)(24),
			record.split(",", -1)(25),
			record.split(",", -1)(26),
			record.split(",", -1)(27),
			record.split(",", -1)(28)))
		
		val connectionProperties = new Properties()
		connectionProperties.put("driver", "org.postgresql.Driver")
		connectionProperties.put("user", "ilya")
		connectionProperties.put("password", "1234")
		structuredData.foreachRDD { rdd =>
			val df = rdd.toDF()
			if(!df.head(1).isEmpty){
				df.createOrReplaceTempView("crash")
				
				val injured = StructType(
				StructField("timestamp", TimestampType, false) :: 
				StructField("count_injured", LongType, false) ::
				StructField("count_pedestrians_injured", LongType, false) :: 
				StructField("count_cyclist_injured", LongType, false) :: 
				StructField("count_motorist_injured", LongType, false) :: Nil)	
				spark.createDataFrame(spark.sparkContext.emptyRDD[Row], injured).write.mode("ignore")
	                                .jdbc("jdbc:postgresql://localhost:5432/testing", "injured", connectionProperties)

				spark.sql("select current_timestamp timestamp, sum(cast(NUMBER_OF_PERSONS_INJURED as bigint)) count_injured, sum(cast(NUMBER_OF_PEDESTRIANS_INJURED as bigint)) count_pedestrians_injured, sum(cast(NUMBER_OF_CYCLIST_INJURED as bigint)) count_cyclist_injured, sum(cast(NUMBER_OF_MOTORIST_INJURED as bigint)) count_motorist_injured from crash")
					.write.mode("overwrite")
					.jdbc("jdbc:postgresql://localhost:5432/testing", "injured", connectionProperties)

				val raiting_zip_code = StructType(
				StructField("timestamp", TimestampType, false) ::
				StructField("rank", IntegerType, false) ::
				StructField("ZIP_CODE", StringType, false) ::
				StructField("count_crashes", LongType, false) :: Nil)
				spark.createDataFrame(spark.sparkContext.emptyRDD[Row], raiting_zip_code).write.mode("ignore")
	                                .jdbc("jdbc:postgresql://localhost:5432/testing", "raiting_zip_code", connectionProperties)
				
				spark.sql("select current_timestamp timestamp, rank() over(order by count(*) desc) rank, ZIP_CODE, count(*) count_crashes from crash group by ZIP_CODE order by count_crashes desc limit 20")
					.write.mode("overwrite")
	                                .jdbc("jdbc:postgresql://localhost:5432/testing", "raiting_zip_code", connectionProperties)
				
				val raiting_borough = StructType(
				StructField("timestamp", TimestampType, false) ::
	                        StructField("rank", IntegerType, false) ::
	                        StructField("BOROUGH", StringType, false) ::
	                        StructField("count_crashes", LongType, false) :: Nil)
	                        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], raiting_borough).write.mode("ignore")
	                                .jdbc("jdbc:postgresql://localhost:5432/testing", "raiting_borough", connectionProperties)
			
				spark.sql("select current_timestamp timestamp, rank() over(order by count(*) desc) rank, BOROUGH, count(*) count_crashes from crash group by BOROUGH order by count_crashes desc limit 10")
					.write.mode("overwrite")
	                                .jdbc("jdbc:postgresql://localhost:5432/testing", "raiting_borough", connectionProperties)
			}
		}
		ssc.start()
                ssc.awaitTermination()
        }
}


