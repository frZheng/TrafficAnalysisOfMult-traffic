import GeneralFunctionSets.transTimeToTimestamp
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object sql {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.csv("D:\\subwayData\\spark\\data\\simulate.csv")
      .toDF("id", "state", "station", "tripTime")
//    df.show()


//    df.createOrReplaceTempView("trip")
//    spark.sql("select * from trip").show
//    spark.sql("select id, state from trip").show
//    spark.sql("select avg(age) from trip").show
//    spark.sql("select id, state, station, tripTime from trip group by id").show
//    val dataset:DataFrame = df.groupBy(col("id"))
    spark.stop()
  }
}
