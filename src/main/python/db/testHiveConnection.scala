import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
class SparkHive{

  def main(args: Array[String]): Unit = {

    val conf = new org.apache.spark.SparkConf().setAppName("Spark to Hive Connectivity")
    //val sc = new org.apache.spark.SparkContext(conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val df = hiveContext.sql("show tables")
    df.show()
    val df3 = hiveContext.sql("show databases")
    df3.show()
  }
}
