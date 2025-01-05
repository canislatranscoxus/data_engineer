import org.apache.spark._
import org.apache.spark.sql.SparkSession

object sql_01 {

  def main(args: Array[String]) {

    println("\n\n Hello SQL")

    val spark = SparkSession
      .builder()
      .appName( "sql_01" )
      .master( "local[*]" )
      .getOrCreate()

    spark.sparkContext.setLogLevel( "ERROR" )

    println("\n\n bye SQL")

  }
}