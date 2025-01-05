import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession


object HelloWorld {
  def main(args: Array[String]): Unit = {

    println( "Hello spark world !" )

    //val spark: SparkSession = SparkSession.builder.appName("app_hello_p2").getOrCreate()

/*
    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()
*/


      val conf = new SparkConf().setAppName( "hello_world" )
      conf.setMaster("local[*]")
      val sc = new SparkContext(conf)


    println( "bye sharks !" )

  }
}