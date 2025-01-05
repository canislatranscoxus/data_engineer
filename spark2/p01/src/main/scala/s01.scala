// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object s01 {

  def main( args: Array[ String ] ): Unit = {
    print( "s01, ... begin" )

    // initialise spark context
    //val conf = new SparkConf().setAppName( s01.getClass.getName )
    //val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    val spark: SparkSession = SparkSession.builder
      .appName( "app_s01" )
      .getOrCreate()

    // do stuff
    println("************")
    println("************")
    println("Hello, world!")
    val rdd = spark.sparkContext.parallelize(Array(1 to 10))
    rdd.count()
    println("************")
    println("************")

    // terminate spark context
    spark.stop()


  }

}
