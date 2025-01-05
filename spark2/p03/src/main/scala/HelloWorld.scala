import org.apache.spark._


object HelloWorld {

  def main(args: Array[String]) {

    println( "Hello World" )

    // Set up a SparkContext named WordCount that runs locally using
    // all available cores.
    val conf = new SparkConf().setAppName("WordCount")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    println( "bye sharks" )

    }


  }
