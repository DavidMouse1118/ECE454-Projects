import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def getMovieRatings(line: String): (Array[String]) = {
    val tokens = line.split(",", -1)
    val ratings = tokens.slice(1, tokens.size)

    return ratings.filter(_ != "")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Task 2")
      .set("spark.default.parallelism", "1") // Force only one reducer

    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile.flatMap(getMovieRatings)
      .map(_ => ("count", 1 ))
      .reduceByKey(_ + _)
      .map(x => x._2)
      .saveAsTextFile(args(1))
  }
}
