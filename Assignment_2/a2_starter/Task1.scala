import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer

object Task1 {
  def findHighestRatingUsers(movieRating: String): (String) = {
    val tokens = movieRating.split(",")
    val movieTitle = tokens(0)
    val ratings = tokens.slice(1, tokens.size)
    val maxRating = ratings.max
    var userIds = ArrayBuffer[Int]()

    for(i <- 0 until ratings.length){
      if (ratings(i) == maxRating) {
        userIds += (i+1)
      }
    }

    return movieTitle + "," + userIds.mkString(",")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile.map(findHighestRatingUsers)
      .saveAsTextFile(args(1))
  }
}
