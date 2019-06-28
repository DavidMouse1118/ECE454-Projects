import org.apache.spark.{SparkContext, SparkConf}
import scala.math.min

// please don't change the object name
// Performance: 44.21user 5.67system 7:04.37elapsed 11%CPU (0avgtext+0avgdata 755708maxresident)k
object Task4 {
  def findSimilarity(movie1: String, movie2: String): String = {
    val ratings1 = movie1.split(",")
    val ratings2 = movie2.split(",")

    val moviePair = ratings1(0) + "," + ratings2(0)
    var similarity = 0

    for(i <- 1 until min(ratings1.length, ratings2.length)){
      if (ratings1(i) == ratings2(i) && ratings1(i) != "") {
        similarity += 1
      }
    }

    return moviePair + "," + similarity
  } 

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.cartesian(textFile)
      .filter{case (a,b) => a < b}
      .map{case (a, b) => findSimilarity(a, b)}
      .saveAsTextFile(args(1))
  }
}
