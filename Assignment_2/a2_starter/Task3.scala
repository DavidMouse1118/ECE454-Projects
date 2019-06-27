import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer

// please don't change the object name
object Task3 {
  def userIdToRatingCount(line: String): ArrayBuffer[(Int, Int)] = {
      val ratings = line.split(",")
      var userRatingCount = ArrayBuffer[(Int, Int)]()

      for(i <- 1 until ratings.length){
        if (ratings(i) != "") {
          val count = (i, 1)
          userRatingCount += count
        }
      }

      return userRatingCount
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile.flatMap(userIdToRatingCount)
      .reduceByKey(_ + _)
      .map(x => x._1 + "," + x._2)
      .saveAsTextFile(args(1))
  }
}
