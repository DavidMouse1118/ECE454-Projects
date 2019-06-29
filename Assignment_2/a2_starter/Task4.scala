import org.apache.spark.{SparkContext, SparkConf}
import scala.math.min
import scala.collection.Map
import org.apache.spark.rdd.RDD

// Performance (with broadcast and hashmap)
// 45.53user 5.19system 1:09.62elapsed 72%CPU (0avgtext+0avgdata 773204maxresident)k
// 0inputs+16outputs (0major+377086minor)pagefaults 0swaps
// Found 3 items
// -rw-r--r--   2 z498zhan z498zhan          0 2019-06-29 15:50 /user/z498zhan/a2_starter_code_output/_SUCCESS
// -rw-r--r--   2 z498zhan z498zhan    1254855 2019-06-29 15:50 /user/z498zhan/a2_starter_code_output/part-00000
// -rw-r--r--   2 z498zhan z498zhan    1344210 2019-06-29 15:50 /user/z498zhan/a2_starter_code_output/part-00001
// 1254855 + 1344210 = 2599065

// Old Performance (with built-in cartesian product)
// 49.17user 5.43system 7:05.98elapsed 12%CPU (0avgtext+0avgdata 754144maxresident)k
// 0inputs+16outputs (0major+381832minor)pagefaults 0swaps
// Found 5 items
// -rw-r--r--   2 z498zhan z498zhan          0 2019-06-29 16:04 /user/z498zhan/a2_starter_code_output/_SUCCESS
// -rw-r--r--   2 z498zhan z498zhan     652680 2019-06-29 16:00 /user/z498zhan/a2_starter_code_output/part-00000
// -rw-r--r--   2 z498zhan z498zhan     602175 2019-06-29 16:00 /user/z498zhan/a2_starter_code_output/part-00001
// -rw-r--r--   2 z498zhan z498zhan     703185 2019-06-29 16:04 /user/z498zhan/a2_starter_code_output/part-00002
// -rw-r--r--   2 z498zhan z498zhan     641025 2019-06-29 16:03 /user/z498zhan/a2_starter_code_output/part-00003
// 652680 + 602175 + 703185 + 641025 = 2599065

object Task4 {
  def findSimilarity(ratings1: Array[Byte], ratings2: Array[Byte]): Int = {
    var similarity = 0

    for(i <- 0 until ratings1.length){
      if (ratings1(i) == ratings2(i) && ratings1(i) != 0) {
        similarity += 1
      }
    }

    return similarity
  } 

  def buildMovieRatingsMap(movieRatings: RDD[String]): Map[String, Array[Byte]] = {
    movieRatings.map(movieRating => {
      val tokens = movieRating.split(",", -1)
      val title = tokens(0)
      val ratings = new Array[Byte](tokens.length - 1)

      for (i <- 0 until ratings.length) {
        if (tokens(i + 1) != "") {
          ratings(i) = tokens(i + 1).toByte
        }
      }
      (title, ratings)
    }).collectAsMap()
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val movieRatingsMap = sc.broadcast(buildMovieRatingsMap(textFile))

    val output = textFile.flatMap(movie1 => {
      val title1 = movie1.split(",", 2)(0)
      val ratings1 = movieRatingsMap.value(title1)

      movieRatingsMap.value
        .filterKeys(title2 => title2 > title1)
        .map(movie2 => {
          val title2 = movie2._1
          val ratings2 = movie2._2

          val similarity = findSimilarity(ratings1, ratings2)
          (title1 + "," + title2 + "," + similarity)
        })
    }).saveAsTextFile(args(1))
  }
}
