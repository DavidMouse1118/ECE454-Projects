import org.apache.spark.{SparkContext, SparkConf}

object SparkWC {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Word Count Application")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val counts = textFile.flatMap(line => line.split(","))
                 .map(word => (word, 1 ))
                 .reduceByKey(_ + _)
		 .map(x => x._1 + "," + x._2)
		 .saveAsTextFile(args(1))
  }
}
