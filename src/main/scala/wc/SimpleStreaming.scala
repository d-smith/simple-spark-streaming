import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object SimpleStreaming {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into tokens.  A line has the app id and the service called
    val words = lines.flatMap(_.split(" "))

    val servicesByApp = lines.map(l => {
      val split = l.split(" ")
      (split(0),split(1))
    })

    val services = servicesByApp map {
      p => p._2
    }

    servicesByApp.countByValue().print()
    services.countByValue().print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
