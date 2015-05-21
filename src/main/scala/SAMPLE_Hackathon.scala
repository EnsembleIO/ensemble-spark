import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql._

import scala.collection.immutable.HashMap


object SAMPLE_Hackathon {
  def main(args: Array[String]) {

    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)
    sc.setLocalProperty("spark.serializer", classOf[KryoSerializer].getName)


    // Elasticsearch-Hadoop setup
    val esJobConf = new JobConf(sc.hadoopConfiguration)
    //esJobConf.setOutputFormat(classOf[EsOutputFormat])
    esJobConf.setOutputCommitter(classOf[FileOutputCommitter])
    esJobConf.set(ConfigurationOptions.ES_NODES, "127.0.0.1")
    esJobConf.set(ConfigurationOptions.ES_PORT, "9200")
    FileOutputFormat.setOutputPath(esJobConf, new Path("-"))

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    /*case class TraceIn ( ID: String,
                         SID: String,
                         DATE_LOG: String,
                         WEIGHT: Double
                         )*/

    val newsPath = "/Users/alex/ensemble-faker/fake-news.json"

    val news = sqlContext.jsonFile(newsPath)

    news.printSchema()

    news.registerTempTable("news")


    // Alternatively, a SchemaRDD can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)

    news.saveToEs("test/test")

    /*
    println("----Depense IN----")
    val traceIn = sc.textFile("/Users/alex/ensemble-faker/fake-weight*.csv").map(_.split(";")).map(
      c => (c(0), TraceIn(
        c(0),
        c(1),
        c(2),
        c(3).toDouble
      ))
    )
    traceIn.take(3).foreach(println)

    traceIn.map({case (k,v) => v}).map(rowToMap).saveToEs("hackathon/news")

    def rowToMap(t: TraceIn) = {
      val fields = HashMap(
        "id" -> t.ID,
        "sid" -> t.SID,
        "dateTrace" -> t.DATE_LOG,
        "weight" -> t.WEIGHT
      )
      fields
    }
    */

  }

}
