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
    esJobConf.set(ConfigurationOptions.ES_NODES, "localhost")
    //esJobConf.set(ConfigurationOptions.ES_NODES, "https://fzan4xer:1l0nx4qxyuup12ul@azalea-9794680.us-east-1.bonsai.io/")
    esJobConf.set(ConfigurationOptions.ES_PORT, "9200")
    //esJobConf.set(ConfigurationOptions.ES_PORT, "80")
    FileOutputFormat.setOutputPath(esJobConf, new Path("-"))

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val fakePath = "/Users/alex/ensemble-faker/fake-news.json"
    val fake = sqlContext.jsonFile(fakePath)
    fake.printSchema()
    fake.saveToEs("hackathon/news")

  }

}
