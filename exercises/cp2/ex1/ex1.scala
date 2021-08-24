import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark._

object SimpleGraphApp {
  def main(args: Array[String]){
    // Configure the program 
    val conf = new SparkConf()
          .setAppName("Tiny Social")
          .setMaster("local")
          .set("spark.driver.memory", "2G")
    val sc = new SparkContext(conf)
    // Load some data into RDDs
    val graph = GraphLoader.edgeListFile( sc,
    "/home/felipe/UFRJ/TCC/spark-3.1.2-bin-hadoop3.2/data/graphx/Email-Enron.txt",false,1)
    val verticesAttr = graph.vertices.take(5)
    verticesAttr.map {line => println(line)}
    val edgesAttr = graph.edges.take(5)
    edgesAttr.map {line => println(line)}
    val outgoingLinks = graph.edges.filter(_.srcId == 19021).map(_.dstId).collect()
    outgoingLinks.map {line => println(line)}
    val incomingLinks = graph.edges.filter(_.dstId == 19021).map(_.srcId).collect()
    incomingLinks.map {line => println(line)}
  }
}