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
    "/home/felipe/UFRJ/TCC/spark-3.1.2-bin-hadoop3.2/data/graphx/followers.txt",false,1)
    // Define a reduce operation to compute the highest degree vertex
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    // Compute the max degrees
    val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
    println("MAX degree is: "+maxInDegree+" and has "+graph.numEdges+" edges")
  }
}