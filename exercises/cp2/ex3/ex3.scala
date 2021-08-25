import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import scala.io.Source
import scala.math.abs
import breeze.linalg.SparseVector

object SimpleGraphApp {
  def main(args: Array[String]){
    // Configure the program 
    val conf = new SparkConf()
           .setAppName("Flavors")
           .setMaster("local")
           .set("spark.driver.memory", "2G")
    val sc = new SparkContext(conf)

    type Feature = breeze.linalg.SparseVector[Int]//essa linha precisa estar dentro do object
    // Load some data into RDD
    val filePath = "/home/felipe/UFRJ/TCC/spark-3.1.2-bin-hadoop3.2/data/graphx/"
   
    val featureMap: Map[Long, Feature] =
      Source.fromFile(filePath+"ego.feat")
      .getLines()
      .map
      {
        line =>
          val row = line split ' '
          val key = abs(row.head.hashCode.toLong)
          val feat = SparseVector(row.tail.map(_.toInt))
          (key, feat)
      }.toMap

    val edges: RDD[Edge[Int]] =
      sc.textFile(filePath+"gplus_combined.txt")
      .map 
      {
        line =>
        val row = line split ' '
        val srcId = abs(row(0).hashCode.toLong)
        val dstId = abs(row(1).hashCode.toLong)
        val srcFeat = featureMap(srcId)
        val dstFeat = featureMap(dstId)
        val numCommonFeats = srcFeat dot dstFeat
        Edge(srcId, dstId, numCommonFeats)
      }
    // val egoNetwork: Graph[Int,Int] = Graph.fromEdges(edges, 1)

    val emailGraph = GraphLoader.edgeListFile(sc,filePath+"Email-Enron.txt",false,1)
    //println("Numero de arestas com peso 3: "+egoNetwork.edges.filter(_.attr == 3).count())
  }
}