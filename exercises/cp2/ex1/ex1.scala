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
    val emailGraph = GraphLoader.edgeListFile( sc,
    "/home/felipe/UFRJ/TCC/spark-3.1.2-bin-hadoop3.2/data/graphx/Email-Enron.txt",false,1)
    val verticesAttr = emailGraph.vertices.take(5)
    verticesAttr.map {line => println(line)}
    val edgesAttr = emailGraph.edges.take(5)
    edgesAttr.map {line => println(line)}
    val outgoingLinks = emailGraph.edges.filter(_.srcId == 19021).map(_.dstId).collect()
    outgoingLinks.map {line => println(line)}
    val incomingLinks = emailGraph.edges.filter(_.dstId == 19021).map(_.srcId).collect()
    incomingLinks.map {line => println(line)}
    println("Numero de vértices: " + emailGraph.numVertices+" Numero de arestas: "+emailGraph.numEdges)
    println("Grau médio: "+emailGraph.inDegrees.map(_._2).sum / emailGraph.numVertices)
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = 
    {
      if (a._2 > b._2) a else b
    }
    println("Encontrando o vértice que mais mandou emails: "+emailGraph.outDegrees.reduce(max))
    println("Encontrando os vértices que menos mandaram emails: "+emailGraph.outDegrees.filter(_._2 <= 1).count)
    val histogram = emailGraph.degrees.
      map(t => (t._2,t._1)).
      groupByKey.map(t => (t._1,t._2.size)).
      sortBy(_._1).collect()
    histogram.map{println}
  }
}