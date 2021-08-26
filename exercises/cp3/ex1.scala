import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import scala.io.Source
import org.graphstream.graph.{Graph => GraphStream}
import org.graphstream.graph.implementations._
import org.jfree.chart.axis.ValueAxis
import breeze.linalg._
import breeze.plot._

//A DEFINICAO DAS CLASSES PRECISAM ESTAR FORA DA DEFINICAO DO OBJETO
//abstract class VertexProperty extends java.io.Serializable
abstract class FNNode{val name: String}
case class Ingredient(name: String, category: String)  extends FNNode
case class Compound(name: String, cas: String) extends FNNode


object SimpleGraphApp {
  def main(args: Array[String]){
    // Configure the program 
    val conf = new SparkConf()
           .setAppName("Flavors")
           .setMaster("local")
           .set("spark.driver.memory", "2G")
    val sc = new SparkContext(conf)
    //especifica a tecnologia utilizada para efetuar a visualização
    System.setProperty("org.graphstream.ui", "swing");
    
    // Load some data into RDDs
    Source
    .fromFile("/home/felipe/UFRJ/TCC/spark-3.1.2-bin-hadoop3.2/data/graphx/ingr_comp/ingr_info.tsv")
    .getLines()
    .take(7)
    .foreach(println)
    val filePath = "/home/felipe/UFRJ/TCC/spark-3.1.2-bin-hadoop3.2/data/graphx/ingr_comp/"
   
    val ingredients: RDD[(VertexId, FNNode)] =
        sc.textFile(filePath+"ingr_info.tsv")
        .filter(! _.startsWith("#"))
        .map {
              line =>
              val row = line.split('\t')
              (row(0).toInt, Ingredient(row(1), row(2)))
             }

    ingredients.take(5).map{x => println(x)}

    val compounds: RDD[(VertexId, FNNode)] =
        sc.textFile(filePath+"comp_info.tsv")
        .filter(! _.startsWith("#"))
        .map{
              line =>
              val row = line split '\t'
              (10000L + row(0).toInt, Compound(row(1), row(2)))
            }
        
    val links: RDD[Edge[Int]] =
        sc.textFile(filePath+"ingr_comp.tsv")
        .filter(! _.startsWith("#"))
        .map {
              line =>
              val row = line split '\t'
              Edge(row(0).toInt, 10000L + row(1).toInt, 1)
            }

    val nodes = ingredients ++ compounds
    val foodNetwork = Graph(nodes, links)
    def showTriplet(t: EdgeTriplet[FNNode,Int]): String =
      "The ingredient " ++ t.srcAttr.name ++ " contains " ++ t.dstAttr.name
    foodNetwork.triplets.take(5).
      foreach(showTriplet _ andThen println _)
    
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = 
    {
      if (a._2 > b._2) a else b
    }  
    val mostCommonIngredientId = foodNetwork.outDegrees.reduce(max)
    val mostCommonIngredientName = foodNetwork.vertices.filter(_._1 == mostCommonIngredientId._1).collect()
    println("Obtendo o ingrediente que contém o maior número de compostos: "+mostCommonIngredientName(0)._2.name)
    val recipeWithMostIngredientsId = foodNetwork.inDegrees.reduce(max)
    val recipeWithMostIngredientsName = foodNetwork.vertices.filter(_._1 == recipeWithMostIngredientsId._1).collect()
    println("Obtendo o composto mais presente em ingredientes: "+recipeWithMostIngredientsName(0)._2.name)

    // Create a SingleGraph class for GraphStream visualization
    // graph.addAttribute("ui.stylesheet","url(file:.//style/stylesheet)")
    // graph.addAttribute("ui.quality")
    // graph.addAttribute("ui.antialias")

    //plota o grafo em si
    // Create a SingleGraph class for GraphStream visualization
    val graph: SingleGraph = new SingleGraph("FoodNetwork")
    graph.setStrict(false);
    graph.setAutoCreate(true);
    // Given the foodNetwork, load the graphX vertices into GraphStream
    for ((id,_) <- foodNetwork.vertices.collect()) {
      val node = graph.addNode(id.toString).asInstanceOf[SingleNode]
    }
    // Load the graphX edges into GraphStream edges
    for (Edge(x,y,_) <- foodNetwork.edges.collect()) {
      val edge = graph.addEdge(x.toString ++ y.toString,
      x.toString, y.toString,true)
      .asInstanceOf[AbstractEdge]
    }
    graph.display(false)
    // println(graph)

    //SHOW DEGREE DISTRIBUTION
    def degreeHistogram(net: Graph[FNNode, Int]): Array[(Int, Int)] =
      net.degrees.map(t => (t._2,t._1)).
      groupByKey.map(t => (t._1,t._2.size)).
      sortBy(_._1).collect()

    val nn = foodNetwork.numVertices
    val degreeDistribution = degreeHistogram(foodNetwork).map({case
      (d,n) => (d,n.toDouble/nn)})
    //normalize the total frequencies by the number of vertices
    val f = Figure()
    val p1 = f.subplot(2,1,0)
    val x = new DenseVector(degreeDistribution.map(_._1.toDouble))
    val y = new DenseVector(degreeDistribution.map(_._2))
    p1.xlabel = "Degrees"
    p1.ylabel = "Distribution"
    p1 += plot(x, y)
    p1.title = "Degree distribution of food network"
    val p2 = f.subplot(2,1,1)
    val foodDegrees = foodNetwork.degrees.map(_._2).collect()
    p1.xlabel = "Degrees"
    p1.ylabel = "Histogram of node degrees"
    p2 += hist(foodDegrees, 10)
  }
}