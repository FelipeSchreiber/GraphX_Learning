import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import scala.io.Source
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
              val row = line split '\t'
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
  }
}