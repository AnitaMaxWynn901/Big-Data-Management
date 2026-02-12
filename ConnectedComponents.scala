package BDAPrj
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ConnectedComponents {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Graph Creation")
      .setMaster("local[*]") // Adjust "local[*]" based on your cluster setup
    // Create SparkContext
    val sc = new SparkContext(conf)

    val vertexArray = Array(
      (1L, ("A", 28)),
      (2L, ("B", 27)),
      (3L, ("C", 65)),
      (4L, ("D", 42)),
      (5L, ("E", 55)),
      (6L, ("F", 50)),
      (7L, ("G", 53)),
    )

    // Vertices 1 - 6 are connected, 7 and 8 are connected.
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 20),
      Edge(3L, 2L, 4),
      Edge(3L, 1L, 4),
      Edge(4L, 5L, 1),
      Edge(4L, 7L, 2),
      Edge(4L, 6L, 8),
      Edge(5L, 6L, 3),
      Edge(7L, 6L, 3)
    )

    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]]               = sc.parallelize(edgeArray)
    val graph: Graph[(String, Int), Int]      = Graph(vertexRDD, edgeRDD)

    val cc = graph.connectedComponents().vertices.collectAsMap()

    cc.foreach {
      case (vertexId, clusterId) =>
        println(s"Vertex $vertexId belongs to cluster $clusterId")
    }
    sc.stop()
  }
}