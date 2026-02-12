package BDAPrj
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GraphTextBook {
  def main(args: Array[String]): Unit = {
    // Create a SparkConf object
    val conf = new SparkConf()
      .setAppName("Graph Creation")
      .setMaster("local[*]") // Adjust "local[*]" based on your cluster setup
    // Create SparkContext
    val sc = new SparkContext(conf)

    case class User(name: String, age: Int)
    val users = List((1L, User("Alex", 26)), (2L, User("Bill", 42)), (3L, User("Carol", 18)),
      (4L, User("Dave", 16)), (5L, User("Eve", 45)), (6L, User("Farell", 30)),
      (7L, User("Garry", 32)), (8L, User("Harry", 36)), (9L, User("Ivan", 28)),
      (10L, User("Jill", 48))
    )
    val usersRDD = sc.parallelize(users)
    val follows = List(Edge(1L, 2L, 1), Edge(2L, 3L, 1), Edge(3L, 1L, 1), Edge(3L, 4L, 1),
      Edge(3L, 5L, 1), Edge(4L, 5L, 1), Edge(6L, 5L, 1), Edge(7L, 6L, 1),
      Edge(6L, 8L, 1), Edge(7L, 8L, 1), Edge(7L, 9L, 1), Edge(9L, 8L, 1),
      Edge(8L, 10L, 1), Edge(10L, 9L, 1), Edge(1L, 11L, 1)
    )
    val followsRDD = sc.parallelize(follows)
    val defaultUser = User("NA", 0)

    // Create the Graph
    val socialGraph = Graph(usersRDD, followsRDD, defaultUser)

    // Print no. of edges & vertices
    val numEdges = socialGraph.numEdges
    val numVertices = socialGraph.numVertices

    println("No. of Vertices = " + numVertices)
    println("No. of Edges = " + numEdges)

    // Alter the attribute of the edges to "follows"
    val followsGraph = socialGraph.mapEdges( (n) => "follows")
    for (edge <- followsGraph.edges.collect()) {
      println(s"Edge: ${edge.srcId} -> ${edge.dstId}, Attribute: ${edge.attr}")
    }
  }
}

