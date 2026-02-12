from graphframes import GraphFrame
from pyspark.sql import SparkSession
import pyspark

spark = SparkSession.builder \
    .appName("GraphFrames Example") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.1-s_2.12") \
    .getOrCreate()

# Create a sample graph (vertices and edges)
vertices = spark.createDataFrame([("a",), ("b",), ("c",), ("d",)], ["id"])
edges    = spark.createDataFrame([("a", "b"), ("b", "c"), ("c", "d")], ["src", "dst"])
g = GraphFrame(vertices, edges)

# Compute connected components
# = g.connectedComponents()

# Display the result (vertices with component IDs)
#result.vertices.show()
g.vertices.show()
g.edges.show()

spark.stop()