# pip install networkx
# pip install graphframes
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from graphframes import GraphFrame
spark = SparkSession.builder \
    .appName("GraphFrames Example") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.1-s_2.12") \
    .getOrCreate()

# Create vertices DataFrame
vertices = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "Darwin")
], ["id", "name"])

# Create edges DataFrame
edges = spark.createDataFrame([
    (1, 2, "friend"),
    (2, 3, "friend"),
    (2, 4, "follows"),
    (3, 4, "likes")
], ["src", "dst", "relationship"])

# Create GraphFrame
g = GraphFrame(vertices, edges)

# Show the vertices and edges
g.vertices.show()
g.edges.show()

# Detect Communities =================> ERROR
#result = g.connectedComponents()
#result.show()

# Calculate PageRank
results = g.pageRank(resetProbability=0.15, maxIter=10)
results.vertices.select("id", "name", "pagerank").show()

# Filter edges for users who are friends with Bob (id 2)
subgraph = g.edges.filter("src = 2 OR dst = 2")
real_subgraph = GraphFrame(g.vertices, subgraph)

# Show the subgraph
real_subgraph.edges.show()

# Run Breadth First Search on the original graph, g
bfs_result = g.bfs(fromExpr="id = '1'", toExpr="id = '4'")
bfs_result.show()

# Find communities using edge betweenness
communities = g.communityEdgeBetweenness()

# Show the communities...........................
print("Coummnities using Betweenness:")
communities.show()

"""
# Draw the Graph with nodes and edges
# NetworkX is a powerful, open-source Python library to create & manipulate Graphs
import networkx as nx
G = nx.Graph()

print('Creating a Graph with networkx') 
G.add_node(1)
G.add_node(2)
print(G.nodes())
G.add_edge(1,2)
print(G.nodes())
G.add_node(3)
G.add_edge(3,2)
print(G.edges())
 
# Get node numbers and add them to the Graph
row_list = vertices.select('id').collect()
mynodelist = [ int(row.id) for row in row_list ]
print(mynodelist)
G.add_nodes_from(mynodelist)

# Get edges and add them to the Graph
row_list = edges.select('src', 'dst').collect()
myedgelist = []
for row in row_list:
    myedgelist.append(row)
print(myedgelist)
G.add_edges_from(myedgelist)

# Draw the Graph
options = {
    'node_color' : 'maroon',
    "font_color" : "white",
    'node_size'  : 1000,
    'width'      : 3,
    'font_weight': 'bold'
}
nx.draw_shell(G, with_labels=True, **options)
plt.show()
"""
spark.stop()