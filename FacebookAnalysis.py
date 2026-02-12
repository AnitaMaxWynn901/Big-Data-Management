# Facebook Data Analysis
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from   random import randint

#%matplotlib inline
facebook = pd.read_csv(
    "facebook_combined.txt.gz",
    compression="gzip",
    sep=" ",
    names=["start_node", "end_node"],
)
#print(facebook)

G = nx.from_pandas_edgelist(facebook, "start_node", "end_node")
fig, ax = plt.subplots(figsize=(15, 9))
ax.axis("off")
plot_options = {"node_size": 10, "with_labels": False, "width": 0.15}
nx.draw_networkx(G, pos=nx.random_layout(G), ax=ax, **plot_options)

pos = nx.spring_layout(G, iterations=15, seed=1721)
fig, ax = plt.subplots(figsize=(15, 9))
ax.axis("off")
nx.draw_networkx(G, pos=pos, ax=ax, **plot_options)
plt.show()

# Total number of nodes & edges in network
print(G.number_of_nodes())
print(G.number_of_edges())
