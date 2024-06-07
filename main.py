import pandas as pd

# Load the dataset
file_path = "p2p-Gnutella04.txt"
data = pd.read_csv(
    file_path, sep="\t", comment="#", header=None, names=["FromNodeId", "ToNodeId"]
)

# Find all sinks (nodes with no outgoing edges)
outgoing_counts = data["FromNodeId"].value_counts()
all_nodes = set(data["FromNodeId"]).union(set(data["ToNodeId"]))
sinks = sorted([node for node in all_nodes if node not in outgoing_counts])

# Save the sinks to sinks.csv
sinks_df = pd.DataFrame(sinks, columns=["NodeId"])
sinks_df.to_csv("sinks.csv", index=False)

# Remove the sinks from the data
data = data[~data["FromNodeId"].isin(sinks)]


# Implement the PageRank algorithm
def compute_pagerank(data, iterations=10, damping_factor=0.85):
    all_nodes = set(data["FromNodeId"]).union(set(data["ToNodeId"]))
    N = len(all_nodes)
    initial_pagerank = 1 / N

    # Initialize PageRank values
    pagerank = {node: initial_pagerank for node in all_nodes}

    # Create adjacency lists for in-links and out-links
    out_links = {node: [] for node in all_nodes}
    in_links = {node: [] for node in all_nodes}

    for from_node, to_node in data.values:
        out_links[from_node].append(to_node)
        in_links[to_node].append(from_node)

    # Perform PageRank iterations
    for _ in range(iterations):
        new_pagerank = {node: (1 - damping_factor) / N for node in all_nodes}
        for node in all_nodes:
            for in_link in in_links[node]:
                if len(out_links[in_link]) > 0:
                    new_pagerank[node] += (
                        damping_factor * pagerank[in_link] / len(out_links[in_link])
                    )
        pagerank = new_pagerank

    return pagerank


# Compute PageRank
pagerank = compute_pagerank(data)

# Save the PageRank results to PR_results.csv
pagerank_df = pd.DataFrame(pagerank.items(), columns=["NodeId", "PageRank"])
pagerank_df = pagerank_df.sort_values(by="PageRank", ascending=False)
pagerank_df.to_csv("PR_results.csv", index=False)
