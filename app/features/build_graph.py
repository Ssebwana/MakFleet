import os
import numpy as np
import pandas as pd
from app.db.postgres import get_pg_connection

OUTPUT_DIR = "data/processed"


def fetch_nodes():
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT node_id, node_name, node_type
                FROM campus_nodes
                ORDER BY node_id
            """)
            rows = cur.fetchall()
            return pd.DataFrame(rows)
    finally:
        conn.close()


def fetch_edges():
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT edge_id, source_node, target_node, edge_type, speed_limit_kmh
                FROM campus_edges
                ORDER BY edge_id
            """)
            rows = cur.fetchall()
            return pd.DataFrame(rows)
    finally:
        conn.close()


def build_graph():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    nodes_df = fetch_nodes()
    edges_df = fetch_edges()

    if nodes_df.empty:
        raise ValueError("No campus nodes found.")
    if edges_df.empty:
        raise ValueError("No campus edges found.")

    # Keep only complete edges
    edges_df = edges_df.dropna(subset=["source_node", "target_node"]).copy()

    # Convert safely
    edges_df["source_node"] = pd.to_numeric(edges_df["source_node"], errors="coerce")
    edges_df["target_node"] = pd.to_numeric(edges_df["target_node"], errors="coerce")

    # Drop anything still invalid
    edges_df = edges_df.dropna(subset=["source_node", "target_node"]).copy()
    edges_df["source_node"] = edges_df["source_node"].astype(int)
    edges_df["target_node"] = edges_df["target_node"].astype(int)

    node_id_to_idx = {
        int(node_id): idx for idx, node_id in enumerate(nodes_df["node_id"].tolist())
    }

    valid_edges = edges_df[
        edges_df["source_node"].isin(node_id_to_idx.keys()) &
        edges_df["target_node"].isin(node_id_to_idx.keys())
    ].copy()

    if valid_edges.empty:
        raise ValueError("No valid campus edges found after cleaning.")

    sources = valid_edges["source_node"].map(node_id_to_idx).tolist()
    targets = valid_edges["target_node"].map(node_id_to_idx).tolist()

    # Undirected graph
    edge_index = np.array(
        [
            sources + targets,
            targets + sources
        ],
        dtype=np.int64
    )

    edge_weight = np.ones(edge_index.shape[1], dtype=np.float32)

    node_map = nodes_df.copy()
    node_map["node_idx"] = node_map["node_id"].map(node_id_to_idx)

    np.save(os.path.join(OUTPUT_DIR, "edge_index.npy"), edge_index)
    np.save(os.path.join(OUTPUT_DIR, "edge_weight.npy"), edge_weight)
    node_map.to_csv(os.path.join(OUTPUT_DIR, "node_map.csv"), index=False)

    print("Saved edge_index.npy, edge_weight.npy, node_map.csv")


if __name__ == "__main__":
    build_graph()