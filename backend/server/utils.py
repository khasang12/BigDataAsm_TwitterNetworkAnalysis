import gc
import itertools
import numpy as np
from typing import Dict, List, Tuple

TOP_K_PREDICTIONS = 8

def calculate_adjacency_matrix(embeddings: Dict[int, List[float]], threshold=0.0) -> Dict[Tuple[int, int], float]:
    def get_edge_weight(i, j) -> float:
        if embeddings[i] is None or embeddings[j] is None:
            return -1
        return np.dot(embeddings[i], embeddings[j])

    nodes = list(embeddings.keys())
    nodes = sorted(nodes)
    adj_mtx_r = {}
    cnt = 0
    for pair in itertools.combinations(nodes, 2):

        if cnt % 1000000 == 0:
            adj_mtx_r = {k: v for k, v in sorted(adj_mtx_r.items(), key=lambda item: -1 * item[1])}
            adj_mtx_r = {k: adj_mtx_r[k] for k in list(adj_mtx_r)[:TOP_K_PREDICTIONS]}
            gc.collect()

        weight = get_edge_weight(pair[0], pair[1])
        if weight <= threshold:
            continue
        cnt += 1
        adj_mtx_r[(pair[0], pair[1])] = get_edge_weight(pair[0], pair[1])

    return adj_mtx_r