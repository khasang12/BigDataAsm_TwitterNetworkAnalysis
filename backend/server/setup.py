from typing import Dict, List
from gqlalchemy import (
    Memgraph,
    MemgraphKafkaStream,
    MemgraphPulsarStream,
    MemgraphTrigger,
)
from gqlalchemy.models import (
    TriggerEventType,
    TriggerEventObject,
    TriggerExecutionPhase,
)
from time import sleep
import logging
import os

from server.utils import calculate_adjacency_matrix

BROKER = os.getenv("BROKER", "kafka")

log = logging.getLogger("server")


def connect_to_memgraph(memgraph_ip, memgraph_port):
    memgraph = Memgraph(host=memgraph_ip, port=int(memgraph_port))
    while True:
        try:
            if memgraph._get_cached_connection().is_active():
                memgraph.execute("""CALL node2vec_online.set_streamwalk_updater(7200, 2, 0.9, 604800, 2, True) YIELD message RETURN message;""")
                memgraph.execute("""CALL node2vec_online.set_word2vec_learner(2, 0.01, True, 1, 1) YIELD message RETURN message;""")
                return memgraph
        except:
            log.info("Memgraph probably isn't running.")
            sleep(1)

def get_embeddings(mg):
    rows = mg.execute_and_fetch("""CALL node2vec_online.get() YIELD node, embedding""")
    node_embeddings = []
    for row in rows:
        node_embeddings.append({
            "node_id": row['node'].__dict__["_id"],
            "node_embedding": row['embedding']
        })
    return node_embeddings

def get_predictions(mg, limit=50):
    embs = get_embeddings(mg)
    embeddings: Dict[int, List[float]] = {}
    for item in embs:
        embeddings[item["node_id"]] = item["node_embedding"]
    adj_matrix = calculate_adjacency_matrix(embeddings)
    sorted_predicted_edges = {k: v for k, v in sorted(adj_matrix.items(), key=lambda item: -1 * item[1])}
    preds = []
    lim = int(limit)
    for edge, similarity in sorted_predicted_edges.items():
        if lim < 0:
            break
        preds.append([{
            "vertex_from": edge[0],
            "vertex_to": edge[1],
            "similarity": similarity
        }])
        lim -= 1
    return preds


def run(memgraph):
    try:
        memgraph.drop_database()
        #print("Setting up PageRank")
        memgraph.execute("CALL pagerank_online.set(100, 0.2) YIELD *")
        memgraph.execute(
            """CREATE TRIGGER pagerank_trigger 
               BEFORE COMMIT 
               EXECUTE CALL pagerank_online.update(createdVertices, createdEdges, deletedVertices, deletedEdges) YIELD *
               SET node.rank = rank
               CALL publisher.update_rank(node, rank);"""
        )

        #print("Setting up community detection")
        memgraph.execute(
            "CALL community_detection_online.set(True, False, 0.7, 4.0, 0.1, 'weight', 1.0, 100, 5) YIELD *;"
        )
        memgraph.execute(
            """CREATE TRIGGER labelrankt_trigger 
               BEFORE COMMIT
               EXECUTE CALL community_detection_online.update(createdVertices, createdEdges, updatedVertices, updatedEdges, deletedVertices, deletedEdges) 
               YIELD node, community_id
               SET node.cluster=community_id
               CALL publisher.update_cluster(node, community_id);"""
        )
        
        #print("Creating triggers on Memgraph")
        memgraph.execute(
            "CREATE TRIGGER created_trigger ON CREATE AFTER COMMIT EXECUTE CALL publisher.create(createdObjects)"
        )

        #print("Creating stream connections on Memgraph")
        # memgraph.execute(
        #     "CREATE KAFKA STREAM retweets TOPICS retweets TRANSFORM twitter.tweet"
        # )

        # memgraph.execute("START STREAM retweets")
        # if BROKER == "kafka":
        #     stream = MemgraphKafkaStream(
        #         name="retweets",
        #         topics=["retweets"],
        #         transform="twitter.tweet",
        #         bootstrap_servers="'kafka:9092'",
        #     )
        #     print("stream created")
        # else:
        #     stream = MemgraphPulsarStream(
        #         name="retweets",
        #         topics=["retweets"],
        #         transform="twitter.tweet",
        #         service_url="'pulsar://pulsar:6650'",
        #     )
        # print("gogogo start")
        # memgraph.create_stream(stream)
        # memgraph.start_stream(stream)
        # print("gogogo end")

        # log.info("Creating triggers on Memgraph")
        # trigger = MemgraphTrigger(
        #     name="created_trigger",
        #     event_type=TriggerEventType.CREATE,
        #     event_object=TriggerEventObject.ALL,
        #     execution_phase=TriggerExecutionPhase.AFTER,
        #     statement="CALL publisher.create(createdObjects)",
        # )
        # memgraph.create_trigger(trigger)
        # print("create_trigger")

    except Exception as e:
        log.info(f"Error on stream and trigger creation: {e}")
        pass
