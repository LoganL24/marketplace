import json
import importlib
import math
import time
from dataclasses import dataclass

import grpc
from proto.src.marketplace_pb2 import Item
from utils.config import NODE_PORT, DOCKER_IMAGE, DOCKER_NETWORK


@dataclass
class SearchHit:
    item: Item
    score: float


def item_to_vector(item: Item) -> list[float]:
    """Build a small numeric vector from item fields for similarity ops."""
    text = " ".join([item.title, item.category, item.description]).lower()
    token_count = float(len(text.split()))
    text_len = float(len(text))
    return [
        token_count,
        text_len,
        float(item.starting_price),
        float(item.current_price),
        float(item.quantity),
        float(item.version),
    ]


def cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    vec_a = [float(x) for x in a]
    vec_b = [float(x) for x in b]
    dot = sum(x * y for x, y in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(x * x for x in vec_a))
    norm_b = math.sqrt(sum(y * y for y in vec_b))
    denom = norm_a * norm_b
    if denom == 0:
        return 0.0
    return float(dot / denom)


def cosine_distance(a: list[float], b: list[float]) -> float:
    return 1.0 - cosine_similarity(a, b)


def update_centroid(records: list[Item]) -> list[float]:
    if not records:
        return []
    vectors = [item_to_vector(record) for record in records]
    dim = len(vectors[0])
    return [sum(v[i] for v in vectors) / len(vectors) for i in range(dim)]


def local_top_k(records: list[Item], query_embedding: list[float], top_k: int) -> list[SearchHit]:
    hits: list[SearchHit] = [
        SearchHit(
            item=record,
            score=cosine_similarity(item_to_vector(record), query_embedding),
        )
        for record in records
    ]
    hits.sort(key=lambda hit: hit.score, reverse=True)
    return hits[:top_k]


def kmeans_split(
    records: list[Item], max_iters: int = 6
) -> tuple[list[Item], list[Item], list[float], list[float]]:
    if len(records) < 2:
        centroid = update_centroid(records)
        return records, [], centroid, []

    embeddings: list[list[float]] = [item_to_vector(record) for record in records]
    c1: list[float] = list(embeddings[0])
    c2: list[float] = list(embeddings[-1])

    cluster1: list[Item] = []
    cluster2: list[Item] = []

    for _ in range(max_iters):
        cluster1 = []
        cluster2 = []

        for record, embedding in zip(records, embeddings):
            if cosine_similarity(embedding, c1) >= cosine_similarity(
                embedding, c2
            ):
                cluster1.append(record)
            else:
                cluster2.append(record)

        if not cluster1 or not cluster2:
            midpoint = len(records) // 2
            cluster1 = records[:midpoint]
            cluster2 = records[midpoint:]
            break

        c1 = update_centroid(cluster1)
        c2 = update_centroid(cluster2)

    centroid1 = update_centroid(cluster1)
    centroid2 = update_centroid(cluster2)
    return cluster1, cluster2, centroid1, centroid2


def corpus_line_to_record(jsonl_line: str) -> Item:
    obj: dict = json.loads(jsonl_line)
    return Item(
        item_id=obj["item_id"],
        seller_id=obj.get("seller_id", ""),
        title=obj.get("title", ""),
        category=obj.get("category", ""),
        description=obj.get("description", ""),
        starting_price=obj.get("starting_price", 0.0),
        current_price=obj.get("current_price", 0.0),
        quantity=obj.get("quantity", 0),
        status=obj.get("status", ""),
        version=obj.get("version", 0),
    )


def choose_closest_node(nodes, embedding: list[float]):
    if len(nodes) == 1:
        return nodes[0]

    with_centroids = [
        node for node in nodes if node["centroid"]
    ]
    if not with_centroids:
        return nodes[0]

    return max(
        with_centroids,
        key=lambda node: cosine_similarity(embedding, list(node["centroid"])),
    )


def wait_for_grpc_target(target: str, retry_seconds: float = 0.5) -> None:
    while True:
        try:
            with grpc.insecure_channel(target) as channel:
                grpc.channel_ready_future(channel).result(timeout=1)
            return
        except grpc.FutureTimeoutError:
            time.sleep(retry_seconds)
        except grpc.RpcError:
            time.sleep(retry_seconds)


def create_storage_node(node_num: int) -> str:
    docker = importlib.import_module("docker")
    docker_errors = importlib.import_module("docker.errors")
    client = docker.from_env()
    name: str = f"storage-node-{node_num}"
    target: str = f"{name}:{NODE_PORT}"

    try:
        client.containers.get(name).remove(force=True)
    except docker_errors.NotFound:
        pass

    client.containers.run(
        DOCKER_IMAGE,
        name=name,
        hostname=name,
        network=DOCKER_NETWORK,
        detach=True,
        working_dir="/app",
        command=["python", "-u", "storage_node/node.py"],
        environment={
            "GRPC_SERVER_PORT": str(NODE_PORT),
            "NODE_TARGET": target,
            "PYTHONPATH": "/app:/app/proto/src",
        },
    )
    wait_for_grpc_target(target)
    return target
