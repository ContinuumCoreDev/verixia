import time
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct

local = QdrantClient(host="127.0.0.1", port=6335)
CLOUD_URL = "d98b1c4b-cb98-4006-90aa-064f43a6c2dc.us-east-1-1.aws.cloud.qdrant.io"
CLOUD_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.9skpxOtq3x8-VwDTEZgooecvuwwY9q5qQHILSHCFMnM"
cloud = QdrantClient(url="https://" + CLOUD_URL + ":6333", api_key=CLOUD_KEY, timeout=60)

local_count = local.get_collection("verixia_legal").points_count
cloud_count = cloud.get_collection("verixia_legal").points_count
print(f"Local:  {local_count}")
print(f"Cloud:  {cloud_count}")
print(f"Delta:  {local_count - cloud_count}")

offset = None
total  = 0
batch  = 0
import time as t
start  = t.time()

while True:
    result = local.scroll(
        collection_name="verixia_legal",
        limit=25,
        offset=offset,
        with_vectors=True,
        with_payload=True,
    )
    points, next_offset = result
    if not points:
        break
    for attempt in range(3):
        try:
            cloud.upsert(
                collection_name="verixia_legal",
                points=[PointStruct(id=p.id, vector=p.vector, payload=p.payload) for p in points],
                wait=True,
            )
            break
        except Exception as e:
            print(f"Retry {attempt+1}: {e}")
            t.sleep(5)
    total += len(points)
    batch += 1
    if batch % 50 == 0:
        elapsed = (t.time() - start) / 60
        print(f"  {total}/{local_count} — {elapsed:.1f}m elapsed")
    t.sleep(0.8)
    offset = next_offset
    if next_offset is None:
        break

final = cloud.get_collection("verixia_legal").points_count
print(f"Done. Cloud: {final} points.")
