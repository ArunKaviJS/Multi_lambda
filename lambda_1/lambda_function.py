"""Lambda 1 - pdf-splitter.

Trigger : S3 ObjectCreated on  uploads/*.pdf
Does    : count pages -> deterministic file_id -> for every batch of 10 pages:
          split them, save to pages/{file_id}/page_N.pdf (in parallel), then
          invoke Lambda 2 ONCE for that batch (async).
          Also writes the file record to MongoDB.

Safe to retry: file_id is derived from bucket/key/etag, page keys are
overwritten, and Lambda 2 upserts pages, so a retry never duplicates data.

Environment variables:
  MONGO_URI        MongoDB Atlas connection string (required)
  MONGO_DB         database name (default: multilambda)
  PAGE_LAMBDA_NAME name of Lambda 2 (default: Bodhi_lambda_2_multilambda)
  BATCH_SIZE       pages per batch / per Lambda 2 invocation (default: 10)
"""
import hashlib
import io
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
from botocore.config import Config
from pymongo import MongoClient
from pypdf import PdfReader, PdfWriter

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
PAGE_LAMBDA_NAME = os.environ.get("PAGE_LAMBDA_NAME", "Bodhi_lambda_2_multilambda")
MONGO_DB = os.environ.get("MONGO_DB", "multilambda")

_cfg = Config(max_pool_connections=BATCH_SIZE * 2, retries={"max_attempts": 5})
s3 = boto3.client("s3", config=_cfg)
lambda_client = boto3.client("lambda", config=_cfg)

_mongo = None


def get_db():
    """Reuse the Mongo connection across warm invocations."""
    global _mongo
    if _mongo is None:
        _mongo = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=8000)
    return _mongo[MONGO_DB]


def store_page(bucket, page_key, page_bytes):
    """Runs in a worker thread: save one page PDF to S3."""
    s3.put_object(
        Bucket=bucket, Key=page_key, Body=page_bytes, ContentType="application/pdf"
    )


def invoke_batch(bucket, file_id, pages):
    """One async Lambda 2 invocation for a whole batch of pages."""
    lambda_client.invoke(
        FunctionName=PAGE_LAMBDA_NAME,
        InvocationType="Event",
        Payload=json.dumps(
            {"file_id": file_id, "bucket": bucket, "pages": pages}
        ).encode(),
    )


def process_pdf(bucket, key):
    file_name = key.split("/")[-1]

    obj = s3.get_object(Bucket=bucket, Key=key)
    etag = obj["ETag"].strip('"')
    pdf_bytes = obj["Body"].read()

    # Same upload -> same file_id, so retries reuse the same record.
    file_id = hashlib.sha256(f"{bucket}/{key}/{etag}".encode()).hexdigest()[:32]

    db = get_db()
    existing = db.files.find_one({"file_id": file_id})
    if existing and existing.get("status") in ("completed", "completed_with_errors"):
        print(f"{file_name}: already finished, skipping")
        return {"file_id": file_id, "skipped": True}

    reader = PdfReader(io.BytesIO(pdf_bytes))
    total_pages = len(reader.pages)
    now = datetime.now(timezone.utc)

    # Create the record once; never reset counters on a retry.
    db.files.update_one(
        {"file_id": file_id},
        {
            "$setOnInsert": {
                "file_id": file_id,
                "file_name": file_name,
                "source_key": key,
                "total_pages": total_pages,
                "processed_pages": 0,
                "failed_count": 0,
                "failed_pages": [],
                "missed_pages": 0,
                "status": "processing",
                "created_at": now,
                "updated_at": now,
            }
        },
        upsert=True,
    )

    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as pool:
        for start in range(0, total_pages, BATCH_SIZE):
            futures = []
            batch = []
            for idx in range(start, min(start + BATCH_SIZE, total_pages)):
                page_number = idx + 1
                page_key = f"pages/{file_id}/page_{page_number}.pdf"
                writer = PdfWriter()
                writer.add_page(reader.pages[idx])
                buf = io.BytesIO()
                writer.write(buf)
                futures.append(
                    pool.submit(store_page, bucket, page_key, buf.getvalue())
                )
                batch.append({"page_number": page_number, "key": page_key})
            for f in futures:
                f.result()  # all pages of the batch are in S3 before we invoke
            invoke_batch(bucket, file_id, batch)

    print(f"{file_name}: file_id={file_id} pages={total_pages}")
    return {"file_id": file_id, "total_pages": total_pages}


def lambda_handler(event, context):
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        # guard against loops / wrong files
        if not key.startswith("uploads/") or not key.lower().endswith(".pdf"):
            print(f"skipping {key}")
            continue
        try:
            results.append(process_pdf(bucket, key))
        except Exception as exc:
            print(f"failed processing {key}: {exc}")
            raise
    return {"processed": results}
