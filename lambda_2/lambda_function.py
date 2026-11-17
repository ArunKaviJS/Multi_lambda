"""Lambda 2 - page-processor.

Trigger : async invoke from Lambda 1, payload
          {file_id, page_number, bucket, key}
Does    : read page PDF from S3 -> extract text -> save to MongoDB `pages`
          -> increment files.processed_pages (and mark completed at the end).

Environment variables:
  MONGO_URI  MongoDB Atlas connection string (required)
  MONGO_DB   database name (default: multilambda)
"""
import io
import os
from datetime import datetime, timezone

import boto3
from pymongo import ASCENDING, MongoClient, ReturnDocument
from pypdf import PdfReader

s3 = boto3.client("s3")
MONGO_DB = os.environ.get("MONGO_DB", "multilambda")

_mongo = None
_indexed = False


def get_db():
    global _mongo, _indexed
    if _mongo is None:
        _mongo = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=8000)
    db = _mongo[MONGO_DB]
    if not _indexed:
        db.pages.create_index(
            [("file_id", ASCENDING), ("page_number", ASCENDING)], unique=True
        )
        db.files.create_index("file_id", unique=True)
        _indexed = True
    return db


def lambda_handler(event, context):
    file_id = event["file_id"]
    page_number = int(event["page_number"])
    bucket = event["bucket"]
    key = event["key"]

    db = get_db()

    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    reader = PdfReader(io.BytesIO(body))
    text = "\n".join((p.extract_text() or "") for p in reader.pages)

    # Upsert so an async retry never creates a duplicate page.
    res = db.pages.update_one(
        {"file_id": file_id, "page_number": page_number},
        {
            "$set": {"text": text, "updated_at": datetime.now(timezone.utc)},
            "$setOnInsert": {"file_id": file_id, "page_number": page_number},
        },
        upsert=True,
    )

    # Only count the page the first time it is stored (retry-safe).
    if res.upserted_id is not None:
        doc = db.files.find_one_and_update(
            {"file_id": file_id},
            {"$inc": {"processed_pages": 1}},
            return_document=ReturnDocument.AFTER,
        )
        if doc and doc["processed_pages"] >= doc["total_pages"]:
            db.files.update_one(
                {"file_id": file_id},
                {
                    "$set": {
                        "status": "completed",
                        "completed_at": datetime.now(timezone.utc),
                    }
                },
            )

    print(f"file_id={file_id} page={page_number} chars={len(text)}")
    return {"file_id": file_id, "page_number": page_number, "chars": len(text)}
