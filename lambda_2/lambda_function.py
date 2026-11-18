"""Lambda 2 - page-processor.

Trigger : async invoke from Lambda 1, one invocation per batch of pages:
          {file_id, bucket, pages: [{page_number, key}, ...]}
Does    : for each page -> read from S3 -> extract text -> save to MongoDB
          `pages` -> update counters on the `files` record.

A page that fails is NOT lost silently: it is recorded in
files.failed_pages (page_number + error) and failed_count. When every page is
accounted for, the file status becomes `completed` (no failures) or
`completed_with_errors`, and missed_pages holds how many pages are missing.

Environment variables:
  MONGO_URI  MongoDB Atlas connection string (required)
  MONGO_DB   database name (default: multilambda)
"""
import io
import os
from datetime import datetime, timezone

import boto3
from pymongo import ASCENDING, MongoClient
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


def now():
    return datetime.now(timezone.utc)


def extract_text(bucket, key):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    reader = PdfReader(io.BytesIO(body))
    return "\n".join((p.extract_text() or "") for p in reader.pages)


def record_success(db, file_id, page_number, text):
    # Upsert so a retry never creates a duplicate page.
    res = db.pages.update_one(
        {"file_id": file_id, "page_number": page_number},
        {
            "$set": {"text": text, "updated_at": now()},
            "$setOnInsert": {"file_id": file_id, "page_number": page_number},
        },
        upsert=True,
    )
    if res.upserted_id is not None:  # count a page only the first time
        db.files.update_one(
            {"file_id": file_id},
            {"$inc": {"processed_pages": 1}, "$set": {"updated_at": now()}},
        )
    # If an earlier attempt had marked this page failed, clear that.
    cleared = db.files.update_one(
        {"file_id": file_id, "failed_pages.page_number": page_number},
        {"$pull": {"failed_pages": {"page_number": page_number}}, "$inc": {"failed_count": -1}},
    )
    return cleared.modified_count


def record_failure(db, file_id, page_number, error):
    # Only record once per page (guard against duplicate retries).
    db.files.update_one(
        {"file_id": file_id, "failed_pages.page_number": {"$ne": page_number}},
        {
            "$push": {"failed_pages": {"page_number": page_number, "error": str(error)[:500]}},
            "$inc": {"failed_count": 1},
            "$set": {"updated_at": now()},
        },
    )


def finalize(db, file_id):
    """If every page is accounted for (done or failed), close the file."""
    doc = db.files.find_one({"file_id": file_id})
    if not doc:
        return
    done = doc["processed_pages"] + doc["failed_count"]
    if done >= doc["total_pages"]:
        missed = doc["total_pages"] - doc["processed_pages"]
        db.files.update_one(
            {"file_id": file_id},
            {
                "$set": {
                    "missed_pages": missed,
                    "status": "completed" if missed == 0 else "completed_with_errors",
                    "completed_at": now(),
                }
            },
        )


def lambda_handler(event, context):
    file_id = event["file_id"]
    bucket = event["bucket"]
    pages = event["pages"]

    db = get_db()
    ok, failed = [], []

    for page in pages:
        page_number = int(page["page_number"])
        try:
            text = extract_text(bucket, page["key"])
            record_success(db, file_id, page_number, text)
            ok.append(page_number)
        except Exception as exc:  # keep going; record the miss in the DB
            print(f"file_id={file_id} page={page_number} FAILED: {exc}")
            record_failure(db, file_id, page_number, exc)
            failed.append(page_number)

    finalize(db, file_id)
    print(f"file_id={file_id} ok={len(ok)} failed={failed}")
    return {"file_id": file_id, "ok": ok, "failed": failed}
