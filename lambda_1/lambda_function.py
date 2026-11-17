"""Lambda 1 - pdf-splitter.

Trigger : S3 ObjectCreated on  uploads/*.pdf
Does    : count pages -> create file_id -> save each page to
          pages/{file_id}/page_N.pdf -> write file record to MongoDB ->
          invoke Lambda 2 asynchronously once per page.

Environment variables:
  MONGO_URI        MongoDB Atlas connection string (required)
  MONGO_DB         database name (default: multilambda)
  PAGE_LAMBDA_NAME name of Lambda 2 (default: Bodhi_lambda_2_multilambda)
"""
import io
import json
import os
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
from pymongo import MongoClient
from pypdf import PdfReader, PdfWriter

s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")

PAGE_LAMBDA_NAME = os.environ.get("PAGE_LAMBDA_NAME", "Bodhi_lambda_2_multilambda")
MONGO_DB = os.environ.get("MONGO_DB", "multilambda")

_mongo = None


def get_db():
    """Reuse the Mongo connection across warm invocations."""
    global _mongo
    if _mongo is None:
        _mongo = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=8000)
    return _mongo[MONGO_DB]


def process_pdf(bucket, key):
    file_name = key.split("/")[-1]
    file_id = uuid.uuid4().hex

    pdf_bytes = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    reader = PdfReader(io.BytesIO(pdf_bytes))
    total_pages = len(reader.pages)

    # 1. file record first, so Lambda 2 can always find it
    get_db().files.insert_one(
        {
            "file_id": file_id,
            "file_name": file_name,
            "source_key": key,
            "total_pages": total_pages,
            "processed_pages": 0,
            "status": "processing",
            "created_at": datetime.now(timezone.utc),
        }
    )

    # 2. split + save every page
    page_keys = []
    for i, page in enumerate(reader.pages, start=1):
        writer = PdfWriter()
        writer.add_page(page)
        buf = io.BytesIO()
        writer.write(buf)
        page_key = f"pages/{file_id}/page_{i}.pdf"
        s3.put_object(
            Bucket=bucket, Key=page_key, Body=buf.getvalue(), ContentType="application/pdf"
        )
        page_keys.append(page_key)

    # 3. fan out: one async invocation per page
    for i, page_key in enumerate(page_keys, start=1):
        lambda_client.invoke(
            FunctionName=PAGE_LAMBDA_NAME,
            InvocationType="Event",
            Payload=json.dumps(
                {
                    "file_id": file_id,
                    "page_number": i,
                    "bucket": bucket,
                    "key": page_key,
                }
            ).encode(),
        )

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
            # mark failure if we got far enough to create a record
            print(f"failed processing {key}: {exc}")
            raise
    return {"processed": results}
