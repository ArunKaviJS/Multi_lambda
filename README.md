# Multi-Lambda PDF pipeline

```
User uploads PDF -> S3 uploads/ -> Lambda 1 (split) -> N x Lambda 2 (extract text) -> MongoDB Atlas
```

| Folder | Lambda | Deploy workflow |
|---|---|---|
| `lambda_1/` | `Bodhi_lambda_1_multilambda` (splitter) | `.github/workflows/deploy-lambda-1.yml` |
| `lambda_2/` | `Bodhi_lambda_2_multilambda` (page processor) | `.github/workflows/deploy-lambda-2.yml` |

Each workflow has a `paths:` filter, so pushing to `main` deploys **only the Lambda whose folder changed**.
Zip goes to `s3://bodhi-multi-lambda/lambda_N/lambda.zip`, then `update-function-code` is called.

## How to upload a file

Upload any PDF under the `uploads/` prefix of the bucket:

```bash
aws s3 cp my.pdf s3://bodhi-multi-lambda/uploads/my.pdf --region ap-south-1
```

(or S3 console -> bucket `bodhi-multi-lambda` -> `uploads/` -> Upload). The S3 event starts Lambda 1.

## One-time setup

### 1. GitHub repo secrets (Settings -> Secrets and variables -> Actions)
- `AWS_ACCESS_KEY`
- `AWS_SECRET_KEY`

### 2. Lambda configuration (both functions already exist)
| Setting | Lambda 1 | Lambda 2 |
|---|---|---|
| Runtime | Python 3.12 | Python 3.12 |
| Handler | `lambda_function.lambda_handler` | `lambda_function.lambda_handler` |
| Timeout | 300 s | 60 s |
| Memory | 1024 MB | 512 MB |
| Env vars | `MONGO_URI`, optional `MONGO_DB`, `PAGE_LAMBDA_NAME` | `MONGO_URI`, optional `MONGO_DB` |

Do NOT put the Lambda env vars in the repo; set them in the Lambda console.

### 3. IAM permissions

**a) The GitHub deploy user** (the owner of `AWS_ACCESS_KEY`/`AWS_SECRET_KEY`):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": ["arn:aws:s3:::bodhi-multi-lambda/lambda_1/*", "arn:aws:s3:::bodhi-multi-lambda/lambda_2/*"] },
    { "Effect": "Allow",
      "Action": ["lambda:UpdateFunctionCode", "lambda:GetFunction", "lambda:GetFunctionConfiguration"],
      "Resource": [
        "arn:aws:lambda:ap-south-1:<ACCOUNT_ID>:function:Bodhi_lambda_1_multilambda",
        "arn:aws:lambda:ap-south-1:<ACCOUNT_ID>:function:Bodhi_lambda_2_multilambda" ] }
  ]
}
```

**b) Lambda 1 execution role**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::bodhi-multi-lambda/uploads/*" },
    { "Effect": "Allow", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::bodhi-multi-lambda/pages/*" },
    { "Effect": "Allow", "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:ap-south-1:<ACCOUNT_ID>:function:Bodhi_lambda_2_multilambda" }
  ]
}
```
plus the managed policy `AWSLambdaBasicExecutionRole` (CloudWatch Logs).

**c) Lambda 2 execution role**: `s3:GetObject` on `arn:aws:s3:::bodhi-multi-lambda/pages/*` + `AWSLambdaBasicExecutionRole`.

**d) Let S3 call Lambda 1** (resource policy on the function):
```bash
aws lambda add-permission --function-name Bodhi_lambda_1_multilambda \
  --statement-id s3-invoke --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::bodhi-multi-lambda \
  --source-account <ACCOUNT_ID> --region ap-south-1
```

### 4. S3 event notification
Bucket `bodhi-multi-lambda` -> Properties -> Event notifications -> create:
- Event type: `s3:ObjectCreated:*`
- Prefix: `uploads/`, Suffix: `.pdf`
- Destination: Lambda `Bodhi_lambda_1_multilambda`

Prefix filtering matters: the pipeline writes to `pages/` and CI writes to `lambda_N/` in the same bucket, and neither must re-trigger Lambda 1.

### 5. MongoDB Atlas
Lambdas run outside a VPC, so in Atlas -> Network Access allow `0.0.0.0/0` (protect with a strong DB user), or put the Lambdas in a VPC with a NAT gateway and allow its IP.

## MongoDB data
- `files`: `{ file_id, file_name, source_key, total_pages, processed_pages, status }` (`processing` -> `completed`)
- `pages`: `{ file_id, page_number, text }`

Lambda 2 is retry-safe: a unique index on `(file_id, page_number)` and an upsert mean an async retry never double-counts `processed_pages`.
