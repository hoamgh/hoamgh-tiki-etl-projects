# Ecom-Data-Crawler

Pipeline toi gian:

1. Crawl du lieu tu Tiki
2. Upload snapshot len S3
3. Dong bo vao SQL Server (schema Products/Categories/Brands/Sellers)

## Cai dat

```bash
pip install -r requirements.txt
```

Tao file .env (khong commit) voi cac bien toi thieu:

- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_DEFAULT_REGION=ap-southeast-2
- S3_BUCKET_NAME=ten-bucket-cua-ban
- S3_RAW_PREFIX=raw

## Cach chay

### 1) Crawl du lieu

```bash
python main.py
```

Output:
- File CSV raw theo run id tren S3, vi du: `s3://<bucket>/raw/run_YYYYMMDD_HHMMSS.csv`

Them khoi `s3` trong `config.json` (hoac dung bien moi truong `.env`):

{
  "s3": {
    "bucket_name": "ten-bucket-cua-ban",
    "raw_prefix": "raw",
    "region": "ap-southeast-2"
  }
}

## Luong du lieu

`crawl -> in-memory snapshot -> s3 -> sql server snapshot`

## Ghi chu

- SQL Server schema muc tieu:
  - `dbo.Categories`
  - `dbo.Brands`
  - `dbo.Sellers`
  - `dbo.Products`