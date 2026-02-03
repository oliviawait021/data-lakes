import pandas as pd
import json
import boto3
from tqdm import tqdm
from io import BytesIO

# =================== CONFIGURATION ===================
# Figure out these first three from your familiarity with how the docker compose file works:
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"

# You don't need to change these:
BUCKET_NAME = "data-lake"
minio_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)


# =================== FUNCTION: Extract Metadata from CSV ===================
# This function will be used to extract two pieces of metadata from a given 
# order CSV. It will need to accept the "file_contents" (which logic I provide
# in the process_orders() function below), read through the first few lines of
# the contents, and find and extract the "platform" and "date" information.
def extract_metadata_from_csv(file_contents):
    platform, date = None, None

    for line in file_contents:
        decoded = line.decode("utf-8").strip() if isinstance(line, bytes) else line.strip()
        if not decoded.startswith("#"):
            break  # No more metadata lines
        decoded_lower = decoded[1:].strip().lower()
        if decoded_lower.startswith("platform"):
            if ":" in decoded_lower:
                platform = decoded_lower.split(":", 1)[1].strip()
            elif "=" in decoded_lower:
                platform = decoded_lower.split("=", 1)[1].strip()
        elif decoded_lower.startswith("date"):
            if ":" in decoded_lower:
                date = decoded_lower.split(":", 1)[1].strip()
            elif "=" in decoded_lower:
                date = decoded_lower.split("=", 1)[1].strip()

    return platform, date

  
# =================== FUNCTION: Process Orders (CSV → Parquet) ===================
# This function iterates through the raw orders data in the data lake, and processes
# each one, converting the files from csv to parquet, storing the result in an 
# analytics-friendly way in the analytics layer in the data lake. 
def process_orders():
    # The parquet files will need to have properly defined columns, which aren't 
    # actually in the csv files. So I'll give them to you here:
    order_columns = [
        "order_id", "user_id", "order_time", "total_amount",
        "product_id", "product_category", "quantity",
        "discount_applied", "payment_type", "shipping_method"
    ]
    
    # This will list all files in the data lake whose location keys start with the
    # provided prefix:
    prefix = "raw/orders/"
    raw_files = minio_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix).get("Contents", [])
    
    # The total_processed counter and `with tqdm()` are just for the progress bar
    total_processed = 0 
    with tqdm(total=len(raw_files), desc="Processing Orders", unit="file", ascii="░▒█", ncols=100) as pbar:

        # Now you can look through all of the raw files and do some processing. I'll 
        # give you the logic for reading the files because it's a bit tricky:
        for file in raw_files:
            if file["Key"].endswith("/"):
                pbar.update(1)
                continue  # Skip directory markers

            obj = minio_client.get_object(Bucket=BUCKET_NAME, Key=file["Key"])
            file_contents = obj["Body"].readlines()  # the file is actually stored in the "Body" of the MinIO object

            # Now you can use the extract_metadata function defined above to get the
            # platform and date
            platform, date = extract_metadata_from_csv(file_contents)

            # Fallback: extract date from file key if not in metadata (e.g. raw/orders/2025/01/01/orders_2025-01-01_xxx.csv)
            if not date:
                key_parts = file["Key"].split("/")
                filename = key_parts[-1] if key_parts else ""
                if filename.startswith("orders_") and "_" in filename:
                    date = filename.split("_")[1]  # YYYY-MM-DD

            if not platform or not date:
                print(f"⚠️ Skipping {file['Key']} due to missing metadata.")
                pbar.update(1)
                continue

            # Finally, you can us an in-memory buffer (BytesIO) to read the data into a pandas dataframe
            # (And yes, that's complex, so here's the code to do it)
            df = pd.read_csv(BytesIO(b"".join(file_contents)), comment="#", names=order_columns)

            # Format order_time as datetime for Trino compatibility
            df["order_time"] = pd.to_datetime(df["order_time"])

            year, month, day = date.split("-") if len(date) >= 10 else (None, None, None)
            if not all([year, month, day]):
                pbar.update(1)
                continue

            # Analytics path: analytics/orders/platform={platform}/year={year}/month={month}/day={day}/orders_{date}_{hash}.parquet
            analytics_prefix = "analytics/orders"
            filename_base = file["Key"].split("/")[-1].replace(".csv", "")
            analytics_key = f"{analytics_prefix}/platform={platform}/year={year}/month={month}/day={day}/{filename_base}.parquet"

            # Lastly, I'll give you the code that actually inserts the parquet-formatted data in
            # the data lake:
            minio_client.put_object(Bucket=BUCKET_NAME, Key=analytics_key, Body=df.to_parquet(index=False))
            
            # You can also just leave these two progress bar updaters alone:
            total_processed += 1
            pbar.update(1)

    print(f"\n✅ {total_processed} order files processed and stored in '{analytics_prefix}'.\n")

  
  

# =================== FUNCTION: Process Clickstream Data (JSON → Parquet) ===================
# This function iterates through the raw clickstream data in the data lake, and processes
# each one, converting the files from json to parquet, storing the result in an 
# analytics-friendly way in the analytics layer in the data lake. 
def process_clickstream():

    # We'll use a different prefix this time, and the code for reading the json files from 
    # MinIO is also slightly different, mostly because there are so many files that we have
    # to get the full list in chunks:
    prefix = "raw/clickstream/"
    raw_files = []
    continuation_token = None

    while True:
        list_params = {"Bucket": BUCKET_NAME, "Prefix": prefix}
        if continuation_token:
            list_params["ContinuationToken"] = continuation_token

        response = minio_client.list_objects_v2(**list_params)
        raw_files.extend(response.get("Contents", []))

        # Check if there are more files to fetch
        if response.get("IsTruncated"):  # If results are truncated, continue
            continuation_token = response["NextContinuationToken"]
        else:
            break  # No more results
    
    # The total_processed counter and with functionality are just for the progress bar
    total_processed = 0
    with tqdm(total=len(raw_files), desc="Processing Clickstream", unit="file", ascii="░▒█", ncols=100) as pbar:
        
        # Now you can look through all of the raw files and do some processing. I'll 
        # give you the logic for reading the files because it's a bit tricky:
        for file in raw_files:
            if file["Key"].endswith("/"):
                pbar.update(1)
                continue  # Skip directory markers
            if not file["Key"].endswith(".json"):
                pbar.update(1)
                continue  # Only process JSON files

            obj = minio_client.get_object(Bucket=BUCKET_NAME, Key=file["Key"])
            json_content = json.loads(obj["Body"].read().decode("utf-8"))

            # Extract metadata from file_metadata
            file_metadata = json_content.get("file_metadata", {})
            platform = file_metadata.get("platform")
            date_str = file_metadata.get("date")
            export_id = file_metadata.get("export_id")

            if not platform or not date_str or not export_id:
                print(f"⚠️ Skipping {file['Key']} due to missing metadata.")
                pbar.update(1)
                continue

            year, month, day = date_str.split("-") if len(date_str) >= 10 else (None, None, None)
            if not all([year, month, day]):
                pbar.update(1)
                continue

            # Convert data array to DataFrame
            data = json_content.get("data", [])
            if not data:
                pbar.update(1)
                continue

            df = pd.DataFrame(data)

            # Ensure all expected columns exist (search_query may be missing in some events)
            for col in ["user_id", "event_time", "event_type", "page", "search_query", "product_id"]:
                if col not in df.columns:
                    df[col] = None

            # Format event_time as datetime for Trino compatibility
            df["event_time"] = pd.to_datetime(df["event_time"])

            # Keep only columns that belong in the parquet (no partition columns)
            df = df[["user_id", "event_time", "event_type", "page", "search_query", "product_id"]]

            # Analytics path: analytics/clickstream/platform={platform}/year={year}/month={month}/day={day}/{export_id}.parquet
            analytics_prefix = "analytics/clickstream"
            analytics_key = f"{analytics_prefix}/platform={platform}/year={year}/month={month}/day={day}/{export_id}.parquet"           

            # Again, I'll give you the code that actually inserts the parquet-formatted data in
            # the data lake:
            minio_client.put_object(Bucket=BUCKET_NAME, Key=analytics_key, Body=df.to_parquet(index=False))
            
            # You can also just leave these two progress bar updaters alone:
            total_processed += 1
            pbar.update(1)

    print(f"\n✅ {total_processed} clickstream files processed and stored in '{analytics_prefix}'.\n")

# =================== EXECUTE SCRIPT ===================
# This function is the one that guides the flow of the script when it's run. 
# Its functionality should be obvious. :) 
if __name__ == "__main__":
    print("Starting data transformation...")
    process_orders()
    process_clickstream()
    print("🎉 Transformation complete!")


