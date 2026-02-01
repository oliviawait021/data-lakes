import pandas as pd
import json
import boto3
from tqdm import tqdm
from io import BytesIO

# =================== CONFIGURATION ===================
# Figure out these first three from your familiarity with how the docker compose file works:
MINIO_ENDPOINT = ""
MINIO_ACCESS_KEY = ""
MINIO_SECRET_KEY = ""

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
    
    # **[Logic for extracting the platform and date goes here.]**

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
            obj = minio_client.get_object(Bucket=BUCKET_NAME, Key=file["Key"])
            file_contents = obj["Body"].readlines() # the file is actually stored in the "Body" of the MinIO object
            
            # Now you can use the extract_metadata function defined above to get the
            # platform and date
            platform, date = extract_metadata_from_csv(file_contents)
            if not platform or not date:
                print(f"⚠️ Skipping {file['Key']} due to missing metadata.")
                continue
            
            # Finally, you can us an in-memory buffer (BytesIO) to read the data into a pandas dataframe
            # (And yes, that's complex, so here's the code to do it)
            df = pd.read_csv(BytesIO(b"".join(file_contents)), comment="#", names=order_columns)
            
            # Okay now you get to be creative and do any cleaning/formatting you find to be necessary
            

            # **[Add any cleaning logic here, which you can do directly on the pandas df object]**
            

            # Lastly, you'll need to figure out the logic for creating the analytics structure
            # that is defined in the hierarchy example provided in the instructions. We'll start 
            # with an analytics prefix, but then you'll need to programatically construct the 
            # full length key that produces the desired hierarchy.
            analytics_prefix = "analytics/orders"
            
            # **[Add key construction logic here (ultimately modifying the analytics_key assignment operation)]**
            
            analytics_key = f"{analytics_prefix}"           

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
            obj = minio_client.get_object(Bucket=BUCKET_NAME, Key=file["Key"])
            json_content = json.loads(obj["Body"].read().decode("utf-8"))
            
            # The json_content object is just a dict, so now you can figure out how
            # to extract the metadata that we need from the json structure. 
            
            
            # **[Add logic to extract metadata from the json_content object here]**
            df = pd.DataFrame()
            
            # Now convert the json data to a dataframe that will make it easy to 
            # convert to a parquet file later. You can also apply any necessary
            # formatting or cleaning to the columns of the dataframe here.
            
            # **[Add logic for converting json to a dataframe, as well as any cleaning/formatting.]**
            
            # Again, you'll need to figure out the logic for creating the analytics structure 
            # In the data lake. (This will likely be very similar or identical to what you came
            # up with for the orders CSVs above.)
            analytics_prefix = "analytics/clickstream"
            
            # **[Add key construction logic here (ultimately modifying the analytics_key assignment operation)]**

            analytics_key = f"{analytics_prefix}"           

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


