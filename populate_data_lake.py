import boto3
from datetime import datetime
from pathlib import Path
from tqdm import tqdm

# =================== CONFIGURATION ===================
# Figure out these first three from your familiarity with how the docker compose file works:
MINIO_ENDPOINT = ""
MINIO_ACCESS_KEY = ""
MINIO_SECRET_KEY = ""

# You don't need to change the next two:
RAW_INPUT_DIR = "raw-input"
BUCKET_NAME = "data-lake"

# Connect to MinIO using boto3
# Note that: (1) you don't need to change this s3_client setup, and 
#            (2) in case you care, this is also how you would send stuff to actual S3.
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Ensure bucket exists
try:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
except s3_client.exceptions.BucketAlreadyOwnedByYou:
    pass  # Bucket already exists

# =================== FUNCTION: Upload to MinIO ===================
# Takes a file_path of a raw file to be uploaded, then uploads the 
# file to the correct location in the hierarchy in the MinIO data lake.
def upload_to_minio(file_path):

    # Given the file_path, you'll need to figure out how to extract the
    # information you need in order to decide where the file should be
    # placed in the data lake. Ultimately, you're looking to decide how
    # to assign the right string value to the `s3_path` variable.
    # 
    # Hint 1: This will probably need to be conditional on whether the
    #         file is a csv or json file.
    # Hint 2: The logic for how to construct the path for csvs and json
    #         files can be derived from the hierachy/architecture image
    #         in the instructions.


    # **[Logic for constructing the insertion location key goes here.]**
    
    s3_path = "raw"

    # Here is the logic for actually uploading the file to the 
    # data lake. (No need to edit this code chunk.)
    try:
        s3_client.upload_file(str(file_path), BUCKET_NAME, s3_path)
        return True
    except Exception as e:
        print(f"✖ Failed to upload {file_path}: {e}")
        return False



# ================ FUNCTION: Process Files for Ingestion ================
# This function is the main driver of the data lake population process.
# It Iterates through the raw input files and calls the upload function to 
# get them uploaded to MinIO. I'll add comments to explain what it's doing,
# but note that you don't need to edit anything in this function.
def process_files():
    # This will return a list of files in the provided input directory
    files = list(Path(RAW_INPUT_DIR).glob("*"))  
    
    # The success_count counter and `with tqdm()` are just for the progress bar
    success_count = 0
    with tqdm(total=len(files), desc="Uploading Files", unit="file", ascii="░▒█", ncols=100) as pbar:
        # Now we just interate through the list of files and pass each to the 
        # upload process. That function returns true if succesful, in which case
        # the success counter and progress bar are updated.
        for file in files:
            if upload_to_minio(file):
                success_count += 1
            pbar.update(1)

    print(f"\n✅ Successfully uploaded {success_count} files from '{RAW_INPUT_DIR}' to 's3://{BUCKET_NAME}'.")

# =================== EXECUTE SCRIPT ===================
# This function is the one that guides the flow of the script when it's run. 
# Its functionality should be obvious. :) 
if __name__ == "__main__":
    process_files()
