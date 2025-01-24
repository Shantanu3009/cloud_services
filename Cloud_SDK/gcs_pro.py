from datetime import timedelta
from google.cloud import storage
from google.auth import impersonated_credentials
import google

SERVICE_ACCOUNT = 'kaggle-service-account-1@my-kaggle-project-434518.iam.gserviceaccount.com'

def get_impersonated_credentials(target_service_account):
    source_credentials, _ = google.auth.default()

    target_credentials = impersonated_credentials.Credentials(
        source_credentials = source_credentials,
        target_principal = target_service_account,
        target_scopes = ['https://www.googleapis.com/auth/cloud-platform']
    )
    return target_credentials


def get_signed_url(bucket_name, blob_name, expiration_time):

    credentials =get_impersonated_credentials(SERVICE_ACCOUNT)
    client = storage.Client(credentials= credentials)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    url = blob.generate_signed_url(
        expiration = expiration_time,
        method = "GET"
    )
    return url


def generate_signed_url(bucket_name, blob_name, expiration_seconds):
    """Generate a signed URL for a blob."""
    client = storage.Client.from_service_account_json("/home/shantanusgh0/my-kaggle-project-434518-774acd4fae53.json")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    url = blob.generate_signed_url(
        expiration=timedelta(seconds=expiration_seconds),
        method="GET"
    )
    return url
# Generate signed URL valid for 1 hour (3600 seconds)
#signed_url = generate_signed_url("shantanu_kaggle_bucket", "Image_Folder/image_1.png", 3600)
#print(f"Generated signed URL: {signed_url}")

# ----------Set a lifecycle rule for a GCS bucket to delete files older than 30 days------

def set_lifecycle_rule(bucket_name):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    rules = bucket.lifecycle_rules

    print(f"Earlier Lifecycle management rules for bucket {bucket_name} are {list(rules)}")
    bucket.add_lifecycle_delete_rule(age=7)
    bucket.patch()

    rules = bucket.lifecycle_rules
    print(f"Now Lifecycle management is enable for bucket {bucket_name} and the rules are {list(rules)}")


# Replace with your bucket name
bucket_name = "shantanu_kaggle_bucket"
set_lifecycle_rule(bucket_name)

"""
---Transition to NEARLINE storage after 60 days.---
bucket.add_lifecycle_rule({
    "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
    "condition": {"age": 60}
})
---Delete noncurrent versions after 7 days.----
bucket.add_lifecycle_rule({
    "action": {"type": "Delete"},
    "condition": {"isLive": False, "age": 7}
})
---Delete objects after January 1, 2025.-----
bucket.add_lifecycle_rule({
    "action": {"type": "Delete"},
    "condition": {"createdBefore": "2025-01-01"}
})
---Keep only the most recent 5 versions of objects.
bucket.add_lifecycle_rule({
    "action": {"type": "Delete"},
    "condition": {"numNewerVersions": 5}
})
---Delete objects in the STANDARD storage class older than 90 days.
bucket.add_lifecycle_rule({
    "action": {"type": "Delete"},
    "condition": {"age": 90, "matchesStorageClass": ["STANDARD"]}

--Transition to COLDLINE storage for objects older than 365 days and larger than 1GB.
bucket.add_lifecycle_rule({
    "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
    "condition": {"age": 365, "matchesStorageClass": ["NEARLINE"], "sizeGreaterThan": 1 * 1024 * 1024 * 1024}
})



Condition	Description
age:	Number of days since object creation.
createdBefore:	Specific date before which objects must be created.
isLive:	True for current versions, False for noncurrent versions.
numNewerVersions:	Minimum number of newer versions to retain.
matchesStorageClass:	Apply rule only to objects in specified storage classes.
sizeGreaterThan:	Apply rule to objects larger than the specified size (in bytes).
sizeLessThan:	Apply rule to objects smaller than the specified size (in bytes).

for rule in bucket.lifecycle_rules:
    print(rule)

bucket.clear_lifecycles()
bucket.patch()
"""


