# Copyright 2024 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Vigenair storage service.

This module provides methods for interacting with Google Cloud Storage.
"""

import datetime
from google.cloud import storage
from google.cloud.storage import blob
from typing import Optional, Union


def generate_gcs_signed_url(
    bucket_name: str,
    blob_name: str,
    expiration_minutes: int = 120,
    method: str = 'GET',
    content_type: Optional[str] = None,
) -> Optional[str]:
  """Generates a V4 signed URL for a GCS object.

  Args:
    bucket_name: The name of the GCS bucket.
    blob_name: The path/name of the object within the bucket (e.g., 'data/file.txt').
    expiration_minutes: The number of minutes the URL should be valid for.
      Defaults to 15 minutes.
    method: The HTTP method the signed URL allows ('GET', 'PUT', 'DELETE', etc.).
      Defaults to 'GET'.
    content_type: Required for 'PUT' requests to ensure the uploaded file has
      the correct MIME type. Defaults to None.

  Returns:
    The generated signed URL as a string, or None if the blob does not exist.
  """
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)

  # A Blob object is required to generate the signed URL
  blob_object: blob.Blob = bucket.blob(blob_name)

  if not blob_object.exists():
    # Note: For PUT requests (uploads), the blob might not exist yet,
    # but the API still expects to be able to check for its existence
    # if a public endpoint is being used for object creation, so this check
    # might need to be removed or adjusted depending on the specific use case
    # (e.g., if you are creating a signed URL for a file that doesn't exist yet).
    # For a 'GET' request, the file *must* exist.
    logging.warning(
        'SIGNED_URL - Could not find blob "%s" in bucket "%s".',
        blob_name,
        bucket_name,
    )
    return None

  try:
    # Set the URL expiration time
    expiration_time = datetime.timedelta(minutes=expiration_minutes)

    # Dictionary for optional headers, used primarily for 'PUT'
    # requests to enforce content-type
    headers = None
    if method == 'PUT' and content_type:
      headers = {'Content-Type': content_type}

    # Generate the signed URL using V4 signing
    signed_url = blob_object.generate_signed_url(
        version='v4',
        expiration=expiration_time,
        method=method,
        headers=headers,
    )

    logging.info(
        'SIGNED_URL - Generated %s signed URL for "%s" in bucket "%s" '
        'valid for %d minutes.',
        method,
        blob_name,
        bucket_name,
        expiration_minutes,
    )
    return signed_url
  except Exception as e:
    logging.error('SIGNED_URL - Failed to generate signed URL: %r', e)
    return None

