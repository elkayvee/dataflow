import os
import pandas as pd
import numpy as np

from google.cloud import storage

# Raw data file for the airbnb data
raw_filename = 'AB_NYC_2019.csv'

# File scrubbed of the data wrangling challenges:
clean_filename = 'AB_NYC_2019.txt'

# gcs bucket name

BUCKET = 'springml-airbnb'

"""Downloads a blob from the bucket."""

storage_client = storage.Client()

bucket = storage_client.get_bucket(BUCKET)
blob = bucket.blob(raw_filename)
blob.download_to_filename(raw_filename)

df = pd.read_csv(raw_filename)

# Replace 'NaN' occurring in the numerical columns with '0' and blanks from string columns
c = df.select_dtypes(np.number).columns
df[c] = df[c].fillna('')
df = df.fillna('0')

# Remove the newline character ('\n') and Carriage Return {'\r') from the 'name' field

df.name = df.name.str.replace('\r','')
df.name = df.name.str.replace('\n','')
df.name = df.name.str.replace('"', '')

# Write out the scrubbed data to the 'clean_filename':
df.to_csv(clean_filename, sep='\t', encoding='utf-8', index=False)

# The clean data file still contains non-ASCII characters that cause codec error since it is python 2.7
os.system("LANG=C sed -i -E 's/[\d128-\d255]//g' AB_NYC_2019.txt")

"""Upload clean file to the bucket."""

blob_ = bucket.blob(clean_filename)
blob_.upload_from_filename(clean_filename)

