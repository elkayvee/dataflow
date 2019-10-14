# Python implementation of a dataflow job:

This exercise is a python implementation of a dataflow job on the google cloud which ingests raw CSV file ([AB_NYC_2019.csv](https://www.kaggle.com/dgomonov/new-york-city-airbnb-open-data)), processes it, converts the records into a dictionary format aligned with the schema of BigQuery table (_[w266-final-199221:springml.airbnb]()_) and writes to it. After the ingestion of the csv file, a second workflow in parallel processes the lines to collect neighbourhood listings (highlighted by the unique _id_ of each record), applies the **GroupByKey()** transform to collect the listings by neighbourhoods, combines the frequency or count of each listing per neighbourhood, formats it according to a simplified schema and writes the summary data into another BigQuery table (_[w266-final-199221:springml.airbnbsummary]()_).

Broadly it uses three google cloud components (as shown in the schematic high-level flow diagram):
1. Cloud Storage 
2. Dataflow
3. BigQuery

Before embarking on the dataflow job, a pre-requisite is to enable the relevant APIs to be able to communicate with the disparate components. 

The datafile from the link is unzipped and stored on the BUCKET (created expressly for this exercise - _[springml-airbnb]()_) as raw data file. 

## EDA & Data Preparation:

A cursory EDA of the raw datafile reveals there are **48,895** records and **16** columns with one or more having missing values in several records. The header line of the raw data file is represented in the BigQuery schema below:

![BigQuery schema of the original data](https://github.com/elkayvee/dataflow/blob/master/images/airbnb%20schema.png)

It also manifests a few problematic data-wrangling challenges:
a) The second column (_name_) is essentially multi-line string with one or more newline (_'\n'_) and CR (_'\r'_) characters and potentially with multiple comma puncutations. A typical csv used a double-quote enclosure to isolate a text field but the _apache_beam.io.ReadFromText_ breaks it into multi-records causing the data to be messed up in the dataflow ingestion step. This must be addressed beofre the historical data is ingested through the pipeline
b) The presence of double-quotes (viz., "Central Park") within the double-quotes that the csv file uses as an enclosure of the multiple commas within the string 
c) The program that generated this raw data file most likely used non-unicode (most likely _'utf-8'_) encoding but the current dataflow uses python 2.7. This gives rise to codec encoding error when processing non-ASCII characters.

The script [dataflow_file.py]() addresses these precise challenges by reading pandas read_csv function, deleting the potential one or more _'\n'_ and _'\r'_ characters within the dataframe column _name_, replacing the missing numerical values (_NaN_) with _0_ and filling the missing strings fields with blanks. It then uses unix utility _sed_ to delete non-ASCII characters from the text _in-place_. As a matter of abundant pre-caution, it also changes the delimiter to _'\t'_ when writing the file lest the presence of one or more commas mess up the text within the 'name' field. It then saves the new file using 'to_csv' utility with ".txt" extension and stores it on the bucket as a new file. It is consistent with the best practice to have the original data be available. Hence the choice to write it as a separate file. Now the data is ready to be ingested.

Dataflow job

Output

  
