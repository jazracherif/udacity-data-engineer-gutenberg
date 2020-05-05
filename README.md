[components]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/components.png
[dag]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/dag.png
[crawler]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/glue-crawler.png
[athena]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/athena-tables.png
[athena-query]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/athena-query.png

# Udacity Data Engineer Capstone: Gutenber books ETL

Developed originally to evaluate the reading difficulty of technical material for the Navy, the [Flesch–Kincaid readability tests](https://en.wikipedia.org/wiki/Flesch–Kincaid_readability_tests) have since been used widely on all sorts of text, including the reading levels of books. One of their main use in education circles is to enable teachers to find and recommend books based on their difficulty, as measured by these scores.

In this project, I create an ETL pipeline that compute such socres on all english books in the [Gutenberg](www.gutenberg.org) archive. The ETL is orchestrated via Airflow and invokes AWS EMR to run the calculations in a distributed fashion. With this approach, over 22k books (\~8GB) have been ingested.

For every text, the number of sentences, words, and syllables are counted up in order to compute the two final scores:
- [Flesch–Kincaid grade level](https://en.wikipedia.org/wiki/Flesch–Kincaid_readability_tests#Flesch–Kincaid_grade_level) 
- Flesch reading ease(https://en.wikipedia.org/wiki/Flesch–Kincaid_readability_tests#Flesch_reading_ease)

The ETL architecture as implemented in the project consists of extract raw data from the gutenberg site itself, processing it into basic catalog information and textual file and uploading them into S3, then creating a Spark EMR cluster and running a Spark Application to compute the scores, which are then written back into S3 in parquet format, and ingested by Glue/Athena for analysis.

![Architectural Components][components]


## The data model

The raw dataset consist :
1. RDF files containing the full description of the [Gutenberg catalog](http://www.gutenberg.org/wiki/Gutenberg:Feeds) in the RDF format
2. text files hosted on the gutenberg website 

Out of these files, we generate the follow tables in our database:

1 dimension table, the catalog table:
- book_title
- author
- language
- book_id
- location

1 fact table, the reading_difficulty table:
- id
- sentence_count
- word_count
- syllables_count
- grade_level
- reading_ease


There is a number of challenges identified with the Gutenberg source of data
- Duplication of titles due to the use of new formats. This can be seen either through the `/old` folder or through copy of files with the `-8.txt` previx
- The Catalog data is in RDF format and the extraction of data must follow the protocl. This means that not all fields are present, and we are limited in terms of meta data
- The mapping between the Catalog data and the text files itself is implict. The rdf filename consist sof the id which is the name of the text field, and can be found under a nested directory.
- The gutenberg data itself contains more than literary books but it is not currently possible to download only the books title
- A single book can have multiple volume and these are separate files and treated as separate books
- The content of these text files includes Gutenberg specic language and the beginning and the end which interferes with the scores calculation. Detecting such boundary is not fullproof


### Number of syllables
Use the Liang algorithm as defined in http://www.tug.org/docs/liang/liang-thesis.pdf
and implemented in [hypenate](https://github.com/jfinkels/hyphenate)


## Installation

### Prerequisits

the AWS credential with read/write control for S3 and EMR must be avilable and store in the `cfg/aws.cfg` file

## Setup

To install the python virtual environment:

`make install`


## Local Spark Development

a local spark installation must be available to run the test.

make sure to be in the venv:

`source venv/bin/activate`

First generate the catalog data:

`python lib/gutenberg.py --cmd download_catalog`

and download the data:

`python lib/gutenberg.py --cmd download_data`

The above will take a few minutes to complete, and store the data in the `data/` folder

Generate the basic data for the catalog:

`python lib/gutenberg.py --cmd generate_catalog`

This will crawl through the directory containing all the text files, extract the id from each filename, load the RDF object and extract information, and output a csv file that links each book id with meta data and a url link. This file will then be used by the spark application to locate which text files to extract.

Finally, to run spark in local mode:

`make spark`

This will invoke `lib/spark-etl.py` which contains the code to generate the final dataset.


## Airflow Pipeline

In order to run the full pipeline with Airflow, you will need to launch the airflow:

`airflow-start.sh`

then navigate to the DAG `gutenberg-etl` implemented in `airflow/dags/gutenberg-etl.py`

The Airflow DAG that performs the ETL is shown below

![Airflow DAG][dag]


This DAG is scheduled to run weekly and will execute the following steps:
1) Download latest catalog data from gutenberg and new text files
2) Generate the catalog.csv file and upload it to S3, together with the relevant text files
3) Create an EMR cluster and run the ETL pipeline as defined in lib/spark-etl.py

## Results

The Spark ETL pipline will generate the tables in the form of Parquet files stored in the S3 bucket {bucket_name}/gutenberg-data/results

A Glue crawler can then be programmed to extract the table and show them in AWS Athena

![Glue Crawler][crawler]

![Athena tables][athena]

The final resutls can be visualized by a query:

![athena-query][athena-query]

