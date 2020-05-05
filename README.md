[components]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/components.png
[dag]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/dag.png
[crawler]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/glue-crawler.png
[athena]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/athena-tables.png
[athena-query]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/athena-query.png
[ganglia-cpu]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/ganglia-cpu.png
[ganglia-mem]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/ganglia-mem.png
[spark-sql-full]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/spark-sql-full.png
[gantt]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/gantt.png
[aws-emr-config]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/aws-emr-config.png
[aws-s3-buckets]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/aws-s3-buckets.png

# Udacity Data Engineer Capstone Project: ETL to compute readability scores for Gutenber books

Developed originally to evaluate the reading difficulty of technical material for the U.S. Navy, the [Flesch–Kincaid readability tests](https://en.wikipedia.org/wiki/Flesch–Kincaid_readability_tests) have since been used widely on all sorts of text, including the reading levels of books. One of their main use in education circles is to enable teachers to find and recommend books based on their reading difficulty, as measured by these scores.

In this project, I create an ETL pipeline that compute such scores on all english books in the [Gutenberg](www.gutenberg.org) archive. The ETL is orchestrated via [Airflow](https://airflow.apache.org) and invokes AWS EMR to run the calculations in a distributed fashion. 

For every text, the number of sentences, words, and syllables are counted up in order to compute the two final scores:
- [Flesch–Kincaid grade level](https://en.wikipedia.org/wiki/Flesch–Kincaid_readability_tests#Flesch–Kincaid_grade_level).
- [Flesch reading ease](https://en.wikipedia.org/wiki/Flesch–Kincaid_readability_tests#Flesch_reading_ease).

The ETL architecture as implemented in the project consists of extracting raw data from the Gutenberg website itself, processing it into catalog information and upload the catalog as well as text files to S3. A Spark EMR cluster is then created and a Spark Application ETL computes the scores, which are then written back into S3 in parquet format, and ingested by Glue/Athena for analysis.

![Architectural Components][components]

## The data model

The raw dataset consist of:
1. 60,000 RDF files containing the full description of the [Gutenberg catalog](http://www.gutenberg.org/wiki/Gutenberg:Feeds) in the RDF format.
2. 41GB on disk (345k items) of text files downloaded from the gutenberg website.

Out of these files, we generate the following tables in our database:

1 dimension table,
    
    catalog: data about the books to process
        book_title, author, language, book_id, location

1 fact table:

    reading_difficulty: contains scores and statistics about each book
        id, sentence_count, word_count, syllables_count, grade_level, reading_ease

There is a number of challenges identified with the Gutenberg source of data:
- Duplication of files for the same title, often due to the use of new formats. This can be seen either through the `/old` folder or through copy of files with the `-8.txt` prefix.
- The catalog data is in RDF format and the extraction of data must follow the protocol. This means that not all fields may be present, and we are limited in terms of meta data, or have to join it separetely with a different source (such as wiki data).
- The mapping between the catalog data and the text files itself is implicit. The RDF filenames contains the id which is also the name of the book's text file, the later has to be extracted from a nested directory structure.
- The Gutenberg data itself contains more than literary books but it is not currently possible to download only the books titles.
- A single book can have multiple volumes and these are separate files and treated as separate books.
- The content of these text files includes Gutenberg specic language and the beginning and the end which interferes with the scores calculation. Detecting such boundary is not fullproof.


The following files are relevant
- `lib/gutenberg.py`: contains code to extract catalog information from the gutenberg website, download teh text files, and upload them to S3.
- `lib\emr.py`: contains all the utilities need to create an EMR cluster, submit a job, and tear down the cluster.
- `lib\spark-etl.py`: contains the Spark Application code for computing the scores.
- `airflow\dags\gutenberg-etl.py`: contains the airflow DAG that orchestrate the whole process.


## Installation

### Prerequisits

AWS credentials with read/write permissions for S3 and AWS EMR must be available and stored in the `cfg/aws.cfg` file.

### Setup

To install the python virtual environment and dependencies:

`make install`

## Local Spark Development

a local spark installation must be available to run Spark in local standalone mode.

make sure to be in the venv:

`source venv/bin/activate`

First generate the catalog data:

`python lib/gutenberg.py --cmd download_catalog`

and download the data:

`python lib/gutenberg.py --cmd download_data`

The above will take up to 30 minutes to complete the first time (40Gb of download), and the data will be store in the `data/` folder.

Generate the catalog file by processing the RDF files:

`python lib/gutenberg.py --cmd generate_catalog`

This can also take up to 30mins, as it crawls through the directory containing all the text files, extract the id from each filename, load the RDF object, extract information, and output a csv file that links each book id with meta data and a url link. This file will then be used by the spark application to locate which text files to extract.

Finally, to run spark in local mode:

`make spark`

This will invoke `lib/spark-etl.py` which contains the code to generate the final dataset. Note that for local dev, a smaller catalog will be used `catalog_mini.csv` which consits of 20 book titles.

## Airflow Pipeline

In order to run the full pipeline with Airflow, you will need to launch airflow:

`airflow-start.sh`

then navigate and enable the DAG `gutenberg-etl` which is implemented in `airflow/dags/gutenberg-etl.py`

The Airflow DAG that performs the ETL is shown below:

![Airflow DAG][dag]

This DAG is scheduled to run weekly and will execute the following steps:
1) Download latest catalog data from Gutenberg and new text files.
2) Generate the catalog.csv file and upload it to S3, together with the relevant text files.
3) Create an EMR cluster and run the ETL pipeline as defined in lib/spark-etl.py.

See an example of the EMR config use for this project

![aws-emr-config][aws-emr-config]

to stop the airflow servers:

`airflow-stop.sh`

## Results

The Spark ETL pipline will generate the tables in the form of Parquet files stored in the S3 bucket {bucket_name}/gutenberg-data/results.

![aws-s3-buckets][aws-s3-buckets]

A Glue crawler can then be programmed to extract the table and show them in AWS Athena:

![Glue Crawler][crawler]

![Athena tables][athena]

The final resutls can be visualized by a query:

![athena-query][athena-query]

The Airflow DAG takes about 75min, scans over an 40gb of text, filters down to about ~20k books and ultimetly computes scores for *16k* books in Spark with the following statistics:
- 50,029,777 sentences were counted.
- 1,095,843,417 words were counted.
- 1,555,430,845 syllables counted.

## Performance

Some considerations regarding performance:
- The generation of the catalog.csv file currently takes time because each file is in the RDF format. This could be done in a distributed fashion in RDF.
- The initial upload of the texts files to S3 will take time. This is because S3 is optimized for a small number of large files rather than a large number of small files.
- The computation of scores in Spark was done via regex for detecting sentences and the [hypenate](https://github.com/jfinkels/hyphenate) module to detect syllabels, which implements the Lian Algorithm as defined in http://www.tug.org/docs/liang/liang-thesis.pdf.

See for example the GANTT chart for the Airflow Task runtime which showcase where the bulk of the computation happens (note that in this case, the text files had already been downloaded once):

![gantt][gantt]


Spark:
- In order to expedite the computations and meet the memory constraints, I used 5 slave workers of EC2 instance type m5.2xlarge, as defined in `cfg/emr.cfg`, with driver and executor memory of 5Gb. This allows me to have 9 active executors each with 2 cores and able to run 2-3 tasks. Under this configuration, the Spark Job takes about 20 minutes to complete.
- I have arrived at this configuration after monitoring the performance and iteratively increasing the memory capacity as well as the number of partitions, as I was running into heap full errors. I used Ganglia to keep track of my machines memory and CPU.

Ganglia snapshots:

![ganglia-cpu][ganglia-cpu]
![ganglia-mem][ganglia-mem]

As shown below the Full Spark SQL plan for computing the scores is complex:

![spark-sql-full][spark-sql-full]

We can also see the list 

