[components]: https://github.com/jazracherif/udacity-data-engineer-gutenberg/blob/master/docs/components.png

# Udacity Data Engineer Capstone: Gutenber books ETL

Originally developed to evaluate the difficulty of techincal reading material for the navy, the [Flesch–Kincaid readability tests](https://en.wikipedia.org/wiki/Flesch–Kincaid_readability_tests) have since been used widely on all sorts of text, including books. One of their main use in education circles is to enable teachers to find and recommend books based on their difficulty, as measured by these scores.

In this project, I create an ETL pipeline that compute such socres on all english book in the [Gutenberg](www.gutenberg.org) archive. The ETL to grab the raw data is implemented over Airflow and integrates with AWS EMR to run the calculations. With this approach, Over 25k books (\~8GB) of text can be ingested.

For every text, the number of sentences, words, and syllables are counted up in order to compute the two final scores:
- [Flesch–Kincaid grade level](https://en.wikipedia.org/wiki/Flesch–Kincaid_readability_tests#Flesch–Kincaid_grade_level) 
- Flesch reading ease(https://en.wikipedia.org/wiki/Flesch–Kincaid_readability_tests#Flesch_reading_ease)

The ETL architecture as implemented in the project consists of extract raw data from the gutenberg site itself, processing it into basic catalog information and textual file and uploading them into S3, then creating a Spark EMR cluster and running a Spark Application to compute the scores, which are then written back into S3 in parquet format, and ingested by Glue/Athena for analysis.

![Architectural Components][components]


## Catalog Data
Download [Gutenberg catalog](http://www.gutenberg.org/wiki/Gutenberg:Feeds)

and untar archive:
`tar -xvf rdf-files.tar`


## e-books Content data
to get files:
`wget -w 2 -m -H "http://www.gutenberg.org/robot/harvest?filetypes[]=txt&langs[]=en"`

or (faster)
`rsync -av --del --include "*.txt" --include='*/'  --exclude='*' aleph.gutenberg.org::gutenberg data/ > /dev/null`


## copy to S3
`aws s3 cp . s3://narrate-data/gutenberg-data --recursive  --exclude "*" --include "*.txt"`


Problems with data
- old files
- old v.s new formats 
- 2 different files \*.txt and \*-8.txt
- Catalog is in RDF format
- Catalog mapping to file directory not explicit.
- can't filter for books only. ?
- same book can have different volume as separate files.
- not all files could be downlaoded ?
- Actual text for a book doesn't start at the beginning
- detect beginning and end of sentences
- propagate python dependencies to workers
- join on the tables
- weird data content: tables 


### Number of syllables
Use the Liang algorithm as defined in http://www.tug.org/docs/liang/liang-thesis.pdf
and implemented in [hypenate](https://github.com/jfinkels/hyphenate)
### Generate Python Dependecies for workers:

```
pip install -t dependencies -r requirements.txt
cd dependencies
zip -r ../dependencies.zip .
```

Run the spark jobs with the following command:

`spark-submit --py-files dependencies.zip spark-etl.py --mode emr`

