from datetime import datetime, date
import os
import argparse
import itertools
from hyphenate import hyphenate_word

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, 
    col, 
    split, 
    regexp_replace,
    explode,
    flatten,
    size,
    input_file_name,
    expr,
    sum as f_sum,
)
from pyspark.sql.functions import (
    year, 
    month, 
    dayofmonth, 
    hour, weekofyear, 
    date_format, 
    dayofweek,
)

from pyspark.sql.types import (
    TimestampType,
    DateType,
    ArrayType,
    StringType,
    IntegerType,
)


def create_spark_session(mode):
    """ Create a Spark Session and set the memory requirements
    """    
    if mode == 'local':
        setMaster = "local[2]"
    elif mode == 'emr':
        setMaster = "yarn"

    conf = SparkConf() \
             .setMaster(setMaster) \
             .setAppName("ETL") \
             .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
             .set("spark.executor.memory", "5g")  \
             .set("spark.driver.memory", "5g") \
             .set("spark.default.parallelism", "100") \
             .set("spark.executor.memoryOverhead", "5G")

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    return spark


def process_data(spark, input_data, output_data, mode):
    """ Ingest the Catalog and process all the files included in it,
        computing reading statistics and storing them in parquet files.

        Currently handles only english titles.
    """
    if mode == 'local':
        catalog_data_url = input_data + "catalog/catalog_mini.csv"
    else:
        catalog_data_url = input_data + "catalog/catalog.csv"


    df = spark.read.csv(catalog_data_url, sep='\t', header=True)

    print(df.printSchema())

    catalog_table = df.select(["title", "author", "language", "id", "_url"]) \
                      .filter("language = 'en'")

    catalog_table = catalog_table.withColumnRenamed("title", "book_title") \
                                 .withColumnRenamed("_url", "location") \
                                 .withColumnRenamed("id", "book_id") \
                                 .distinct()

    catalog_table.write.parquet(output_data+"catalog", 
                              mode='overwrite',
                              # partitionBy=["author"],
                            )

    # Collect the list of files. This can be large ~20k
    book_url_list = [input_data + str(x.location)[6:] for x in catalog_table.select("location").collect()]

    # Get list of Book ids from filenames
    files_df = spark.read.text(book_url_list, wholetext=True)

    print("partition size", files_df.rdd.getNumPartitions())

    # Create a book_id column based on filename
    BOOK_IF_UDF = udf(lambda x: x.split("/")[-1].split(".txt")[0], StringType())
    files_df = files_df.withColumn("id", BOOK_IF_UDF(input_file_name()))

    """
     Each Row in the files_df will represent 1 full book length text
     In the following, we split each row's data to find the number of sentences,
     the number of words and the number of syllables, in order to compute the
     grade level and reading ease scores.
    """

    # Regex to detect a sentence from From https://stackoverflow.com/questions/25735644/
    #    python-regex-for-splitting-text-into-sentences-sentence-tokenizing
    SENTENCE_REGEX = "(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"
    SYLLABLES_UDF = udf(lambda x: len(hyphenate_word(x)), IntegerType())

    # Generate list of sentences, words and syllables for each book.
    sentences = files_df.select("id", split("value", SENTENCE_REGEX).alias("sentences"))
    sentence_count = sentences.select("id", size("sentences").alias("sentence_count"))

    words_df = files_df.select("id", split("value", ' ').alias("words"))
    word_count = words_df.select("id", size("words").alias("word_count"))
    syllables_count = words_df.select("id", explode("words").alias("syllables")) \
                           .select("id", SYLLABLES_UDF("syllables").alias("syllables")) \
                           .groupby("id").agg(f_sum("syllables").alias("syllables_count"))


    # compute all scores
    results = sentence_count.join(word_count, "id", "inner").join(syllables_count, "id", "inner")

    results = results.withColumn("grade_level", 0.39 * col("word_count") \
                                    / col("sentence_count") + 11.8 *  \
                                     col("syllables_count") / col("word_count") - 15.59)
    
    results = results.withColumn("reading_ease", 206.835 - col("word_count") \
                                     / col("sentence_count") - 84.6  * col("syllables_count") \
                                     / col("word_count"))

    results.write.parquet(path=output_data+"reading_difficulty",
                    mode="overwrite",
                    )

def argparser():
    """ Command Line parser for the script
    """

    parser = argparse.ArgumentParser(description='ETL Pipeline utility')

    parser.add_argument('--mode', 
                        type=str,
                        required=True,
                        choices=["local", 
                                 "emr", 
                                 ]
                        )

    args = parser.parse_args()

    return args


def main():
    """ Main entrypoint for the Spark ETL. 
        2 modes are supported: Local or EMR
    """
    args = argparser()
    mode = args.mode
 
    spark = create_spark_session(mode)

    if mode == 'emr':
        input_data = "s3://jazra-gutenberg/gutenberg-data/data/"
        output_data = "s3://jazra-gutenberg/gutenberg-data/results/"
        spark.sparkContext.addPyFile("/tmp/spark-dependencies.zip")
    elif mode == 'local':
        input_data = "data/"
        output_data = "out/"
        spark.sparkContext.addPyFile("spark-dependencies.zip")

    # process_catalog(spark, input_data, output_data)
    process_data(spark, input_data, output_data, mode)


if __name__ == "__main__":
    main()
