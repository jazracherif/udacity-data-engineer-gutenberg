


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

`spark-submit --py-files dependencies.zip etl.py --mode emr`

