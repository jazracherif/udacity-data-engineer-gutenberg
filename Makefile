
clean:
	rm -R venv

venv:
	python3 -m venv venv

requirements:
	source venv/bin/activate; \
	pip install -r requirements.txt;

install: venv requirements
	export AIRFLOW_HOME=./airflow;  \
	export AIRFLOW__CORE__FERNET_KEY=$(cat fernet.key); \
	export PYTHONPATH=$PYTHONPATH:$(pwd); \
	source venv/bin/activate; airflow initdb

spark-dep:
	source venv/bin/activate; \
	pip install -t spark-dependencies -r requirements_spark.txt; \
	cd spark-dependencies; \
	zip -r ../spark-dependencies.zip . ;

spark: spark-dep
	spark-submit --py-files spark-dependencies.zip lib/spark-etl.py --mode local

test-spark:
	@source venv/bin/activate; python test_local.py 