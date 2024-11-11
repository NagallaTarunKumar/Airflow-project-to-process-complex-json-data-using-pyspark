This is an Apache Airflow DAG for processing JSON data files with Spark, transforming the data, and storing it in a CSV format. 
It is scheduled to run on daily basis, it checks the source directory for new JSON files. 
If a new/ unprocessed file is discovered at the source then the JSON data is processed using Spark and stored at the destination in CSV format.
