# Dbt project with postgres

## To install the dependancy packages

pip install -r requirements.txt

## project structure explained

dbt_project.yml: contains all the configurations of the project

profiles.yml: contains all the configurations of the database

package.yml: contains all the dependencies of the project

models: contains all the sql files

macros: contains all the macros files

seeds: contains all the local csv files that will be used as seeds

snapshots: contains all the records that will be used as snapshots if any changes in the csv file

tests: contains all the tests files

analytics: contains all the sql files that will be used to create the analytics benefit is did not create a new table or view  


## Materialized 
Type	            Use Case	                            Example
View	            Lightweight,                            real-time data	Today’s orders
Table	            Frequent queries,                       static data	Historical sales aggregates
Incremental 	    Large,                                  append-only datasets	Daily log processing
Ephemeral	        Reusable intermediate steps	Customer    preference calculation
Materialized View	Auto-refreshed summaries	            Real-time revenue dashboard

## To run the project

dbt run
dbt test
dbt show --select sql_file_name
dbt docs generate
dbt docs serve