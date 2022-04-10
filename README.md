# Drugs publications analysis
## Requirements

- [Brew](https://brew.sh/)
- [Pip3](https://pip.pypa.io/en/stable/installation/)
- [Python3.8](https://www.python.org/downloads/)

>Note: We recommend you to use python version **3.8**

## Getting Started

To set up dev environment:

````shell
pip3 install virtualenv
python3 -m virtualenv venv
source ./venv/bin/activate
pip3 install -r requirements.txt
````


This project aims to help users analyze drugs by processing raw data from several sources to find, by drug: pubmed articles, journals and clinical trials who mentioned this drug.

Example input files are located in resources folder:
- clinical_trials.csv
- drugs.csv
- pubmed.csv

## Main pipeline
the main pipeline generates a graph showing the relation between the drugs and medical publications

 To launch a job, run:
````shell
python -m src.jobs.pipeline_drugs_mentioned --drugs_file_path=resources/drugs.csv --pubmed_file_path=resources/pubmed.csv --clinical_trials_file_path=resources/clinical_trials.csv --result_file_path=resources/result_drug_mentioned
````

##  Additional feature:
find the 'biggest' journal who mentioned the biggest number of different drugs

To launch a biggest journal job, you should run:
````shell
python -m src.jobs.pipeline_biggest_journal --json_file_path=test/resources/result_drug_mentioned.json --result_file_path=test/resources/result_biggest_journal
 ````
 
To run the job with Airflow, you can specify these paths in application_args of PythonSubmitOperator or ShellSubmitOperator.


## Tests
Integration tests are implemented in test folder, this tests launch the full pipeline il local environment and write result in local resources folder
 
To run tests, use command: 
````shell
python -m unittest discover test
````

## Production

Apache Beam has unified programming model to define and execute data processing pipelines  for billions of lines of data, we can adjust parameters to let our code to scale massively:
- Use in scalable runner like DataFlowRunner or Flink
- Enable auto scaling, to process data massively in paralele
- Limit maxNumWorkers to control cost.
- Enable enableStreamingEngine, if we process data in streaming mode or Dataflow Shuffle
- Split source data to files with moderate size 
