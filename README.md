# access-log-analyzer
[![Build Status](https://app.travis-ci.com/jjxp/access-log-analyzer.svg?token=TkxJzcxKuz9yBNXCfFKU&branch=develop_unit_tests)](https://app.travis-ci.com/jjxp/access-log-analyzer)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=jjxp_access-log-analyzer&metric=alert_status)](https://sonarcloud.io/dashboard?id=jjxp_access-log-analyzer)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=jjxp_access-log-analyzer&metric=coverage)](https://sonarcloud.io/dashboard?id=jjxp_access-log-analyzer)

This is a Python coding challenge proposed by secureworks.
The purpose of this job is to download an access log dataset from NASA (July, 95) and compute the n-most-frequent visitors and URLs for each day of the trace using Spark.

## Features
- Integration with Travis CI, which offers automatic execution of unit tests
- Integrated with SonarQube, providing static code analysis and test coverage metrics
- Includes auto-generated documentation ([CHECK IT!]( https://htmlpreview.github.io/?https://github.com/jjxp/access-log-analyzer/blob/main/docs/jobs/nasa_access_logs_analyzer.html))
- Integration with Docker

## Installation and use
access-log-analyzer can be executed both in local and Docker.
### Local
*Assumptions:* git, Spark, Python and Java must be already installed, configured and ready-to-use in your local machine.
It may work on other versions, but it is only guaranteed to work in Spark 3.1, Python 3.8 and Java 1.8.

- Clone the repository:
```
git clone https://github.com/jjxp/access-log-analyzer.git
cd access-log-analyzer/
```

- Install the required dependencies
```
pip install -r requirements.txt
```

- Run the program

_n: Number of most-frequent visitors/hosts that you want to calculate_

_dataset_url: FTP URL for the dataset that you want to download (ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz)_
```
python jobs/nasa_access_logs_analyzer.py {{n}} {{dataset_url}}
```

- Check your results in the generated folders under your current directory:

{{n}}\_most\_frequent_visitors.csv

{{n}}\_most\_frequent_urls.csv

### Docker
*Assumptions:* git and Docker must be already installed, configured and ready-to-use in your local machine.

- Clone the repository:
```
git clone https://github.com/jjxp/access-log-analyzer.git
cd access-log-analyzer/
```

- Build the Docker image
```
docker build -t access-log-analyzer .
```

- Run the program

_pwd: Your working directory (%cd% in Windows cmd, ${PWD} in Windows PowerShell and ${pwd} in Linux_

_n: Number of most-frequent visitors/hosts that you want to calculate_

_dataset_url: FTP URL for the dataset that you want to download (ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz)_
```
docker run -p4050:4050 -v {{pwd}}:/opt/application access-log-analyzer driver local:///opt/application/jobs/nasa_access_logs_analyzer.py {{n}} {{dataset_url}}
```
In the previous command, you'll notice that:
- Networking is enabled, and local port 4050 is mapped to target port 4050. This, together with the Spark configuration defined in the job, allows you to access the Spark Web UI in http://localhost:4050/jobs in the meantime the jobs is running.
- A volume is defined, which will allow you to access the output CSV files of the job.

At last, you can check your results in the generated folders under your current directory:

{{n}}\_most\_frequent_visitors.csv

{{n}}\_most\_frequent_urls.csv

## FAQ

###### Why are you parsing the logs with a regex and RDDs?
Regex are a very powerful tool to check the validity of the data, and eventually splitting it into groups.

RDDs are, from a programmer's perspective, the lowest abstraction level in Spark. They are fault-tolerant, distributed, and its map-reduce operations allow us to have control over the way we want Spark to perform the computations. RDDs are excelent for both structured and non-structured data and for map-reduce operations for each line. Just what we need.

###### Why are the main operations performed using DataFrames?
DataFrames are a lot easier to understand than RDDs, which means that it will be easier to develop, to mantain, and for others to understand. DataFrames are ideal for complex operations like the Window functions that are needed to calculate the n-most-frequent values for each day.

Also, from a computational perspective, DataFrames usually perform better than RDDs.

###### Why are you using an already existing base image in your Dockerfile from DataMechanics?
I have chosen to use a base image to simplify the configuration effort it would require, and to avoid potential issues that could arise in the process.
Instead, I considered it a better idea to use a image that is built by a real company that dedicates to this, and that is also tested and used by many other companies.

## Assumptions

Taking into account the requirements, I have assumed the following points:

- Only one source file will be analyzed at a time. That is, multiple logs are not supported, and if you want to analyze the logs of August, you will have to execute again the program.
- Source datasets will always have the same format: Common Log Format. Any other formats rather than CLF could potentially cause the program to consider the lines as not valid.
- Source datasets will always be hosted in a FTP server which requires no login.
- Source datasets will always meet the requirements for SparkContext's textFile function to be able to read them. To understand these requirements, please, refer to the official documentation [here](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.SparkContext.textFile.html).
- If 'n' is greater than the maximum number of visitors or hosts for a particular day (max_n from now on), then n will be set to max_n for that day.
- The program will only consider all the days in the trace. That is, if for some reason there weren't any logs for a particular day, that day will not be present in the output.
- The program will not be executed in an external Spark cluster. I have chosen to package Spark and the Python application in the same container and run it in local mode because (1) it's too little data (around 20mb) and (2) it will be a lot easier to get the application running. For sure this is not a scenario that you will see in a Production environment, but it's great for this exercise's purpose.

If we wanted to run the program in a real cluster, we would only package the Python code in a single image without any Spark binaries. Instead, the Spark cluster will be already set up (or we could set it up, for instance using Amazon EMR or the big-data-europe templates available [here](https://github.com/big-data-europe/docker-spark)) and the application images would be tagged and pushed to a repository that connects directly to Spark.