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
