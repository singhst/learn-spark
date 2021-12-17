# Apache Airflow

- [Apache Airflow](#apache-airflow)
- [Installation](#installation)

# Installation

URL: 
https://pypi.org/project/apache-airflow/

**Be careful to the python version. My Python is in v3.8, change the `3.7` to `3.8`.**

```shell
pip install 'apache-airflow==2.2.2' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.2/constraints-3.7.txt"
```

```

http://edwardinaction.blogspot.com/2019/12/airflow-docker.html

```shell
docker run -d -p 8080:8080 -v /path/to/dags/on/your/local/machine/:/usr/local/airflow/dags  puckel/docker-airflow webserver
```