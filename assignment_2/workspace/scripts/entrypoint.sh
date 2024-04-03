#!/usr/bin/env bash
airflow db init
airflow users create -r Admin -u admin -e mohammedkumte@gmail.com -f admin -l user -p admin1234
airflow webserver