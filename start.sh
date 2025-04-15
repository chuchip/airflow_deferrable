# source ./airflow_env/bin/activate
nohup airflow webserver -p 8080 > webserver.log 2>&1 &
nohup airflow scheduler > scheduler.log 2>&1 &
nohup airflow triggerer > triggerer.log 2>&1 &
