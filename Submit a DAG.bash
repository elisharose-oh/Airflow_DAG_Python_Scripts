#Submit a DAG
#Submitting a DAG is as simple as copying the DAG python file into dags folder in the AIRFLOW_HOME directory
#Open a terminal and run the command below to submit the DAG that was recently created 
cp my_first_dag.py $AIRFLOW_HOME/dags

#Verify that our DAG actually got submitted
airflow dags list

#Verify that DAG is part of the output. You should see your DAG name in the output.
airflow dags list|grep "my-first-dag"

#List out all the tasks in your DAG
airflow tasks list my-first-dag
