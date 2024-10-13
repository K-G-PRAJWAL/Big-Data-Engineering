# Learn Apache Airflow

Apache Airflow is an open-source platform to programmatically author, schedule, and monitor your data pipelines or workflows.

### Key concepts

- Workflow: a sequence of tasks that processes a set of data helping you to build pipelines.
- Scheduling: the process of planning, controlling, and optimizing when a particular task should be done.
- Authoring: a workflow using Airflow is done by writing python scripts to create DAGs (Directed acyclic graphs).
- DAG: are collections of tasks and describe how to run a workflow written in Python. Pipelines are designed as a directed acyclic graph by dividing a pipeline into tasks that can be executed independently. Then these tasks are combined logically as a graph.
- Task: defines a unit of work within a DAG i.e. A node with the DAG. Task is an implementation of an Operator.
- Task instance: represents a specific run of a task characterized by a DAG, a task, and a point in time.
- Operators: Operators are atomic components in a DAG describing a single task in the pipeline.Airflow provides operators for common tasks - Ex. PythonOperator, BashOperator. We are free to create our own custom operator as well. (https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/index.html, https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html)
- Sensors: Are special types of operators whose purpose is to wait on some external or internal trigger - Ex: ExternalTaskSensor, HivePartitionSensor, S3KeySensor
- Hooks: Provide a uniform interface to access external services like S3, MySQL, Hive, EMR, etc.
- Scheduling: The DAGs and tasks can be run on demand or can be scheduled to be run at a certain frequency defined as a cron expression in the DAG.
