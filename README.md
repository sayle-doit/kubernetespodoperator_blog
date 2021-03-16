# Sayle's KubernetesPodOperator Medium Article Code

This is the codebase to be used with the Medium article located at: <TOOD Insert URL here>

It contains a DAG that will create a new GKE node pool, run 2 KubernetesPodOperator tasks, and then delete the created node pool.

## Notes
In order to run this you will need to create a new Cloud Composer instance inside of your GCP project and then copy the contents of this repository into your DAGs Google Cloud Storage bucket.

Once that has been completed, refresh your Airflow UI and it will show up as a DAG named sample_dag_task_running_on_gke in there.

## Task Definitions
create_node_pool_task<br />
Creates a new GKE node pool.

etl_task, etl_task2<br />
User-replaceable tasks that will load up an Ubuntu container that sleeps for 120 seconds and then exits.

delete_node_pool_task<br />
Deletes the created node pool.

## Environment Variables
NODE_COUNT<br />
The number of nodes to provision inside the node pool. Default value is 3

MACHINE_TYPE<br />
The instance type for instances provisioned inside the node pool. Default value is e2-standard-8<br />
This equates to a VM with 8 vCPUs and 32GB of RAM

SCOPES<br />
The scopes that will be applied to the nodes inside the node pool. Default value is default,cloud-platform
