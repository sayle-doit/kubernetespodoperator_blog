import datetime

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators import kubernetes_pod
from airflow.models import Variable

import os

JOB_NAME = 'sample_dag_task_running_on_gke'
start_date = datetime.datetime(2021, 1, 31)

default_args = {
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
}

with models.DAG(JOB_NAME,
                default_args=default_args,
                schedule_interval=None,
                catchup=False) as dag:

    # This value can be customized to whatever format is preferred for the node pool name
    # Default node pool naming format is <cluster name>-node-pool-<execution_date>
    node_pool_value = "$COMPOSER_ENVIRONMENT-node-pool-$(echo {{ ts_nodash }} | awk '{print tolower($0)}')"

    create_node_pool_command = """
    # Set some environment variables in case they were not set already
    [ -z "${NODE_COUNT}" ] && NODE_COUNT=3
    [ -z "${MACHINE_TYPE}" ] && MACHINE_TYPE=e2-standard-8
    [ -z "${SCOPES}" ] && SCOPES=default,cloud-platform
    [ -z "${NODE_POOL_VARIABLE}" ] && NODE_POOL_VARIABLE=node_pool
    
    # Generate node-pool name 
    NODE_POOL=""" + node_pool_value + """
    gcloud container node-pools create "$NODE_POOL" --project $GCP_PROJECT --cluster $COMPOSER_GKE_NAME \
    --num-nodes "$NODE_COUNT" --zone $COMPOSER_GKE_ZONE --machine-type $MACHINE_TYPE --scopes $SCOPES \
    --enable-autoupgrade
    
    # Set the airflow variable name
    airflow variables -s $NODE_POOL_VARIABLE $NODE_POOL
    """

    delete_node_pools_command = """
    # Generate node-pool name 
    NODE_POOL=""" + node_pool_value + """
    
    gcloud container node-pools delete "$NODE_POOL" --zone $COMPOSER_GKE_ZONE --cluster $COMPOSER_GKE_NAME --quiet
    """

    # Tasks definitions
    create_node_pool_task = BashOperator(
        task_id="create_node_pool",
        bash_command=create_node_pool_command,
        xcom_push=True,
        dag=dag
    )

    delete_node_pool_task = BashOperator(
        task_id="delete_node_pool",
        bash_command=delete_node_pools_command,
        trigger_rule='all_done',        # Always run even if failures so the node pool is deleted
        dag=dag
    )

    compute_resources = \
        {'request_cpu': '1200m',
         'request_memory': '3Gi',
         'limit_cpu': '1600m',
         'limit_memory': '6Gi'}

    etl_task = kubernetes_pod.KubernetesPodOperator(
        task_id='etl-task',
        name='etl',
        namespace='default',
        image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
        resources=compute_resources,
        cmds=["sh", "-c", 'echo \'Sleeping..\'; sleep 120; echo \'Done!\''],
        startup_timeout_seconds=720,
        # affinity allows you to constrain which nodes your pod is eligible to
        # be scheduled on, based on labels on the node. In this case, if the
        # label 'cloud.google.com/gke-nodepool' with value
        # 'nodepool-label-value' or 'nodepool-label-value2' is not found on any
        # nodes, it will fail to schedule.
        affinity={
            'nodeAffinity': {
                # requiredDuringSchedulingIgnoredDuringExecution means in order
                # for a pod to be scheduled on a node, the node must have the
                # specified labels. However, if labels on a node change at
                # runtime such that the affinity rules on a pod are no longer
                # met, the pod will still continue to run on the node.
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            # When nodepools are created in Google Kubernetes
                            # Engine, the nodes inside of that nodepool are
                            # automatically assigned the label
                            # 'cloud.google.com/gke-nodepool' with the value of
                            # the nodepool's name.
                            'key': 'cloud.google.com/gke-nodepool',
                            'operator': 'In',
                            # The label key's value that pods can be scheduled
                            # on.
                            # In this case it will execute the command on the node
                            # pool created by the Airflow bash operator.
                            'values': [
                                Variable.get("node_pool", default_var=node_pool_value)
                            ]
                        }]
                    }]
                }
            }
        })

    etl_task2 = kubernetes_pod.KubernetesPodOperator(
        task_id='etl-task2',
        name='etl',
        namespace='default',
        image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
        resources=compute_resources,
        cmds=["sh", "-c", 'echo \'Sleeping..\'; sleep 120; echo \'Done!\''],
        startup_timeout_seconds=720,
        # affinity allows you to constrain which nodes your pod is eligible to
        # be scheduled on, based on labels on the node. In this case, if the
        # label 'cloud.google.com/gke-nodepool' with value
        # 'nodepool-label-value' or 'nodepool-label-value2' is not found on any
        # nodes, it will fail to schedule.
        affinity={
            'nodeAffinity': {
                # requiredDuringSchedulingIgnoredDuringExecution means in order
                # for a pod to be scheduled on a node, the node must have the
                # specified labels. However, if labels on a node change at
                # runtime such that the affinity rules on a pod are no longer
                # met, the pod will still continue to run on the node.
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            # When nodepools are created in Google Kubernetes
                            # Engine, the nodes inside of that nodepool are
                            # automatically assigned the label
                            # 'cloud.google.com/gke-nodepool' with the value of
                            # the nodepool's name.
                            'key': 'cloud.google.com/gke-nodepool',
                            'operator': 'In',
                            # The label key's value that pods can be scheduled
                            # on.
                            # In this case it will execute the command on the node
                            # pool created by the Airflow bash operator.
                            'values': [
                                Variable.get("node_pool", default_var=node_pool_value)
                            ]
                        }]
                    }]
                }
            }
        })

    # Tasks order
    create_node_pool_task >> [etl_task, etl_task2] >> delete_node_pool_task
