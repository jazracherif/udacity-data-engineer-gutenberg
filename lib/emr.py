import boto3
import configparser
import argparse
import pandas as pd
import json
import os
import subprocess
import logging

# load the logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


credential_config = configparser.ConfigParser()
config = configparser.ConfigParser()

credential_config.read('cfg/aws.cfg')
config.read('cfg/emr.cfg')

AWS_PERMISSION_FILE_URL =os.environ.get("AWS_PERMISSION_FILE_URL")

CFG = {
    'KEY': credential_config.get('AWS', 'KEY'),
    'SECRET': credential_config.get('AWS', 'SECRET'),
    'REGION': credential_config.get('AWS', 'REGION'),

    'MASTER_TYPE': config.get('EMR', 'MASTER_TYPE'),
    'MASTER_INSTANCE_COUNT': config.get('EMR', 'MASTER_INSTANCE_COUNT'),
    'SLAVE_TYPE': config.get('EMR', 'SLAVE_TYPE'),
    'SLAVE_INSTANCE_COUNT': config.get('EMR', 'SLAVE_INSTANCE_COUNT'),
    'EC2_KEY_NAME': config.get('EMR', 'EC2_KEY_NAME'),
}


def list_clusters_steps(clusterId=None, StepStates=['RUNNING', 'PENDING']):
    """ List the steps assocaited with the clusterId and in the state 
        specified in stages

        By default will list the RUNNING steps in the first active EMR cluster

        args:
            * clusterId(str: the cluster ID string
            * StepStates(list): List of all step states. see https://boto3.amazonaws.com/v1
                /documentation/api/latest/reference/services/emr.html#EMR.Client.list_steps
    """
    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    # Get an active clusterId if none was provided.
    if clusterId is None:
        clusters = emr.list_clusters()

        active_clusters = [c["Id"] for c in clusters["Clusters"] 
                if c["Status"]["State"] in ["RUNNING", "WAITING"]]

        clusterId = active_clusters[0]

    # List all steps
    response = emr.list_steps(
                    ClusterId=clusterId,
                    StepStates=StepStates
                )

    for step in response["Steps"]:
        logger.info(f"{step['Status']['State']} -- {step['Id']} -- {step['Name']} -- { ' '.join(step['Config']['Args']) }")


def get_available_cluster():
    """ Get a cluster Id if any is available, else returns None
    """
    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    clusters = emr.list_clusters()

    active_clusters = [c["Id"] for c in clusters["Clusters"] 
            if c["Status"]["State"] in ["RUNNING", "WAITING"]]

    if active_clusters:
        return active_clusters[0]
    else:
        return None

def copy_etl_to_master(cluster_id=None):
    """ Copy the ETL files to the MASTER node of the EMR cluster described by 
        cluster_id. If cluster_id is None copy to the first active cluster.

        This is done using SCP and assumes it is properly setup beforehand.

        TODO: Another approach for this is to upload the files to S3 and import
        those into the cluster using an EMR step.
    """

    logger.info("=== Copy ETL file to Master node")

    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    if cluster_id is None:
        clusters = emr.list_clusters()

        active_clusters = [c["Id"] for c in clusters["Clusters"] 
                if c["Status"]["State"] in ["RUNNING", "WAITING"]]

        cluster_id = active_clusters[0]

    # Get the DNS address for the Master Node
    response = emr.list_instances(
            ClusterId=cluster_id,
            InstanceGroupTypes=['MASTER'],
            InstanceStates=['RUNNING'],
        )
    if response['Instances']:
        master_dns = response['Instances'][0]["PublicDnsName"]
        logger.info("Found Master Node")
    else:
        logger.info("No Running Master")
        master_dns = None

    # SCP the pipeline and dependency files
    if master_dns:
        FILE_NAME = "lib/spark-etl.py"
        DEPENDENCY_FILE = "spark-dependencies.zip"

        URL = f"hadoop@{master_dns}:/tmp/"
        if os.path.exists(FILE_NAME):            
            subprocess.run([
                        "scp",
                        "-o",
                        "StrictHostKeyChecking=no", # avoid the known host check
                        "-o",
                        "UserKnownHostsFile=/dev/null", # avoid the known host check
                        "-i",
                        AWS_PERMISSION_FILE_URL,
                        FILE_NAME, URL], 
                        capture_output=True
                        )
        else:
            logger.info(f"No {FILE_NAME} file")

        # Also copy the dependency zip file if it has been produced
        URL = f"hadoop@{master_dns}:/tmp/"
        if os.path.exists(DEPENDENCY_FILE):            
            subprocess.run([
                        "scp", 
                        "-o",
                        "StrictHostKeyChecking=no", # avoid the known host check
                        "-o",
                        "UserKnownHostsFile=/dev/null", # avoid the known host check
                        "-i",
                        AWS_PERMISSION_FILE_URL,
                        DEPENDENCY_FILE, 
                        URL], 
                        capture_output=True
                        )
        else:
            logger.error(f"No {DEPENDENCY_FILE} file")

    logger.info("Done!")
    return cluster_id

def describe_emr_clusters():
    """ Describe all active EMR clusters
    """
    logger.info("=== Describe EMR Clusters")

    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    clusters = emr.list_clusters()

    if clusters:
        res = pd.DataFrame(clusters['Clusters']).to_json(indent=4)
        logger.info(res)
    else:
        logger.error("Error no cluster information")

    active_clusters = [c["Id"] for c in clusters["Clusters"] 
                if c["Status"]["State"] in ["STARTING", "RUNNING", "WAITING"]]

    if active_clusters is None:
        logger.info("No active cluster")
        return

    response = emr.list_instances(
            ClusterId=active_clusters[0],
            InstanceGroupTypes=['MASTER'],
            InstanceStates=['RUNNING'],
        )

    if response['Instances']:
        res = pd.DataFrame(response['Instances']).to_json(indent=4)
        logger.info(res)
    else:
        logger.info("No Running Master")


def get_cluster_id_from_arn(cluster_arn):
    """ Get the cluster_id corresponding to a cluster_arn.
    """
    logger.info(f"=== Get Cluster Id for {cluster_arn}")

    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    rsp = emr.list_clusters()

    for c in rsp["Clusters"]:
        if c['ClusterArn'] == cluster_arn:
            return c["Id"]

    raise ValueError("Cluster Not Found")


def create_emr_cluster(steps=None, teardown=False):
    """ Create an AWS EMR cluster.

    args:
        * steps: a step dict if we want to run one
        * teardown: whether to teardown the cluster when there are no steps left
    """
    logger.info("=== Create EMR Cluster")

    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    config = {
        "Name": 'spark-EMR',
        "LogUri": 's3://jazra-udacity-emr/logs',
        "ReleaseLabel": 'emr-5.20.0',
        "Applications": [{
                                'Name': 'Spark',
                         },
                         {
                            'Name': 'Ganglia'
                         }],
        "Instances": {
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': CFG['MASTER_TYPE'],
                    'InstanceCount': int(CFG['MASTER_INSTANCE_COUNT']),
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': CFG['SLAVE_TYPE'],
                    'InstanceCount': int(CFG['SLAVE_INSTANCE_COUNT']),
                }
            ],
            'Ec2KeyName': CFG['EC2_KEY_NAME'],
            'KeepJobFlowAliveWhenNoSteps': False if teardown else True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-27ba277f',
        },
        "JobFlowRole": 'EMR_EC2_DefaultRole',
        "ServiceRole": 'EMR_DefaultRole',
    }

    if steps:
        config["steps"] = steps

    # run jobs
    rsp = emr.run_job_flow(**config)

    # Wait till the cluster is created
    cluster_arn = rsp["ClusterArn"]

    cluster_id = get_cluster_id_from_arn(cluster_arn)

    waiter = emr.get_waiter('cluster_running')

    logger.info("Waiting for Cluster setup completed...")
    waiter.wait(
        ClusterId=cluster_id,
        WaiterConfig={
            'Delay': 10,
            'MaxAttempts': 100
        }
    )
    logger.info("Cluster Running!")

    if teardown:
        waiter = emr.get_waiter('cluster_terminated')
        logger.info("Waiting for Cluster termination...")

        waiter.wait(
            ClusterId=cluster_id,
            WaiterConfig={
                'Delay': 10,
                'MaxAttempts': 100
            }
        )

        logger.info("Cluster Terminated!")
        return None
    else:
        return cluster_id


def create_steps_object(step_name, 
                        filename, 
                        dependencies_name=None):
    """ Create a step object for AWS EMR.

    args:
        * step_name(str): the name of the step
        * filename(str): the python file name, assumed under /tmp/
        * dependencies_name(str): Optional dependecies.zip file, assumed under /tmp
    """
    if dependencies_name:
        step_args = ["/usr/bin/spark-submit", 
                     "--py-files", 
                     "/tmp/"+dependencies_name,
                     "/tmp/"+filename, 
                     "--mode", 
                     "emr"
                     ]
    else:
        step_args = ["/usr/bin/spark-submit", 
                     "/tmp/"+filename, 
                     "--mode", 
                     "emr"]

    steps =[{
                'Name': step_name,
                'ActionOnFailure': 'CANCEL_AND_WAIT',            
                'HadoopJarStep': {
                    'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                    'Args': step_args
                    }
            }]

    return steps


def submit_job(cluster_id=None, use_dependencies=False):
    """ Create a step Job and submitted to any available EMR cluster
    """
    logger.info(f"=== Submit ETL job - cluster_id {cluster_id}")
    
    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    # 1. choose a cluster that is available
    if cluster_id is None:
        clusters = emr.list_clusters()
        clusters = [c["Id"] for c in clusters["Clusters"] 
                    if c["Status"]["State"] in ["RUNNING", "WAITING"]]

        if not clusters:
            raise Exception("No valid clusters")

        # take the first relevant cluster and run the step
        cluster_id = clusters[0]

    # 2. Submit the job
    rsp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=create_steps_object(step_name='gutenberg-ETL',
                                  filename="spark-etl.py",
                                  dependencies_name="spark-dependencies.zip")
        )
    logger.info(rsp)

    # 3. Wait for Step Completion
    logger.info("Waiting for Step Completion...")

    step_id = rsp["StepIds"][0]

    waiter = emr.get_waiter('step_complete')
    waiter.wait(
        ClusterId=cluster_id,
        StepId=step_id,
        WaiterConfig={
            'Delay': 10,
            'MaxAttempts': 1000
        }
    )
    logger.info("Done!")


def terminate_cluster(cluster_id):
    """ Create a step Job and submitted to any available EMR cluster

    args:
        * cluster_id: The cluster Id to terminate
    """

    logger.info(f"=== Terminate Cluster {cluster_id}")
    
    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    rsp = emr.terminate_job_flows(JobFlowIds=[cluster_id])

    waiter = emr.get_waiter('cluster_terminated')
    logger.info("Waiting for Cluster termination...")

    waiter.wait(
        ClusterId=cluster_id,
        WaiterConfig={
            'Delay': 10,
            'MaxAttempts': 100
        }
    )

    logger.info("Cluster Terminated!")


def create_and_submit_once():
    """ Create an EMR cluster, submit a job and then teardown the
        cluster.
    """

    cluster_id = get_available_cluster()
    if cluster_id is None:
        cluster_id = create_emr_cluster()

    copy_etl_to_master(cluster_id)

    submit_job(cluster_id)

    terminate_cluster(cluster_id)


def argparser():
    """ Command Line parser for the script
    """

    parser = argparse.ArgumentParser(description='Management utility for EMR cluster')
    parser.add_argument('--cmd', 
                        type=str,
                        required=True,
                        choices=["create-cluster", 
                                 "submit-job", 
                                 "describe-clusters", 
                                 "setup-etl",
                                 "list_clusters_steps",
                                 "create_and_submit_once"]
                        )

    parser.add_argument('--dependencies', 
                        type=bool,
                        required=False,
                        default=False)

    args = parser.parse_args()

    return args


def main():
    """ Get a running EMR cluster and submit the spark-etl.py spark job.

        The spark-etl.pyfile must have already been copied to the master node
    """
    args = argparser()
    cmd = args.cmd

    if cmd == "submit-job":
        copy_etl_to_master()
        submit_job(use_dependencies=args.dependencies)
    elif cmd == "create-cluster":
        create_emr_cluster()
    elif cmd== "describe-clusters":
        describe_emr_clusters()
    elif cmd == "setup-etl":
        copy_etl_to_master()
    elif cmd == "list_clusters_steps":
        list_clusters_steps()
    elif cmd == "create_and_submit_once":
        create_and_submit_once()

if __name__ == "__main__":
    main()
