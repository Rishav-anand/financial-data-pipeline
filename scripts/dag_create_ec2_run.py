from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import boto3, paramiko, time, os

# CONFIG
REGION = 'ap-south-1'
AMI_ID = 'ami-0f535a71b34f2d44a'
INSTANCE_TYPE = 't2.micro'
KEY_NAME = 'key_value_new'
SECURITY_GROUP_IDS = ['sg-081b72a9b8521180d']
IAM_INSTANCE_PROFILE = {'Name': 'ec2_access_s3'}
S3_BUCKET = 'financial-data-pipeline-project'
PEM_PATH = '/home/rishav_ubuntu/.ssh/key_value_new.pem'

# Step 1: Launch EC2
def launch_instance(**context):
    ec2 = boto3.client('ec2', region_name=REGION)
    response = ec2.run_instances(
        ImageId=AMI_ID,
        InstanceType=INSTANCE_TYPE,
        KeyName=KEY_NAME,
        SecurityGroupIds=SECURITY_GROUP_IDS,
        IamInstanceProfile=IAM_INSTANCE_PROFILE,
        MinCount=1,
        MaxCount=1,
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{'Key': 'Name', 'Value': 'Airflow-ETL-Instance'}]
        }]
    )
    instance_id = response['Instances'][0]['InstanceId']
    ec2.get_waiter('instance_running').wait(InstanceIds=[instance_id])
    desc = ec2.describe_instances(InstanceIds=[instance_id])
    public_ip = desc['Reservations'][0]['Instances'][0]['PublicIpAddress']
    context['ti'].xcom_push(key='instance_id', value=instance_id)
    context['ti'].xcom_push(key='public_ip', value=public_ip)

# Step 2: Copy scripts to EC2 via Paramiko
def copy_script_to_ec2(**context):
    public_ip = context['ti'].xcom_pull(task_ids='launch_ec2', key='public_ip')
    s3 = boto3.client('s3')
    files = [
        {"s3_key": "scripts/fetch_exchange_rates.py", "local": "/tmp/fetch_exchange_rates.py", "remote": "/home/ec2-user/fetch_exchange_rates.py"},
        {"s3_key": "scripts/install_dependencies.sh", "local": "/tmp/install_dependencies.sh", "remote": "/home/ec2-user/install_dependencies.sh"}
    ]
    for f in files:
        s3.download_file(S3_BUCKET, f["s3_key"], f["local"])

    key = paramiko.RSAKey.from_private_key_file(PEM_PATH)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for _ in range(5):
        try:
            ssh.connect(hostname=public_ip, username='ec2-user', pkey=key, timeout=10)
            break
        except:
            time.sleep(5)
    sftp = ssh.open_sftp()
    for f in files:
        sftp.put(f["local"], f["remote"])
    sftp.close()
    ssh.close()

# Step 3: Run dos2unix on the shell script
def convert_to_unix(**context):
    public_ip = context['ti'].xcom_pull(task_ids='launch_ec2', key='public_ip')
    key = paramiko.RSAKey.from_private_key_file(PEM_PATH)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=public_ip, username='ec2-user', pkey=key)

    # Step 1: Install dos2unix
    print("📦 Installing dos2unix...")
    stdin, stdout, stderr = ssh.exec_command("sudo yum install dos2unix -y")
    print(stdout.read().decode())
    print(stderr.read().decode())

    # Step 2: Run dos2unix on script
    print("🔧 Converting to UNIX format...")
    stdin, stdout, stderr = ssh.exec_command("dos2unix ~/install_dependencies.sh")
    print(stdout.read().decode())
    print(stderr.read().decode())

    ssh.close()


# Step 4: Run install_dependencies.sh
def run_installer(**context):
    public_ip = context['ti'].xcom_pull(task_ids='launch_ec2', key='public_ip')
    key = paramiko.RSAKey.from_private_key_file(PEM_PATH)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=public_ip, username='ec2-user', pkey=key)
    stdin, stdout, stderr = ssh.exec_command("bash ~/install_dependencies.sh")
    print(stdout.read().decode())
    print(stderr.read().decode())
    ssh.close()

def run_fetch_script(**context):
    public_ip = context['ti'].xcom_pull(task_ids='launch_ec2', key='public_ip')
    key = paramiko.RSAKey.from_private_key_file(PEM_PATH)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=public_ip, username='ec2-user', pkey=key)

    # Run the Python script
    print("🚀 Executing fetch_exchange_rates.py...")
    stdin, stdout, stderr = ssh.exec_command("python3 ~/fetch_exchange_rates.py")
    print(stdout.read().decode())
    print(stderr.read().decode())

    ssh.close()

# DAG
with DAG(
    dag_id='launch_and_prepare_ec2',
    default_args={'owner': 'rishav'},
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['aws', 'ec2'],
) as dag:

    t1 = PythonOperator(
        task_id='launch_ec2',
        python_callable=launch_instance,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='copy_script_to_ec2',
        python_callable=copy_script_to_ec2,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id='convert_script_to_unix',
        python_callable=convert_to_unix,
        provide_context=True
    )

    t4 = PythonOperator(
        task_id='install_dependencies',
        python_callable=run_installer,
        provide_context=True
    )
    
    t5 = PythonOperator(
        task_id='run_fetch_script',
        python_callable=run_fetch_script,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    t1 >> t2 >> t3 >> t4 >> t5
