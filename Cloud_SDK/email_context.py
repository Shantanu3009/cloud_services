import json
import logging
import os
from airflow.utils.email import send_email

# Install pendulum if not already installed
#os.system('pip3 install pendulum')
import pendulum

#import calling_config as ct  # Fixed import for calling_config

SUB_APP = 'shan_bq_to_bt_email_status'
PROJECT_ID = os.environ.get("GCP_PROJECT")
ENV =  'dev'  #PROJECT_ID.split('-')[-1]

# Define email groups
success_to_mail = ["shantanusgh0@gmail.com"]
success_cc_mail = ["shantanusgh0.aiesec@gmail.com"]

if ENV == 'dev':
    fail_to_mail = ["shantanusgh0@gmail.com"]
    fail_cc_mail = ["shantanusgh0.aiesec@gmail.com"]
elif ENV == 'prod':
    fail_to_mail = ["shantanusgh0@gmail.com"]
    fail_cc_mail = ["shantanusgh0.aiesec@gmail.com"]

# Helper function to merge dictionaries
def merge(*args):
    j = {}
    for i in args:
        j.update(i)
    return j

# Email callbacks
def success_call(context):
    email_function(context, "Success")

def fail_call(context):
    email_function(context, "Failed")

date_timezone = str(pendulum.now('US/Eastern'))

def email_function(context, status):
    """
    Sends an email notification based on the DAG run status.

    :param context: The context dictionary provided by Airflow
    :param status: "Success" or "Failed" status of the DAG
    """
    dag_run = context.get('dag_run')
    task_id = context["task_instance"].task_id
    DAG_ID = context["task_instance"].dag_id
    logs_url = context.get('task_instance').log_url

    if status == "Success":
        to_email = success_to_mail
        cc_email = success_cc_mail
        msg = f"""
        <tr>
            <p>Hi,</p>
            <p>Below are the details about the run:</p>
            <p>Dag_id : {DAG_ID},</p>
            <p>Task_id: {task_id},</p>
            <p>Status: <b style="color:green;">{status}</b>,</p>
            <p>Date_Time: {date_timezone},</p>
            <p>Log_URL : <a href="{logs_url}">{logs_url}</a></p>
            <p>Thanks,</p>
            <p>IO-OPS</p>
        </tr>
        """
    else:
        to_email = fail_to_mail
        cc_email = fail_cc_mail
        msg = f"""
        <tr>
            <p>Hi,</p>
            <p>Below are the details about the run:</p>
            <p>Dag_id: {DAG_ID},</p>
            <p>Task_id: {task_id},</p>
            <p>Status: <b style="color:red;">{status}</b>,</p>
            <p>Date_Time: {date_timezone},</p>
            <p>Log_URL: <a href="{logs_url}">{logs_url}</a></p>
            <p>Thanks,</p>
            <p>IO-OPS</p>
        </tr>
        """

    subject = f"{SUB_APP}: Composer Dag Status: Task ID: {task_id} - {status}!!"
    send_email(to=to_email, cc=cc_email, subject=subject, html_content=msg)