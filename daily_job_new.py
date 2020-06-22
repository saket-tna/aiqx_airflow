import datetime as dt
from airflow import DAG
from airflow.contrib.operators.qubole_operator import QuboleOperator
import json
import requests

DAYSERIAL_NUMERIC = dt.datetime.strftime(dt.datetime.now(),"%Y%m%d")

def send_mail(context,success):
    emails = ["a7j9n5x2y5p1b9x0@wearemiq.slack.com"]
    payload ='{"notifications":[{"type":"notify","notification":{"channelType":"EMAIL","contact":{"from":"dwh-team@miqdigital.com","to":[],"cc":[],"bcc":[]},"content":{"subject":"","body":"","type":"html"}}}],"resourceTags":{"TEAM":"RND","PRODUCT":"AIQX"}}'
    headers = {'Content-type': 'application/json','environment': 'PRODUCTION','department': 'TECH','team': 'DATAWAREHOUSE','product':'AIQX','owner':'saket.sagar@miqdigital.com','cache-control': 'no-cache'}
    if success:
        body = "Hi,<br><br>\
                DAG Status: Success<br>\
                DAG: {}<br><br>Airflow".format(context['dag'].dag_id)
    else:
        qbl_cmd_id = str(context['exception']).split()[2]
        body = "Hi,<br><br>\
                DAG Status: Failed<br>\
                Task: {}<br>\
                DAG: {}<br>\
                <a href=\"{}\">Log Link</a><br>\
                <a href = \"https://us.qubole.com/v2/analyze?command_id={}\">QDS Link</a><br><br>\
                Airflow".format(context['ti'].task_id, context['ti'].dag_id, context['ti'].log_url, qbl_cmd_id)

    json_data = json.loads(payload)
    json_data['notifications'][0]['notification']['content']['subject'] = "Airflow alert: {}".format(context['dag'].dag_id)
    json_data['notifications'][0]['notification']['content']['body'] = body
    json_data['notifications'][0]['notification']['contact']['to'] = emails
    payload =  json.dumps(json_data)
    r = requests.post('https://api-gateway.mediaiqdigital.com/notification/notifications',data=payload,headers=headers)
    print(r.status_code, r.text)

def failure_notify_email(context):
    send_mail(context, False)

def success_notify_email(context):
    send_mail(context, True)


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 3, 31),
    'concurrency': 1,
    'retries': 1,
    'on_failure_callback': failure_notify_email
}

dag = DAG('aiqx_daily_job',
            default_args=default_args,
            schedule_interval='30 7 * * *',
            on_success_callback=success_notify_email,
            default_view='graph')

t1 = QuboleOperator(
    task_id='shell',
    command_type="shellcmd",
    script_location="s3://aiqdatabucket/scripts/aiqx/uns_qubole/aiqx_daily_airflow/shell.sh",
    parameters=DAYSERIAL_NUMERIC,
    dag = dag,
    cluster_label='AIQX-uns-adv-segments'
)

t2 = QuboleOperator(
    task_id='user_adv_segments',
    command_type="hivecmd",
    script_location="s3://aiqdatabucket/scripts/aiqx/uns_qubole/aiqx_daily_airflow/user_adv_segments.sql",
    dag = dag,
    cluster_label='AIQX-uns-adv-segments'
)

t3 = QuboleOperator(
    task_id='dpi_dump_ids_new',
    command_type="hivecmd",
    script_location="s3://aiqdatabucket/scripts/aiqx/uns_qubole/aiqx_daily_airflow/dpi_dump_ids_new.sql",
    dag = dag,
    cluster_label='AIQX-uns-adv-segments'
)

t4 = QuboleOperator(
    task_id='adv_segments_counts',
    command_type="hivecmd",
    script_location="s3://aiqdatabucket/scripts/aiqx/uns_qubole/aiqx_daily_airflow/adv_segments_counts.sql",
    dag = dag,
    cluster_label='AIQX-uns-adv-segments'
)

t5 = QuboleOperator(
    task_id='all_advertiser_segment_lu',
    command_type="hivecmd",
    script_location="s3://aiqdatabucket/scripts/aiqx/uns_qubole/aiqx_daily_airflow/all_advertiser_segment_lu.sql",
    dag = dag,
    cluster_label='AIQX-uns-adv-segments'
)

t6 = QuboleOperator(
    task_id='dpi_dump_ids_daily_insert',
    command_type="hivecmd",
    script_location="s3://aiqdatabucket/scripts/aiqx/uns_qubole/aiqx_daily_airflow/dpi_dump_ids_daily_insert.sql",
    dag = dag,
    cluster_label='AIQX-uns-adv-segments'
)

t7 = QuboleOperator(
    task_id='aiqx_univ_search_adv_segments_daily_insert',
    command_type="hivecmd",
    script_location="s3://aiqdatabucket/scripts/aiqx/uns_qubole/aiqx_daily_airflow/aiqx_univ_search_adv_segments_daily_insert.sql",
    macros='[{"DAYSERIAL_NUMERIC" : "' + DAYSERIAL_NUMERIC + '"}]',
    dag = dag,
    cluster_label='AIQX-uns-adv-segments'
)

t8 = QuboleOperator(
    task_id='curl',
    command_type="shellcmd",
    script_location="s3://aiqdatabucket/scripts/aiqx/uns_qubole/aiqx_daily_airflow/curl.sh",
    parameters=DAYSERIAL_NUMERIC,
    dag = dag,
    cluster_label='AIQX-uns-adv-segments'
)

t1 >> t2 >> t3 >> [t4,t6]
t1 >> t5
[t4,t5] >> t7
[t6,t7] >> t8
