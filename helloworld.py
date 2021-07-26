from datetime import timedelta,datetime
from airflow.models import DAG
#from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
from pandas import json_normalize

#he
def _processing_user(task_instance):
        #xcom_pull is used to share the data b/w the tasks
        ans=task_instance.xcom_pull(task_ids=['extracting_user'])
        #process the data and save it as a csv,
        print(ans)
        if not len(ans) or 'results' not in ans[0]:
                raise ValueError('User is Empty')
        an=ans[0].get('results')[0]
        first=an.get("name").get("first")
        last=an.get("name").get("last")
        city=an.get("location").get("city")
        state=an.get("location").get("state")
        country=an.get("location").get("country")        
        username=an.get("login").get("username")
        password=an.get("login").get("password")
        email = an.get("email")
        df=json_normalize({
                'first':first,
                'last':last,
                'email':email,
                'city':city,
                'country':country,
                'state':state,
                'username':username,
                'password':password
        })

        df.to_csv('/mnt/c/dags/output.csv',index=False)

default_args={
        'owner':'airflow',
        'start_date': datetime(2021,5,10),
        #'depends_on_past':False,
        #'email':['mridulpant2010@gmail.com'],
        #'email_on_failure': 'True',
        'retries':1,
        'retry_delay':timedelta(minutes=1),
        }


dag=DAG('FirstProcessing',default_args=default_args,description='Example DAG 0',schedule_interval='@once',)

create_table= PostgresOperator(task_id='create_table',sql=''' 
               create table if not exists new_table(custom_id integer NOT NULL, timestamp TIMESTAMP not null, 
               user_id varchar(50) not null); 
                ''',
                postgres_conn_id='postgres_default',
                dag=dag,
                )
is_api_available = HttpSensor(task_id='is_api_available',http_conn_id='user_api',endpoint='api/',dag=dag,)
#let us create one more task where we have to get the data from the random api
extracting_user = SimpleHttpOperator(task_id='extracting_user',method='GET',http_conn_id='user_api',response_filter=lambda response: json.loads(response.text),endpoint='api/',log_response=True,dag=dag,)


#let us process the above task using the PythonOperator
processing_user = PythonOperator(task_id='processing_user',python_callable=_processing_user,dag=dag,)




'''
t1 = BashOperator(task_id='create_file',bash_command='touch ~/Greet.txt',dag=dag,)
t2= BashOperator(task_id='write_to_file',bash_command='echo "Hello World" > ~/Greet.txt',dag=dag,)
t3= BashOperator(task_id='read_from_file',bash_operator='cat Greet.txt',dag=dag,)

t1>>t2>>t3
'''

