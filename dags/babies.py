#import logging
#import os
import sys
sys.path.append('/opt/airflow/utils') #TODO move to compose

from database_connector import DatabaseConnector
from downloader import Downloader
from transformator import Transformator

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


def download_hashes():
    dowloader = Downloader()
    dowloader.download_from_kaggle('kaggle/us-baby-names', 'hashes.txt', None)
def download_national_names():
    dowloader = Downloader()
    dowloader.download_from_kaggle('kaggle/us-baby-names', "NationalNames.csv", 'hashes.txt')
def download_state_names():
    dowloader = Downloader()
    dowloader.download_from_kaggle('kaggle/us-baby-names', "StateNames.csv", 'hashes.txt')
def download_jewish_names():
    dowloader = Downloader()
    dowloader.download_from_kaggle('netanel246/jewish-baby-names', "data.csv", None)

def transform_state_names():

    transformator = Transformator('StateNames')
    #posible preprocess on transformator.df
    #posible tests on transformator.df
    transformator.save_to_db()
    sql = """
        insert into  "d_Names" ("Name")
        select 
            distinct "Name"
        from  "raw_StateNames" r_statenames
        where not exists
            (select 1
             from "d_Names" d_names
             where r_statenames."Name" = d_names."Name")
        ;
       
        insert into  "f_NameCounts" ("NameId", "Date", "GenderId", "StateId", "Count")
        select
            names."Id",
            ('01-01-'||"Year")::date,
            gendres."Id",
            states."Id",
            "Count"
        from "raw_StateNames" r_statenames
        join "d_Names" names on r_statenames."Name" = names."Name"
        join "d_States" states on r_statenames."State" = states."State"
        join "d_Genders" gendres on r_statenames."Gender" = gendres."Gender"::varchar
        where not exists
            (select 1
             from "f_NameCounts" f_NameCounts
             where
                     r_statenames."Name" =  names."Name" and
                     r_statenames."State" =  states."State" and
                     ('01-01-'||"Year")::date =  f_NameCounts."Date" and
                     r_statenames."Gender" =  gendres."Gender"::varchar);"""
    transformator.transform(sql)


def transform_national_names():
    transformator = Transformator('NationalNames')
    # posible preprocess on transformator.df
    # posible tests on transformator.df
    transformator.save_to_db()
    sql = """
    insert into  "f_NameCounts" ("NameId", "Date", "GenderId", "StateId", "Count")
    select * from(
        with raw_national_counts as
        (
            select
                names."Id" "NameId",
                ('01-01-'||"Year")::date "Date",
                case
                    when "Gender" = 'M' then 0
                    when "Gender" = 'F' then 1
                    else 2
                    end as "GenderId",
                sum("Count") "Count"
            from "raw_NationalNames" raw_national_names
             join "d_Names" names on raw_national_names."Name" = names."Name"
            group by 1,2,3
            ),
        loaded_state_counts as
        (
            select
                "NameId",
                "Date",
                "GenderId",
                sum("Count") "Count"
            from "f_NameCounts" f_NamesCount
            group by 1,2,3
            )
        select
            raw_national_counts."NameId",
            raw_national_counts."Date",
            raw_national_counts. "GenderId",
            0 "StateId",
            raw_national_counts."Count" - coalesce(loaded_state_counts."Count",0)
        from raw_national_counts
        left join loaded_state_counts on
            raw_national_counts."NameId" =  loaded_state_counts."NameId" and
            raw_national_counts."Date" =  loaded_state_counts."Date" and
            raw_national_counts."GenderId" =  loaded_state_counts."GenderId"
        where loaded_state_counts."Count" is null
        )sub_to_load
    where not exists
        (select 1
         from "f_NameCounts" f_NameCounts
         where
            sub_to_load."NameId" =  f_NameCounts."NameId" and
            sub_to_load."StateId" =  f_NameCounts."StateId" and
            sub_to_load."Date" =  f_NameCounts."Date" and
            sub_to_load."GenderId" =  f_NameCounts."GenderId")
    ;"""
    transformator.transform(sql)

def clean_up():
    connector = DatabaseConnector()
    connector.clean_up()

def transform_jewish_names():
    transformator = Transformator('data')
    # posible preprocess on transformator.df
    # posible tests on transformator.df
    transformator.save_to_db()
    sql = """
    update "d_Names"
    set "Origin" = 'Jewish'
    where "Name" in (
        select distinct "Name" from raw_data);"""
    transformator.transform(sql)


args = {

    'owner': 'michal',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='babies', default_args=args, schedule_interval=None)

with dag:
    download_hashes = PythonOperator(
        task_id='download_hashes',
        python_callable=download_hashes
    )
    download_national_names = PythonOperator(
        task_id='download_national_names',
        python_callable=download_national_names
    )
    download_state_names = PythonOperator(
        task_id='download_state_names',
        python_callable=download_state_names
    )
    download_jewish_names = PythonOperator(
        task_id='download_jewish_names',
        python_callable=download_jewish_names
    )
    transform_national_names = PythonOperator(
        task_id='transform_national_names',
        python_callable=transform_national_names
    )
    transform_state_names = PythonOperator(
        task_id='transform_state_names',
        python_callable=transform_state_names
    )
    transform_jewish_names = PythonOperator(
        task_id='transform_jewish_names',
        python_callable=transform_jewish_names
    )
    clean_up = PythonOperator(
        task_id='clean_up',
        python_callable=clean_up
    )
    download_hashes >> download_national_names >> transform_state_names
    download_hashes >> download_state_names >> transform_state_names >> transform_national_names >> transform_jewish_names >> clean_up
    download_hashes >> download_jewish_names >> transform_jewish_names

