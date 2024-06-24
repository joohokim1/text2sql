import os
from datetime import datetime
import anthropic
from google.cloud import bigquery
import pandas as pd
import config

current_dir = os.path.dirname(os.path.abspath(__file__))

anthropic_key_path = os.path.join(current_dir, config.get_config('anthropic.key.file'))
with open (anthropic_key_path, 'r') as anthropic_key_file:
    anthropic_key = anthropic_key_file.read()

bigquery_key_path = os.path.join(current_dir, config.get_config('bigquery.key.file'))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = bigquery_key_path

def on_app_start():
    print("Check required table exists...")
    dataset_id = "metatron"
    required_table = [
        {
            "id": "dataset",
            "schema": [
                bigquery.SchemaField("ds_id", "INTEGER", mode="REQUIRED", description="데이터셋 ID"),
                bigquery.SchemaField("ds_name", "STRING", mode="REQUIRED", description="데이터셋명"),
                bigquery.SchemaField("ds_type", "STRING", mode="REQUIRED", description="데이터셋 타입(Imported or Wrangled)"),
                bigquery.SchemaField("table_name", "STRING", mode="NULLABLE", description="원본 테이블명"),
                bigquery.SchemaField("created_at", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "DATE", mode="REQUIRED"),
            ]
        },
        {
            "id": "dataset_dataflow",
            "schema": [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED", description="ID"),
                bigquery.SchemaField("ds_id", "INTEGER", mode="REQUIRED", description="데이터셋 ID"),
                bigquery.SchemaField("df_id", "INTEGER", mode="REQUIRED", description="데이터플로우 ID"),
                bigquery.SchemaField("created_at", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "DATE", mode="REQUIRED"),
            ]
        },
        {
            "id": "dataflow",
            "schema": [
                bigquery.SchemaField("df_id", "INTEGER", mode="REQUIRED", description="데이터플로우 ID"),
                bigquery.SchemaField("df_name", "STRING", mode="REQUIRED", description="데이터플로우명"),
                bigquery.SchemaField("desc", "STRING", mode="NULLABLE", description="데이터플로우 설명"),
                bigquery.SchemaField("created_at", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "DATE", mode="REQUIRED"),
            ]
        },
        {
            "id": "rule",
            "schema": [
                bigquery.SchemaField("rule_id", "INTEGER", mode="REQUIRED", description="룰 ID"),
                bigquery.SchemaField("ds_id", "INTEGER", mode="REQUIRED", description="데이터셋 ID"),
                bigquery.SchemaField("user_question", "STRING", mode="REQUIRED", description="사용자 질의 내용"),
                bigquery.SchemaField("result_sql", "STRING", mode="NULLABLE", description="생성된 SQL"), # SQL이 나오지 않는 질문을 할 경우 예외 처리 필요
                bigquery.SchemaField("result_table_name", "STRING", mode="NULLABLE", description="result_sql CTAS로 생성된 테이블명"),
                bigquery.SchemaField("applied_yn", "STRING", mode="NULLABLE", description="적용 여부"),
                bigquery.SchemaField("created_at", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "DATE", mode="REQUIRED"),
            ]
        },
    ]

    # Google Cloud BigQuery 클라이언트 설정
    client = bigquery.Client(location=config.get_config('bigquery.region'))
    print('Connected to BigQuery!')

    for table in required_table:
        try:
            # 테이블 참조 생성
            table_ref = client.dataset(dataset_id).table(table['id'])

            # 테이블이 존재하는지 확인
            client.get_table(table_ref)
            print(f"Table {table['id']} already exists.")

        except Exception:
            # 테이블이 없으면 생성
            new_table = bigquery.Table(table_ref, schema=table['schema'])
            new_table = client.create_table(new_table)
            print(f"Table {table['id']} created.")


    client.close()
    print('BigQuery connection closed')

def execute_query_and_get_results(sql_query, params=None):
    try:
        # Google Cloud BigQuery 클라이언트 설정
        client = bigquery.Client(location=config.get_config('bigquery.region'))

        print('Connected to BigQuery!')

        # 쿼리 실행
        print(f"쿼리 실행 : {sql_query}")
        job_config = bigquery.QueryJobConfig()

        if params:
            job_config.query_parameters = params

        query_job = client.query(sql_query, job_config=job_config)
        results = query_job.result()
        
        # 결과 가져오기
        columns = [field.name for field in results.schema]
        rows = [list(row.values()) for row in results]
        return columns, rows

    except Exception as e:
        print(f"Error: {e}")
        return None, None

    finally:
        client.close()
        print('BigQuery connection closed')

def get_sql_query_from_claude(natural_language_query, context=None):

    client = anthropic.Anthropic(
        api_key=anthropic_key
    )

    message = client.messages.create(
        model=config.get_config('anthropic.model'),
        system="너는 Google BigQuery 전문가야. 답변은 부연설명 없이 개행문자가 포함되지 않고 정렬된 SQL 형태로 답변해줘. 컬럼명은 항상 영문으로 설정하고, 지시하지 않은 타입 변환이나 치환과 불필요한 distinct, order by 하지마. 만약 질문 자체가 SELECT SQL문이라면, 질문 그대로 정렬된 SQL문으로 응답해줘. Let's think step by step.",
        max_tokens=1024,
        temperature=0,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"{natural_language_query}"
                    }
                ]
            }
        ]
    )
    return message.content[0].text

def insert_data(workflow_name, type, sql, question, appliedYn, table_name):
        try:
            # Google Cloud BigQuery 클라이언트 설정
            client = bigquery.Client()
            sql = sql.replace("\n", " ")
            update_date = datetime.now()


            df = pd.DataFrame({
                "workflow_name": [workflow_name],
                "type": [type],
                "sql": [sql],
                "seq": [1], # 임시
                "question": [question],
                "appliedYn": [appliedYn],
                "update": [update_date],
                "table_name": [table_name]
            })

            table_id = 'adot-412309.metatron.adot_sql_generator_log'
            table = client.get_table(table_id)
            # 데이터프레임을 테이블에 삽입
            client.load_table_from_dataframe(df, table)

        except Exception as e:
            print(f"Error: {e}")
            return None, None
        finally:
            client.close()
            print('BigQuery connection closed')