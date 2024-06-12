import os
import anthropic
from google.cloud import bigquery

current_dir = os.path.dirname(os.path.abspath(__file__))

anthropic_key_path = os.path.join(current_dir, 'anthropic_key.txt')
with open (anthropic_key_path, 'r') as anthropic_key_file:
    anthropic_key = anthropic_key_file.read()

bigquery_key_path = os.path.join(current_dir, "bigquery_key.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = bigquery_key_path

def execute_query_and_get_results(sql_query):
    try:
        # Google Cloud BigQuery 클라이언트 설정
        client = bigquery.Client()

        print('Connected to BigQuery!')

        # 쿼리 실행
        print(f"쿼리 실행 : {sql_query}")
        query_job = client.query(sql_query)
        results = query_job.result()
        
        # 결과 가져오기
        columns = [field.name for field in results.schema]
        rows = [list(row.values()) for row in results]
        return columns, rows

    except Exception as e:
        print(f"Error: {e}")
        return None, None

    finally:
        print('BigQuery connection closed')

def get_sql_query_from_claude(natural_language_query, context=None):

    client = anthropic.Anthropic(
        api_key=anthropic_key
    )

    message = client.messages.create(
        # model="claude-3-haiku-20240307",
        model="claude-3-sonnet-20240229",
        # model="claude-3-opus-20240229",
        system="너는 Google BigQuery 전문가야. 답변은 부연설명 없이 개행문자가 포함되지 않은 SQL 형태로 답변해줘. 컬럼명은 항상 영문으로 설정하고, 지시하지 않은 타입 변환이나 치환은 하지마. Let's think step by step.",
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

