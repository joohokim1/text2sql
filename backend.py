import os
import anthropic
from google.cloud import bigquery

def execute_query_and_get_results(sql_query):
    try:
        # Google Cloud BigQuery 클라이언트 설정
        key_path = os.path.expanduser("~/bigquery_key.json")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
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
        api_key=os.getenv('ANTHROPIC_API_KEY')
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
    # print(message.content[0].text)
    # print(message.usage)
    return message.content[0].text

