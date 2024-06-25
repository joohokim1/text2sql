-- 앱 실행 시 자동으로 아래의 필수 테이블이 생성되므로 해당 내용은 스키마 참고용으로 활용할 것
CREATE TABLE dataset (
  ds_id INTEGER NOT NULL OPTIONS(description = "데이터셋 ID"),
  ds_name STRING NOT NULL OPTIONS(description = "데이터셋명"),
  ds_type STRING NOT NULL OPTIONS(description = "데이터셋 타입(Imported or Wrangled)"),
  table_name STRING OPTIONS(description = "원본 테이블명"),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
CREATE TABLE dataset_dataflow (
  id INTEGER NOT NULL OPTIONS(description = "ID"),
  ds_id INTEGER NOT NULL OPTIONS(description = "데이터셋 ID"),
  df_id INTEGER NOT NULL OPTIONS(description = "데이터플로우 ID"),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
CREATE TABLE dataflow (
  df_id INTEGER NOT NULL OPTIONS(description = "데이터플로우 ID"),
  df_name STRING NOT NULL OPTIONS(description = "데이터플로우명"),
  desc STRING OPTIONS(description = "데이터플로우 설명"),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
CREATE TABLE rule (
  rule_id INTEGER NOT NULL OPTIONS(description = "룰 ID"),
  ds_id INTEGER NOT NULL OPTIONS(description = "데이터셋 ID"),
  user_question STRING NOT NULL OPTIONS(description = "사용자 질의 내용"),
  result_sql STRING OPTIONS(description = "생성된 SQL"),
  result_table_name STRING OPTIONS(description = "result_sql CTAS로 생성된 테이블명"),
  applied_yn STRING OPTIONS(description = "적용 여부"),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);