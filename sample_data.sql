INSERT INTO metatron.dataset (
    ds_id,
    ds_name,
    ds_type,
    table_name,
    created_at,
    updated_at
  )
VALUES(
    1,
    '테스트_데이터소스',
    'Imported',
    'adot_applog_prd_all',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );