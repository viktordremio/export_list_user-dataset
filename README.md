# export_list_user-dataset

1.  Create VDS in your Dremio Environment

```sql
select \* from
(select REPLACE(CONCAT(REPLACE(TABLE_SCHEMA, '.', '/'), '/', TABLE_NAME), ' ', '%20') as vdss from INFORMATION_SCHEMA."TABLES"
--WHERE TABLE_TYPE IN ('VIEW')
)
where "LEFT"(vdss,1) != ('@')
```

2.  run python script src/export_list_of_user_per_dataset.py
