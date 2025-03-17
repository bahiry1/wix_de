delete
from {dest_table}{table_suffix}
where (date between '{start_date}' and '{end_date}');

insert into {dest_table}{table_suffix}
select * from {stock_data_stg}{table_suffix}
where (date between '{start_date}' and '{end_date}');


