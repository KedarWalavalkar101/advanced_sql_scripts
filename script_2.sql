WITH RECURSIVE dates AS (
    SELECT DATE('2023-01-31') AS date
    UNION ALL
    SELECT date + INTERVAL 1 DAY FROM dates WHERE date < DATE('2023-02-14')
)
,dates_tbl as (
SELECT * FROM dates
)
,all_accounts as (
select distinct account_no
from `2023_w10_account_statements`
)
,master_tbl as (
select *
from dates_tbl cross join all_accounts
)
,agg_table as (
select *
,str_to_date(balance_dt,'%d/%m/%Y') as dt
,count(account_no) over(partition by account_no,str_to_date(balance_dt,'%d/%m/%Y')) as cnt
,row_number() over(partition by account_no,str_to_date(balance_dt,'%d/%m/%Y')) as rn
,last_value(balance) over(partition by account_no,str_to_date(balance_dt,'%d/%m/%Y')) as last_val
 from `2023_w10_account_statements`
)
,agg_tbl as (
select *
from agg_table 
where balance = last_val
)
,final_tbl as (
select account_no, dt, transaction_value, balance as balance_col,  cnt, rn, last_val from agg_tbl
)
, joined_tbl as (
select m.account_no, m.date, transaction_value, balance_col
,sum(case when f.balance_col is not null then 1 else 0 end) over(partition by m.account_no order by m.date) as parts
from master_tbl m
left join final_tbl f on m.account_no = f.account_no and m.date = f.dt
)
,fin as (
select *
,max(balance_col) over(partition by account_no, parts) as bal
from joined_tbl
)
select account_no, bal as `balance`, transaction_value 
from fin
where date = '2023-02-01';