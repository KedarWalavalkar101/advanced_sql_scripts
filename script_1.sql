
with exploded_account_info_tbl_cte as (
SELECT  account_no, account_type,
    trim(SUBSTRING_INDEX(SUBSTRING_INDEX(t.account_holder_id, ',', n.n), ',', -1)) AS account_holder_id,
    balance_dt, balance
FROM
    `2023_w7_account_info` t
    JOIN (
        SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10
    ) n ON LENGTH(t.account_holder_id) - LENGTH(REPLACE(t.account_holder_id, ',', '')) >= n.n - 1
)
,exploded_account_info_tbl as (
select *,
 case when length(account_holder_id) = 7 then concat('0',account_holder_id)
else account_holder_id end as account_holder_id_final
from exploded_account_info_tbl_cte
)
,updated_account_holders_tbl as (
select case when length(account_holder_id) = 7 then concat('0',account_holder_id)
	when length(account_holder_id) = 6 then concat('00',account_holder_id) else account_holder_id end as account_holder_id
, name, dob, concat('0',contact_no) as contact_no, first_line_of_add
from `2023_w7_account_holders`
)
,trans_data as (
select td.*, account_to, account_from
from `2023_w7_transaction_details` td
join `2023_w7_transaction_path` tp on tp.transaction_id = td.transaction_id
)
,joined_data as (
select acc_hol.*, acc_info.account_no, acc_info.account_type, acc_info.balance_dt, acc_info.balance
,t.*
from updated_account_holders_tbl acc_hol
join exploded_account_info_tbl acc_info on acc_info.account_holder_id_final = acc_hol.account_holder_id
join trans_data t on t.account_from = acc_info.account_no
-- where acc_hol.account_holder_id is not null
)
select transaction_id
,account_to
,transaction_dt
,value
,account_no
,account_type
,balance_dt
,balance
,name
,dob
,contact_no
,first_line_of_add
from joined_data
where account_type <> 'Platinum' and value > 1000 and is_cancelled = 'N';