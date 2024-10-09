WITH RECURSIVE dates_cte AS (
    SELECT DATE '2023-01-01' AS date
    UNION ALL
    SELECT date + INTERVAL 1 DAY
    FROM dates_cte
    WHERE date < '2024-12-31'
)
,dates_dim as (
select * from dates_cte
)
,filtered_trans_data as (
select str_to_date(transaction_dt,'%a, %M %d, %Y') as transaction_dt , transaction_no
,substring_index(product_id,'-',1) as product_type
,replace(substring_index(substring_index(product_id,'-',2),'-',-1),'_',' ') as product_scent
,substring_index(product_id,'-',-1) as product_size
,case when cash_or_card = '1' then 'Card' when cash_or_card = '2' then 'Cash' end as cash_or_card, 
loyalty_no, sales_before_discount
from `2024_w10_transactions`
where year(str_to_date(transaction_dt,'%a, %M %d, %Y')) in ('2023','2024')
)
,product_tbl as (
select product_type, product_scent, 
case when pack_size = '' then null else pack_size end as a, 
case when product_size = '' then null else product_size end as b, unit_cost, selling_price
from `2024_w10_products`
)
,new_product_tbl as (
select product_type, product_scent, coalesce(a,b) as size,  unit_cost, selling_price
from product_tbl
)
,updated_loyalty_tbl as (
select loyalty_no
,concat(concat(upper(substr(substring_index(cust_name,' ',-1),1,1)),lower(substr(substring_index(cust_name,' ',-1),2))),' ',
concat(upper(substr(substring_index(cust_name,',',1),1,1)),lower(substr(substring_index(cust_name,',',1),2)))) as cust_name
,case when upper(loyalty_tier) like 'B%' then 'Bronze'
	 when upper(loyalty_tier) like 'S%' then 'Silver'
      when upper(loyalty_tier) like 'G%' then 'Gold' end as loyalty_tier
,case when substring_index(loyalty_discount,'%',1) = '' then null else substring_index(loyalty_discount,'%',1) end as loyalty_discount
from `2024_w10_loyalty`
)
,joined_data as (
select d.date as transaction_date
,min(transaction_dt) over() as min_dt
,max('2024-03-06') over() as max_dt
, transaction_no
,td.product_type
,td.product_scent
,cash_or_card, 
c.loyalty_no, sales_before_discount
, p.size, unit_cost ,selling_price
,sales_before_discount / selling_price as quantity
,cust_name, loyalty_tier, loyalty_discount
from filtered_trans_data td
join new_product_tbl p
on p.product_type = td.product_type and p.product_scent = td.product_scent and td.product_size = p.size
right join dates_dim d on d.date = td.transaction_dt
left join updated_loyalty_tbl c on c.loyalty_no = td.loyalty_no 
)
,final_data as (
select *
,round((sales_before_discount*(1-(loyalty_discount/100))),3) as sales_after_discount
,(round((sales_before_discount*(1-(loyalty_discount/100))),3) - (unit_cost * quantity)) as profit
from joined_data
where transaction_date >= min_dt and transaction_date <= max_dt
)
select transaction_date as `Transaction Date`
,transaction_no as `Transaction Number`
,product_type as `Product Type`
,product_scent as `Product Scent`
,size as `Product Size`
,cash_or_card as `Cash or Card`
,loyalty_no as `Loyalty Number`
,cust_name as `Customer Name`
,loyalty_tier as `Loyalty Tier`
,loyalty_discount as `Loyalty Discount`
,quantity as `Quantity`
,sales_before_discount as `Sales Before Discount`
,sales_after_discount as `Sales After Discount`
,profit as `Profit`
from final_data;