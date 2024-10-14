
-- 2024 Week 9 Solution


with filtered as (
select *
from (
	select *
,sum(case when action = 'Cancelled' then 1 else 0 end) over(partition by customer_id, flight_no) as flg
from w9_cust_actions
) x
where flg = 0
)
, ranked as (
select *
,str_to_date(dt,'%d/%m/%Y') as action_dt
,row_number() over(partition by customer_id, flight_no order by str_to_date(dt,'%d/%m/%Y') desc) as rn
from filtered
)
,seats_booked as (
select *
,sum(1) over(partition by flight_no, class order by action_dt asc) as running_sum
from ranked
where rn = 1
)
select fd.flight_no , fd.flight_dt, fd.class, coalesce(b.running_sum,'No Bookings') as seats_booked_over_time
,fd.capacity
,coalesce((running_sum*1.0/capacity*10),0) as capacity_perc
,b.customer_id
,b.`action`
,coalesce(b.action_dt,'2024-02-28') as dt
,b.`row`, b.seat
from seats_booked b
right join w9_flight_details fd
on fd.flight_no = b.flight_no and fd.class = b.class
order by fd.flight_no , fd.class, coalesce(b.action_dt,'2024-02-28');
