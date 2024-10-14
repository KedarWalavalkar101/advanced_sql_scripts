
-- 2023 wek 41 solution


with cte as (
select nat.student_id
,nam.name as student_name
,nat.classroom
,case when lower(left(nationality,2)) = 'br' then 'Brazil' 
 when lower(left(nationality,2)) = 'ca' then 'Canada' 
 when lower(left(nationality,2)) = 'ch' then 'China' 
 when lower(left(nationality,2)) = 'eg' then 'Egypt' 
 when lower(left(nationality,2)) = 'fr' then 'France' 
 when lower(left(nationality,2)) = 'ge' then 'Germany'
 when lower(left(nationality,2)) = 'it' then 'Italy'
 when lower(left(nationality,2)) = 'me' then 'Mexico' 
 when lower(left(nationality,2)) = 'so' then 'South Korea' 
 when lower(left(nationality,2)) = 'sp' then 'Spain' 
 when lower(left(nationality,2)) = 'us' then 'USA' 
end nationality  
from `2023_w41_student_nationality` nat
join `2023_w41_student_names` nam
on nam.student_id = nat.student_id 
)
,cte2 as (
select classroom
,nationality
,count(student_id) as cnt_of_students
,rank() over(partition by classroom order by count(student_id) desc) as rn
from cte
group by classroom
,nationality
)
select classroom, nationality, cnt_of_students
from cte2 where rn = 1;
