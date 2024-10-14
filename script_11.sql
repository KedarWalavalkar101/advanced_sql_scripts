

-- 2023 week 21 solution


with cte as (
select student_id
,first_name
,last_name
,gender
,dob 
,'2021' as `year`
,'attainment' as `type`, 2021_attainment as marks
from `2023_w21_input`
union
select student_id
,first_name
,last_name
,gender
,dob 
,'2021' as `year`
,'effort' as `type`, 2021_effort as marks
from `2023_w21_input`
union
select student_id
,first_name
,last_name
,gender
,dob 
,'2021' as `year`
,'attendence' as `type`, 2021_attendence as marks
from `2023_w21_input`
union
select student_id
,first_name
,last_name
,gender
,dob 
,'2021' as `year`
,'behaviour' as `type`, 2021_behaviour as marks
from `2023_w21_input`
union
select student_id
,first_name
,last_name
,gender
,dob 
,'2022' as `year`
,'attainment' as `type`, 2022_attainment as marks
from `2023_w21_input`
union
select student_id
,first_name
,last_name
,gender
,dob 
,'2022' as `year`
,'effort' as `type`, 2022_effort as marks
from `2023_w21_input`
union
select student_id
,first_name
,last_name
,gender
,dob 
,'2022' as `year`
,'attendence' as `type`, 2022_attendence as marks
from `2023_w21_input`
union
select student_id
,first_name
,last_name
,gender
,dob 
,'2022' as `year`
,'behaviour' as `type`, 2022_behaviour as marks
from `2023_w21_input`
)
,cte3 as (
select student_id
,first_name
,last_name
,gender
,dob
,`type`
,sum(case when year = '2021' then marks else null end) as `2021`
,sum(case when year = '2022' then marks else null end) as `2022`
from cte
group by student_id
,first_name
,last_name
,gender
,dob
,`type`
)
,student_avg_marks as (
select student_id
,first_name
,last_name
,gender
,dob
,avg(`2021`) as `2021_avg_marks`
,avg(`2022`) as `2022_avg_marks`
,avg(`2022`) - avg(`2021`) as diff
,case when avg(`2022`) - avg(`2021`) > 0 then 'Improvment'
	when  avg(`2022`) - avg(`2021`) = 0 then 'No Change'
    when  avg(`2022`) - avg(`2021`) < 0 then 'Cause For Concern' end as progress
from cte3
group by student_id
,first_name
,last_name
,gender
,dob
)
select * 
from student_avg_marks
where progress = 'Cause For Concern';