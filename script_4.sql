
-- 2023 week 36 solution


with cte as (
select *,
str_to_date(tournament_dt,'%d/%m/%Y') as dt
-- ,sum(matches_played_in_tournament) over(partition by player_id order by  str_to_date(tournament_dt,'%d/%m/%Y')) as summ
-- ,sum(matches_played_in_tournament) over(partition by player_id rows between unbounded preceding and unbounded following)
,sum(matches_played_in_tournament) 
	over(partition by player_id, team order by str_to_date(tournament_dt,'%d/%m/%Y') rows between current row and unbounded following) as rs
from `2023_w36_input`
order by player_id, str_to_date(tournament_dt,'%d/%m/%Y')
)
select team, player_id, total_matches, date_format(dt,'%d/%m/%Y') as tournament_date
,(total_matches - rs) as experience_at_beginning_of_tournament
,matches_played_in_tournament
from cte
where 1=1;
