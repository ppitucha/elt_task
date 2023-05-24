--Which age band, has the highest average monthly spend (avg_3_mths_spend), in each quarter of the year, per each year
-- assuming we using join_date for quarter year calculation
with spend_q as (
    select
        date_format(join_date, 'y') as join_year,
        date_format(join_date, 'q') as join_quarter,
        age_band,
        avg(avg_3_mths_spend) as avg_spend,
        row_number() over (partition by date_format(join_date, 'y'), date_format(join_date, 'q') order by avg(avg_3_mths_spend) desc) as rank
    from input
    group by date_format(join_date, 'y'), date_format(join_date, 'q'), age_band --assuming for date we use join_date
)
select
    join_year,
    join_quarter,
    age_band as age_band_highest_spend,
    avg_spend
from spend_q
where rank = 1