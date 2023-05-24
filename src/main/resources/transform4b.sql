--In which month were the most subscribers connected (join_date), and in which month were the fewest?
--assuming we took only month regardless of year and want this result in one row
with join_m_count as (
    select
        j_month, --assuming we take only month without year if not there should be added j_year to group by
        count(subscr_id) as cnt
    from input
    group by j_month
)
select
    (select j_month as most_subscribers_month from (select *, row_number() over (order by cnt desc) as _rank from join_m_count) where _rank = 1) as most_subscribers_month,
    (select j_month as fewest_subscribers_month from (select *, row_number() over (order by cnt) as _rank from join_m_count) where _rank = 1) as fewest_subscribers_month
