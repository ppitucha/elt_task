--Which tariff (tariff_name) is the most profitable for operator, considering calls, sms and data usage separately, per year. Use avg_3_mths_*usage columns.
--assuming we need statistic per years for tariffs call, sms and data separately
with call_y as (
    select
        date_format(join_date, 'y') as join_year,
        tariff_name,
        avg(avg_3_mths_calls_usage + avg_3_mths_intl_calls_usage + avg_3_mths_roam_calls_usage) as avg_spend, --assuming for most profitable we use avg for category usage (not clearly defined)
        row_number() over (partition by date_format(join_date, 'y') order by avg(avg_3_mths_calls_usage + avg_3_mths_intl_calls_usage + avg_3_mths_roam_calls_usage) desc) as rank
    from input
    group by date_format(join_date, 'y'), tariff_name --assuming for date we use join_date
),
sms_y as (
    select
        date_format(join_date, 'y') as join_year,
        tariff_name,
        avg(avg_3_mths_sms_usage + avg_3_mths_roam_sms_usage) as avg_spend,
        row_number() over (partition by date_format(join_date, 'y') order by avg(avg_3_mths_sms_usage + avg_3_mths_roam_sms_usage) desc) as rank
    from input
    group by date_format(join_date, 'y'), tariff_name --assuming for date we use join_date
),
 data_y as (
     select
         date_format(join_date, 'y') as join_year,
         tariff_name,
         avg(avg_3_mths_data_usage + avg_3_mths_roam_data_usage) as avg_spend,
         row_number() over (partition by date_format(join_date, 'y') order by avg(avg_3_mths_data_usage + avg_3_mths_roam_data_usage) desc) as rank
     from input
     group by date_format(join_date, 'y'), tariff_name --assuming for date we use join_date
 )
select
    call_y.join_year,
    call_y.tariff_name as call_tariff_most_profitable,
    sms_y.tariff_name as sms_tariff_most_profitable,
    data_y.tariff_name as data_tariff_most_profitable
from call_y
join sms_y on sms_y.join_year = call_y.join_year
join data_y on data_y.join_year = call_y.join_year
where call_y.rank = 1
and sms_y.rank = 1
and data_y.rank = 1
