select
    msisdn,
    subscr_id,
    email_address,
    case
        when age < 18 then '<18'
        when age >= 18 and age <= 25 then '18-25'
        when age >= 26 and age <= 35 then '26-35'
        when age >= 36 and age <= 45 then '36-45'
        when age > 45 then '45'
    end as age_band,
    age,
    case
        when gender = 'male' then 'M'
        when gender = 'female' then 'F'
        else 'U'
    end as gender,
    date_format(join_date, 'd') as j_day,
    date_format(join_date, 'M') as j_month,
    date_format(join_date, 'y') as j_year,
    join_date,
    postal_sector,
    handset_model,
    handset_manufacturer,
    needs_segment_name,
    smart_phone_ind,
    operating_system_name,
    lte_subscr_ind,
    bill_cycle_day,
    avg_3_mths_spend,
    avg_3_mths_calls_usage,
    avg_3_mths_sms_usage,
    avg_3_mths_data_usage,
    avg_3_mths_intl_calls_usage,
    avg_3_mths_roam_calls_usage,
    avg_3_mths_roam_sms_usage,
    avg_3_mths_roam_data_usage,
    case
        when avg_3_mths_spend is not null and avg_3_mths_spend <> 0 then 'Y'
        when avg_3_mths_calls_usage is not null and avg_3_mths_calls_usage <> 0 then 'Y'
        when avg_3_mths_sms_usage is not null and avg_3_mths_sms_usage <> 0 then 'Y'
        when avg_3_mths_data_usage is not null and avg_3_mths_data_usage <> 0 then 'Y'
        when avg_3_mths_intl_calls_usage is not null and avg_3_mths_intl_calls_usage <> 0 then 'Y'
        when avg_3_mths_roam_calls_usage is not null and avg_3_mths_roam_calls_usage <> 0 then 'Y'
        when avg_3_mths_roam_sms_usage is not null and avg_3_mths_roam_sms_usage <> 0 then 'Y'
        when avg_3_mths_roam_data_usage is not null and avg_3_mths_roam_data_usage <> 0 then 'Y'
    else 'N'
    end as avg_available,
    data_bolton_ind,
    insurance_bolton_ind,
    o2travel_optin_ind,
    pm_registered_ind,
    connection_dt,
    contract_start_dt,
    contract_end_dt,
    contract_term_mths,
    upgrade_dt,
    cust_tenure_mths,
    pay_and_go_migrated_ind,
    pay_and_go_migrated_dt,
    ported_in_ind,
    ported_in_dt,
    ported_in_from_netwk_name,
    disconnection_dt,
    tariff_name,
    sim_only_ind,
    acquisition_channel_name,
    billing_system_name,
    last_billing_date,
    event_desc,
    contact_event_type_cd,
    event_start_dt,
    campaign_cd,
    texts_optin_ind,
    email_optin_ind,
    phone_optin_ind,
    post_optin_ind,
    all_marketing_optin_ind
from source