--The operator needs to create an e-mail advertising campaign for a specified group of customers. Select subscribers, which are younger than 30, has an active contract (contract_end_dt) and agreed to advertising via email (email_optin_ind).
--assuming we use current_data to check contrackt_end_dt - it might be also external parameter (checking for campaign date)
select
    msisdn,
    subscr_id,
    email_address
from input
where age < 30
and contract_end_dt > current_date()
and email_optin_ind = 'Y'
--and email_address is not null --not specified but it makes sense