-- В якому штаті було куплено найбільше телевізорів покупцями від 20 до 30 років за першу декаду серпня?
-- Мабуть Вересня, бо за Серпень нема даних.
select 
  upe.state,
  count(*) as nmb_sales_per_state
from `silver.sales` s
left join `gold.user_profiles_enriched` upe
  on s.client_id = upe.client_id
where s.purchase_date between '2022-09-01' and '2022-09-10'
  and s.product_name = 'TV'
  and upe.birth_date between 
    date_add(current_date('UTC'), interval -30 YEAR) 
    and 
    date_add(current_date('UTC'), interval -20 YEAR)
group by upe.state
order by nmb_sales_per_state desc
limit 2
;

-- Result:
-- [{
--   "state": "Idaho",
--   "nmb_sales_per_state": "161"
-- }, {
--   "state": "Iowa",
--   "nmb_sales_per_state": "161"
-- }]
