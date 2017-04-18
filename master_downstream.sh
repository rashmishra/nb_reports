v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=nb_reports;
date


# Downstream table loading. Replace existing
v_query_downstream="select
a.order_Id AS order_id
,a.orderline_id as orderline_id,
a.orderline_status as orderline_status
   ,a.vertical AS vertical
  , a.customer_id AS customer_id
  , a.date_time_ist AS date_time_ist
  , a.GB AS GB
  , a.price_after_promo AS price_after_promo
  , a.promoCode AS promocode
  , a.credits_requested AS credits_requested
  , a.platform_type AS platform_type
  , a.deal_id AS deal_id
  , a.offer_Id AS offer_id
  , a.category_id AS category_id
  , a.offer_title AS Offer_title
  , a.merchant_Name AS merchant_name
  , a.merchant_Id AS merchant_id
  , a.transaction_Id AS transaction_id
  , a.Number_of_Vouchers AS number_of_vouchers
  , a.GR AS GR
  , a.deal_owner AS deal_owner
  , a.cm_location AS cm_location
  , a.new_customer_day AS new_customer_day
  , a.new_customer_month as new_customer_month
  , a.last_purchase_date AS last_purchase_date
  , a.first_purchase_date AS first_purchase_date,
    a.cashback_amount as cashback_amount,
    a.referral_program_id as referral_program_id,
 -- , a.promo_length as promo_length, 
  a.promo_name as promo_name, 
  a.promo_description as promo_description, 
  a.promocode_type as promocode_type,
  a.promo_discount_amount as promo_discount_amount, 
  a.promo_discount_percentage as promo_discount_percentage, 
  a.promo_max_cap as promo_max_cap,
  a.promo_cashback_percentage as promo_cashback_percentage, 
  a.promo_cashback_amount as promo_cashback_amount, 
  a.promo_offer_price_range_from as promo_offer_price_range_from, 
  a.promo_is_cashback as promo_is_cashback, 
  a.promo_is_deferential as promo_is_deferential, 
  a.promo_has_user_transacted as promo_has_user_transacted 
  --a.voucher_redeemed as voucher_redeemed
  ,a.ga_channel as ga_channel
  ,a.ga_campaign_grouping as ga_campaign_grouping
  ,a.ga_campaign_name as ga_campaign_name
  ,a.ga_source as ga_source
  ,a.ga_medium as ga_medium
  ,a.ga_keyword as ga_keyword
  ,a.ga_content as ga_content
  ,a.ga_deal_id as ga_deal_id
  ,a.af_imei_number as af_imei_number
  ,a.af_attribution_type as af_attribution_type
  ,a.af_media_source as af_media_source
  ,a.af_fb_adgroup_id as af_fb_adgroup_id
  ,a.af_download_time as af_download_time
  ,a.af_fb_campaign_id as af_fb_campaign_id
  ,a.af_campaign_name  as af_campaign_name
  , a.af_install_time as af_install_time
  ,a.af_platform_type as af_platform_type
  ,a.af_fb_campaign_name  as af_fb_campaign_name
  ,a.af_app_version as af_app_version
  ,a.af_os_version as af_os_version
  ,a.af_fb_adset_id as af_fb_adset_id
  ,a.af_event_type as af_event_type
  ,a.af_ad_name as af_ad_name
  ,a.af_ad_id as af_ad_id
  ,a.af_ad_type  as af_ad_type
  ,a.af_adset_name as af_adset_name
  ,a.af_adset_id as af_adset_id
  ,a.af_campaign_id as af_campaign_id
  ,a.af_channel as af_channel
  , a.af_reaatributed_flag as af_reaatributed_flag
  ,a.af_is_retargeting as af_is_retargeting,
 a.af_is_reengagement as af_is_reengagement 
  , b.date_time_ist AS aquisition_date_time_ist
  , b.promoCode AS aquisition_promocode
  , b.platform_type AS aquisition_platform_type
  , b.deal_id AS aquisition_deal_id
  --, b.offer_Id AS aquisition_offer_id
  , b.category_id AS aquisition_category_id
  , b.merchant_Name AS aquisition_merchant_name
  , b.cm_location AS aquisition_cm_location
   --,b.cashback_amount as Aquisition_cashback_amount
   ,b.ga_channel as aquisition_ga_channel
  ,b.ga_campaign_grouping as aquisition_ga_campaign_grouping
  ,b.ga_campaign_name as aquisition_ga_campaign_name
  ,b.ga_source as aquisition_ga_source
  ,b.ga_medium as aquisition_ga_medium
  ,b.ga_keyword as aquisition_ga_keyword
  ,b.ga_content as aquisition_ga_content
  ,b.af_attribution_type as aquisition_af_attribution_type
  ,b.af_media_source as aquisition_af_media_source
  --,b.af_fb_adgroup_id as Aquisition_af_fb_adgroup_id
  ,b.af_campaign_name  as aquisition_af_campaign_name
  ,b.af_platform_type as aquisition_af_platform_type
  ,b.af_app_version as aquisition_af_app_version
  ,b.af_os_version as aquisition_af_os_version
  ,b.af_is_retargeting as aquisition_af_is_retargeting,
 b.af_is_reengagement as aquisition_af_is_reengagement
    
from

(SELECT  a.order_Id AS order_id
,a.orderline_id as orderline_id,
a.orderline_status as orderline_status
  , a.vertical AS vertical
  , a.customer_id AS customer_id
  , a.date_time_ist AS date_time_ist
  , a.GB AS GB
  , a.price_after_promo AS price_after_promo
  , a.promoCode AS promocode
  , a.credits_requested AS credits_requested
  , a.platform_type AS platform_type
  , a.deal_id AS deal_id
  , a.offer_Id AS offer_id
  , a.category_id AS category_id
  , a.offer_title AS Offer_title
  , a.merchant_Name AS merchant_name
  , a.merchant_Id AS merchant_id
  , a.transaction_Id AS transaction_id
  , a.Number_of_Vouchers AS number_of_vouchers
  , a.GR AS GR
  , a.deal_owner AS deal_owner
  , a.cm_location AS cm_location
  , a.new_customer_day AS new_customer_day
  , a.new_customer_month as new_customer_month
  , a.last_purchase_date AS last_purchase_date
  , a.first_purchase_date AS first_purchase_date,
    a.cashback_amount as cashback_amount
  , a.promo_length as promo_length, 
  a.promo_logic as promo_logic, 
  a.promo_name as promo_name, 
  a.promo_description as promo_description, 
  a.promocode_type as promocode_type,
  a.promo_discount_amount as promo_discount_amount, 
  a.promo_discount_percentage as promo_discount_percentage, 
  a.promo_max_cap as promo_max_cap,
  a.promo_cashback_percentage as promo_cashback_percentage, 
  a.promo_cashback_amount as promo_cashback_amount, 
  a.promo_offer_price_range_from as promo_offer_price_range_from, 
  a.promo_is_cashback as promo_is_cashback, 
  a.promo_is_deferential as promo_is_deferential, 
  a.promo_has_user_transacted as promo_has_user_transacted,
  
  --a.voucher_redeemed as voucher_redeemed,
  a.referral_program_id as  referral_program_id
  --,1 as rank1
  ,y.dcg_ga AS ga_channel
  ,y.campaign_grouping AS ga_campaign_grouping
  ,y.campaign_ga AS ga_campaign_name
  ,y.source_ga AS ga_source
  ,y.medium_ga AS ga_medium
  ,y.keyword_ga AS ga_keyword
  ,y.content_ga AS ga_content
  ,y.dealid_ga AS ga_deal_id
  ,ap.af_imei_number as af_imei_number
  ,ap.af_attribution_type as af_attribution_type
  ,ap.af_media_source as af_media_source
  ,ap.af_fb_adgroup_id as af_fb_adgroup_id
  ,STRING(ap.af_download_time) as af_download_time
  ,ap.af_fb_campaign_id as af_fb_campaign_id
  ,ap.af_campaign_name  as af_campaign_name
  ,STRING(ap.af_install_time)  as af_install_time
  ,ap.af_platform_type as af_platform_type
  ,ap.af_fb_campaign_name  as af_fb_campaign_name
  ,ap.af_app_version as af_app_version
  ,ap.af_os_version as af_os_version
  ,ap.af_fb_adset_id as af_fb_adset_id
  ,ap.af_event_type as af_event_type
  ,ap.af_ad_name as af_ad_name
  ,ap.af_ad_id as af_ad_id
  ,ap.af_ad_type  as af_ad_type
  ,ap.af_adset_name as af_adset_name
  ,ap.af_adset_id as af_adset_id
  ,ap.af_campaign_id as af_campaign_id
  ,ap.af_channel as af_channel
  , ap.af_reaatributed_flag as af_reaatributed_flag
  ,ap.af_is_retargeting as af_is_retargeting,
 ap.af_re_targeting_conversion_type as af_is_reengagement  
 FROM 
 nb_reports.master_transaction  a
 LEFT JOIN (
--  select
-- rank1,
-- orderid_ga,
-- dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga
-- from
-- (
-- select 
-- dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga,orderid_ga,
-- rank() over (partition by orderid_ga  order by  dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga) as rank1
-- from
-- (
SELECT
    dcg AS dcg_ga
    ,campaign_grouping 
    ,campaign AS campaign_ga
    ,source AS source_ga
    ,medium AS medium_ga
    ,keyword AS keyword_ga
    ,content AS content_ga
    ,dealid AS dealid_ga
    ,INTEGER (orderid) AS orderid_ga
  FROM nb_reports.ga_source_medium
  where orderid is not null 
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
  --)
--   where orderid_ga is not null
--   )
--  where rank1 = 1
  ) AS y ON a.order_Id = y.orderid_ga
  LEFT JOIN  
 (select * from nb_reports.downstream_appsflyer  where rank = 1) as ap on a.order_Id = ap.af_order_id 
--Group by 1, 2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,56,57,58,59,60,61,62,63,64,65,67,68,69,70,71,72,73,74,af_ad_id--,75,76,77,78
 
 ) a
 left join 
  (
 SELECT  
   a.customer_id AS customer_id,
   a.date_time_ist AS date_time_ist

  , a.promoCode AS promocode
  , a.platform_type AS platform_type
  , a.deal_id AS deal_id
  --, a.offer_Id AS offer_id
  , a.category_id AS category_id
  , a.merchant_Name AS merchant_name
  , a.cm_location AS cm_location
   -- ,a.cashback_amount as cashback_amount
  ,y.dcg_ga AS ga_channel
  ,y.campaign_grouping AS ga_campaign_grouping
  ,y.campaign_ga AS ga_campaign_name
  ,y.source_ga AS ga_source
  ,y.medium_ga AS ga_medium
  ,y.keyword_ga AS ga_keyword
  ,y.content_ga AS ga_content
  ,ap.af_attribution_type as af_attribution_type
  ,ap.af_media_source as af_media_source
  --,ap.af_fb_adgroup_id as af_fb_adgroup_id
  ,ap.af_campaign_name  as af_campaign_name
  ,ap.af_platform_type as af_platform_type
  ,ap.af_app_version as af_app_version
  ,ap.af_os_version as af_os_version
  ,ap.af_is_retargeting as af_is_retargeting,
 ap.af_re_targeting_conversion_type as af_is_reengagement  
 FROM 
 nb_reports.master_transaction  a
 LEFT JOIN (
--   select
-- rank1,
-- orderid_ga,
-- dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga
-- from
-- (
-- select 
-- dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga,orderid_ga,
-- rank() over (partition by orderid_ga  order by  dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga) as rank1
-- from
-- (
SELECT
    dcg AS dcg_ga
    ,campaign_grouping 
    ,campaign AS campaign_ga
    ,source AS source_ga
    ,medium AS medium_ga
    ,keyword AS keyword_ga
    ,content AS content_ga
    ,dealid AS dealid_ga
    ,INTEGER (orderid) AS orderid_ga
  FROM nb_reports.ga_source_medium
  where orderid is not null 
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
 -- )
 -- where orderid_ga is not null
--   )
--  where rank1 = 1
  ) AS y ON a.order_Id = y.orderid_ga
  LEFT JOIN  
 (select * from nb_reports.downstream_appsflyer where rank = 1) as ap on a.order_Id = ap.af_order_id 
where a.first_transaction = 'TRUE'
Group by 1, 2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23--, 24--, 25,26
 
 ) b on a.customer_id = b.customer_id
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=downstream
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_downstream\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_downstream" &
v_first_pid=$!
v_downstream_pids+=" $v_first_pid"
wait $v_first_pid;

# reengagement loading. Replace existing
v_query_reengagement="select
a.order_Id AS order_id
,a.orderline_id as orderline_id,
a.orderline_status as orderline_status
   ,a.vertical AS vertical
  , a.customer_id AS customer_id
  , a.date_time_ist AS date_time_ist
  , a.GB AS GB
  , a.price_after_promo AS price_after_promo
  , a.promoCode AS promocode
  , a.credits_requested AS credits_requested
  , a.platform_type AS platform_type
  , a.deal_id AS deal_id
  , a.offer_Id AS offer_id
  , a.category_id AS category_id
  , a.offer_title AS Offer_title
  , a.merchant_Name AS merchant_name
  , a.merchant_Id AS merchant_id
  , a.transaction_Id AS transaction_id
  , a.Number_of_Vouchers AS number_of_vouchers
  , a.GR AS GR
  , a.deal_owner AS deal_owner
  , a.cm_location AS cm_location
  , a.new_customer_day AS new_customer_day
  , a.new_customer_month as new_customer_month
  , a.last_purchase_date AS last_purchase_date
  , a.first_purchase_date AS first_purchase_date,
    a.cashback_amount as cashback_amount,
    a.referral_program_id as referral_program_id,
 -- , a.promo_length as promo_length, 
  a.promo_name as promo_name, 
  a.promo_description as promo_description, 
  a.promocode_type as promocode_type,
  a.promo_discount_amount as promo_discount_amount, 
  a.promo_discount_percentage as promo_discount_percentage, 
  a.promo_max_cap as promo_max_cap,
  a.promo_cashback_percentage as promo_cashback_percentage, 
  a.promo_cashback_amount as promo_cashback_amount, 
  a.promo_offer_price_range_from as promo_offer_price_range_from, 
  a.promo_is_cashback as promo_is_cashback, 
  a.promo_is_deferential as promo_is_deferential, 
  a.promo_has_user_transacted as promo_has_user_transacted  
 -- a.voucher_redeemed as voucher_redeemed
  ,a.ga_channel as ga_channel
  ,a.ga_campaign_grouping as ga_campaign_grouping
  ,a.ga_campaign_name as ga_campaign_name
  ,a.ga_source as ga_source
  ,a.ga_medium as ga_medium
  ,a.ga_keyword as ga_keyword
  ,a.ga_content as ga_content
  ,a.ga_deal_id as ga_deal_id
  ,a.af_imei_number as af_imei_number
  ,a.af_attribution_type as af_attribution_type
  ,a.af_media_source as af_media_source
  ,a.af_fb_adgroup_id as af_fb_adgroup_id
  ,a.af_download_time as af_download_time
  ,a.af_fb_campaign_id as af_fb_campaign_id
  ,a.af_campaign_name  as af_campaign_name
  , a.af_install_time as af_install_time
  ,a.af_platform_type as af_platform_type
  ,a.af_fb_campaign_name  as af_fb_campaign_name
  ,a.af_app_version as af_app_version
  ,a.af_os_version as af_os_version
  ,a.af_fb_adset_id as af_fb_adset_id
  ,a.af_event_type as af_event_type
  ,a.af_ad_name as af_ad_name
  ,a.af_ad_id as af_ad_id
  ,a.af_ad_type  as af_ad_type
  ,a.af_adset_name as af_adset_name
  ,a.af_adset_id as af_adset_id
  ,a.af_campaign_id as af_campaign_id
  ,a.af_channel as af_channel
  , a.af_reaatributed_flag as af_reaatributed_flag
  ,a.af_is_retargeting as af_is_retargeting,
 a.af_is_reengagement as af_is_reengagement 
  , b.date_time_ist AS aquisition_date_time_ist
  , b.promoCode AS aquisition_promocode
  , b.platform_type AS aquisition_platform_type
  , b.deal_id AS aquisition_deal_id
  --, b.offer_Id AS aquisition_offer_id
  , b.category_id AS aquisition_category_id
  , b.merchant_Name AS aquisition_merchant_name
  , b.cm_location AS aquisition_cm_location
   --,b.cashback_amount as Aquisition_cashback_amount
   ,b.ga_channel as aquisition_ga_channel
  ,b.ga_campaign_grouping as aquisition_ga_campaign_grouping
  ,b.ga_campaign_name as aquisition_ga_campaign_name
  ,b.ga_source as aquisition_ga_source
  ,b.ga_medium as aquisition_ga_medium
  ,b.ga_keyword as aquisition_ga_keyword
  ,b.ga_content as aquisition_ga_content
  ,b.af_attribution_type as aquisition_af_attribution_type
  ,b.af_media_source as aquisition_af_media_source
  --,b.af_fb_adgroup_id as Aquisition_af_fb_adgroup_id
  ,b.af_campaign_name  as aquisition_af_campaign_name
  ,b.af_platform_type as aquisition_af_platform_type
  ,b.af_app_version as aquisition_af_app_version
  ,b.af_os_version as aquisition_af_os_version
  ,b.af_is_retargeting as aquisition_af_is_retargeting,
 b.af_is_reengagement as aquisition_af_is_reengagement
    
from

(SELECT  a.order_Id AS order_id
,a.orderline_id as orderline_id,
a.orderline_status as orderline_status
  , a.vertical AS vertical
  , a.customer_id AS customer_id
  , a.date_time_ist AS date_time_ist
  , a.GB AS GB
  , a.price_after_promo AS price_after_promo
  , a.promoCode AS promocode
  , a.credits_requested AS credits_requested
  , a.platform_type AS platform_type
  , a.deal_id AS deal_id
  , a.offer_Id AS offer_id
  , a.category_id AS category_id
  , a.offer_title AS Offer_title
  , a.merchant_Name AS merchant_name
  , a.merchant_Id AS merchant_id
  , a.transaction_Id AS transaction_id
  , a.Number_of_Vouchers AS number_of_vouchers
  , a.GR AS GR
  , a.deal_owner AS deal_owner
  , a.cm_location AS cm_location
  , a.new_customer_day AS new_customer_day
  , a.new_customer_month as new_customer_month
  , a.last_purchase_date AS last_purchase_date
  , a.first_purchase_date AS first_purchase_date,
    a.cashback_amount as cashback_amount
  , a.promo_length as promo_length, 
  a.promo_logic as promo_logic, 
  a.promo_name as promo_name, 
  a.promo_description as promo_description, 
  a.promocode_type as promocode_type,
  a.promo_discount_amount as promo_discount_amount, 
  a.promo_discount_percentage as promo_discount_percentage, 
  a.promo_max_cap as promo_max_cap,
  a.promo_cashback_percentage as promo_cashback_percentage, 
  a.promo_cashback_amount as promo_cashback_amount, 
  a.promo_offer_price_range_from as promo_offer_price_range_from, 
  a.promo_is_cashback as promo_is_cashback, 
  a.promo_is_deferential as promo_is_deferential, 
  a.promo_has_user_transacted as promo_has_user_transacted,
  
  --a.voucher_redeemed as voucher_redeemed,
  a.referral_program_id as referral_program_id
  --,1 as rank1
  ,y.dcg_ga AS ga_channel
  ,y.campaign_grouping AS ga_campaign_grouping
  ,y.campaign_ga AS ga_campaign_name
  ,y.source_ga AS ga_source
  ,y.medium_ga AS ga_medium
  ,y.keyword_ga AS ga_keyword
  ,y.content_ga AS ga_content
  ,y.dealid_ga AS ga_deal_id
  ,ap.af_imei_number as af_imei_number
  ,ap.af_attribution_type as af_attribution_type
  ,ap.af_media_source as af_media_source
  ,ap.af_fb_adgroup_id as af_fb_adgroup_id
  ,STRING(ap.af_download_time) as af_download_time
  ,ap.af_fb_campaign_id as af_fb_campaign_id
  ,ap.af_campaign_name  as af_campaign_name
  ,STRING(ap.af_install_time)  as af_install_time
  ,ap.af_platform_type as af_platform_type
  ,ap.af_fb_campaign_name  as af_fb_campaign_name
  ,ap.af_app_version as af_app_version
  ,ap.af_os_version as af_os_version
  ,ap.af_fb_adset_id as af_fb_adset_id
  ,ap.af_event_type as af_event_type
  ,ap.af_ad_name as af_ad_name
  ,ap.af_ad_id as af_ad_id
  ,ap.af_ad_type  as af_ad_type
  ,ap.af_adset_name as af_adset_name
  ,ap.af_adset_id as af_adset_id
  ,ap.af_campaign_id as af_campaign_id
  ,ap.af_channel as af_channel
  , ap.af_reaatributed_flag as af_reaatributed_flag
  ,ap.af_is_retargeting as af_is_retargeting,
 ap.af_re_targeting_conversion_type as af_is_reengagement  
 FROM 
 nb_reports.master_transaction  a
 LEFT JOIN (
  select
rank1,
orderid_ga,
dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga
from
(
select 
dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga,orderid_ga,
rank() over (partition by orderid_ga  order by  dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga) as rank1
from
(
SELECT
    dcg AS dcg_ga
    ,campaign_grouping 
    ,campaign AS campaign_ga
    ,source AS source_ga
    ,medium AS medium_ga
    ,keyword AS keyword_ga
    ,content AS content_ga
    ,dealid AS dealid_ga
    ,INTEGER (orderid) AS orderid_ga
  FROM nb_reports.ga_source_medium
  where orderid is not null 
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
  )
  where orderid_ga is not null
  )
 where rank1 = 1
  ) AS y ON a.order_Id = y.orderid_ga
  LEFT JOIN  
 (select * from nb_reports.reengagement_appsflyer where rank = 1) as ap on a.order_Id = ap.af_order_id 
--Group by 1, 2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,56,57,58,59,60,61,62,63,64,65,67,68,69,70,71,72,73,74,af_ad_id--,75,76,77,78
 
 ) a
 left join 
  (
 SELECT  
   a.customer_id AS customer_id,
   a.date_time_ist AS date_time_ist

  , a.promoCode AS promocode
  , a.platform_type AS platform_type
  , a.deal_id AS deal_id
  --, a.offer_Id AS offer_id
  , a.category_id AS category_id
  , a.merchant_Name AS merchant_name
  , a.cm_location AS cm_location
   -- ,a.cashback_amount as cashback_amount
  ,y.dcg_ga AS ga_channel
  ,y.campaign_grouping AS ga_campaign_grouping
  ,y.campaign_ga AS ga_campaign_name
  ,y.source_ga AS ga_source
  ,y.medium_ga AS ga_medium
  ,y.keyword_ga AS ga_keyword
  ,y.content_ga AS ga_content
  ,ap.af_attribution_type as af_attribution_type
  ,ap.af_media_source as af_media_source
  --,ap.af_fb_adgroup_id as af_fb_adgroup_id
  ,ap.af_campaign_name  as af_campaign_name
  ,ap.af_platform_type as af_platform_type
  ,ap.af_app_version as af_app_version
  ,ap.af_os_version as af_os_version
  ,ap.af_is_retargeting as af_is_retargeting,
 ap.af_re_targeting_conversion_type as af_is_reengagement  
 FROM 
 nb_reports.master_transaction  a
 LEFT JOIN (
  select
rank1,
orderid_ga,
dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga
from
(
select 
dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga,orderid_ga,
rank() over (partition by orderid_ga  order by  dcg_ga,campaign_grouping,campaign_ga,source_ga,medium_ga,keyword_ga,content_ga,dealid_ga) as rank1
from
(
SELECT
    dcg AS dcg_ga
    ,campaign_grouping 
    ,campaign AS campaign_ga
    ,source AS source_ga
    ,medium AS medium_ga
    ,keyword AS keyword_ga
    ,content AS content_ga
    ,dealid AS dealid_ga
    ,INTEGER (orderid) AS orderid_ga
  FROM nb_reports.ga_source_medium
  where orderid is not null 
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
  )
  where orderid_ga is not null
  )
 where rank1 = 1
  ) AS y ON a.order_Id = y.orderid_ga
  LEFT JOIN  
 (select * from nb_reports.reengagement_appsflyer where rank = 1) as ap on a.order_Id = ap.af_order_id 
where a.first_transaction = 'TRUE'
Group by 1, 2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23--, 24--, 25,26
 
 ) b on a.customer_id = b.customer_id
"

tableName=reengagement
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_reengagement\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_reengagement" &
##v_first_pid=$!
v_downstream_pids+=" $!"


if wait $v_downstream_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_downstream_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Downstream  and Reengagement  Tables status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ## sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


