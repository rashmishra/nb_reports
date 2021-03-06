v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=nb_reports;
date

v_query_ga_source_medium="select * from
(
select
DCG, 
campaign_grouping,
      
campaign, 
source, 
medium, 
keyword, 
 content,
    
 dealid,
orderid
from
(select 

DCG, 
campaign_grouping,
      
campaign, 
source, 
medium, 
keyword, 
 content,
    
 dealid,
orderid,
--rank() over (partition by )
rank() over (partition by orderid,date  order by  dcg,dealid, campaign, source, medium, keyword, content, campaign_grouping) as rank1

from

(SELECT  date,
case when trafficSource.medium in ('organic') then 'Organic' 
     when trafficSource.medium in ('Email','email','grpnharvey','nbharvey') then 'Email' 
     
     when (trafficSource.medium IN ('referral')
           AND trafficSource.source IN ('facebook.com',
          'm.facebook.com',
          'lm.facebook.com',
          'web.facebook.com',
          'l.facebook.com',
          't.co',
          'mobilefb.blogspot.com',
          'mobile.facebook.com',
          'linkedin.com',
          'mbasic.facebook.com',
          'apps.facebook.com',
          'meetup.com',
          'beta.facebook.com',
          'plus.url.google.com',
          'blogger.com',
          'business.facebook.com',
          'c.facebook.com',
          'dailymotion.com',
          'fbnotes',
          'groups.google.com',
          'in.linkedin.com',
          'in.pinterest.com',
          'instagram.com',
          'intern.facebook.com',
          'lnkd.in',
          'mtouch.facebook.com',
          'pinterest.com',
          'plus.google.com',
          'prod.facebook.com',
          'quora.com',
          'reddit.com',
         'scribd.com',
          'tinyurl.com',
          'touch.facebook.com',
          'twitter.com',
          'upload.facebook.com',
          'youtube.com')
          OR trafficSource.medium IN ('social')) then 'Social' 
    
     when (
     trafficSource.medium IN ('referral')
    AND trafficSource.source not IN ( 'facebook.com',
      'm.facebook.com',
      'lm.facebook.com',
      'web.facebook.com',
      'l.facebook.com',
      't.co',
      'mobilefb.blogspot.com',
      'mobile.facebook.com',
      'linkedin.com',
      'mbasic.facebook.com',
      'apps.facebook.com',
      'meetup.com',
      'beta.facebook.com',
      'plus.url.google.com',
      'blogger.com',
      'business.facebook.com',
      'c.facebook.com',
      'dailymotion.com',
      'fbnotes',
      'groups.google.com',
      'in.linkedin.com',
      'in.pinterest.com',
      'instagram.com',
      'intern.facebook.com',
      'lnkd.in',
      'mtouch.facebook.com',
      'pinterest.com',
      'plus.google.com',
      'prod.facebook.com',
      'quora.com',
      'reddit.com',
      'scribd.com',
      'tinyurl.com',
      'touch.facebook.com',
      'twitter.com',
      'upload.facebook.com',
      'youtube.com') )                
       then 'Referral'
       
       when (
       trafficSource.medium = '(none)' 
    AND trafficSource.source = '(direct)' 
       )
       then 'Direct'
       
       when (
       trafficSource.adwordsClickInfo.adNetworkType = 'Content'
       or (trafficSource.source in ('criteo','Criteo','facebook','Facebook','YahooNative','timesofindia','tribalfusion','taboola','youtube','instagram','Bookmyshow')
       and trafficSource.medium in ('CPC','cpc','cpm','CPM','CPV','cpv','Flat fee','flat fee','flatfee','Flatfee'))
       )
       then 'Display'
      
      WHEN (
      trafficSource.adwordsClickInfo.customerId = 3038977265 
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      )
      then 'Generic Paid Search'
      
      WHEN (
      trafficSource.adwordsClickInfo.customerId = 2092739606 
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      )
      then 'Brand Paid Search'
      
      WHEN (
      trafficSource.adwordsClickInfo.customerId = 1725894812
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      )
      then 'Merchant Paid Search'

       WHEN (
      trafficSource.adwordsClickInfo.customerId = 7289293664
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      )
      then 'Merchant DTR'
      
      when (
    trafficSource.source in ('aff','omg','dgm','nap','NAP','admitad')
    )
     then 'Affiliates'
      
      else 'Others'
      
    end as dcg,
    
    case 
      when(
      trafficSource.source = 'google' and trafficSource.medium = 'cpc' and trafficSource.campaign contains 'Brand' and trafficSource.adwordsClickInfo.customerId = 2092739606 
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      ) 
      then 'Google Brand Paid Search'
      
      when(
      trafficSource.source = 'google' and trafficSource.medium = 'cpc' and trafficSource.campaign contains 'Generic' and trafficSource.adwordsClickInfo.customerId = 3038977265 
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      ) 
      then 'Google Generic Paid Search'
      
      when(
      trafficSource.source = 'bing' and trafficSource.medium = 'cpc' and trafficSource.campaign contains 'Brand'
      ) 
      then 'Bing Brand Paid Search'
      
      when(
      trafficSource.source = 'bing' and trafficSource.medium = 'cpc' and trafficSource.campaign contains 'Generic'
      ) 
      then 'Bing Generic Paid Search'
      
      when(
      trafficSource.source = 'google' and trafficSource.medium = 'cpc' and (not trafficSource.campaign contains 'marketing' or trafficSource.campaign  contains 'similar') and trafficSource.adwordsClickInfo.adNetworkType = 'Content'
      ) 
      then 'GDN - Outbound'
      
      when(
      trafficSource.source = 'google' and trafficSource.medium = 'cpc' and trafficSource.campaign contains 'marketing' and not trafficSource.campaign  contains 'similar' and trafficSource.adwordsClickInfo.adNetworkType = 'Content'
      ) 
      then 'GDN - Remarketing'
      
      when(
      trafficSource.source in ('criteo','Criteo') and trafficSource.medium in ('cpc','CPC')
       ) 
      then 'Criteo'
      
      when(
      trafficSource.source in ('facebook','Facebook') and trafficSource.medium in ('cpc','CPC','cpm','CPM')
       ) 
      then 'Facebook'
      
      when(
      trafficSource.source in ('YahooNative') and trafficSource.medium in ('cpc','CPC','cpm','CPM')
       ) 
      then 'YahooNative'
      
      when(
      trafficSource.source in ('timesofindia') and trafficSource.medium in ('cpc','CPC')
       ) 
      then 'TOI'
      
      when(
      trafficSource.source in ('tribalfusion') and trafficSource.medium in ('cpm','CPM')
       ) 
      then 'Tribal Fusion'
      
      end as  campaign_grouping,
      
    trafficSource.campaign as campaign, 
    trafficSource.source as source, 
    trafficSource.medium as medium, 
    trafficSource.keyword as keyword, 
    trafficSource.adContent as content,
    
    hits.product.productSKU as dealid,
    hits.transaction.transactionId  as orderid
    
from (TABLE_DATE_RANGE([108795712.ga_sessions_], TIMESTAMP ('2017-08-28'), current_TIMESTAMP ())) 
 
where hits.transaction.transactionId is not null 
group by --date,
dcg, orderid, dealid, campaign, source, medium, keyword, content, campaign_grouping, date
)
)
where rank1 = 1

--order by rank1 desc

),
(
select
DCG, 
campaign_grouping,
      
campaign, 
source, 
medium, 
keyword, 
 content,
    
 dealid,
orderid
from
(select 

DCG, 
campaign_grouping,
      
campaign, 
source, 
medium, 
keyword, 
 content,
    
 dealid,
orderid,
--rank() over (partition by )
rank() over (partition by orderid,date  order by  dcg,dealid, campaign, source, medium, keyword, content, campaign_grouping) as rank1

from

(SELECT  date,
case when trafficSource.medium in ('organic') then 'Organic' 
     when trafficSource.medium in ('Email','email','grpnharvey','nbharvey') then 'Email' 
     
     when (trafficSource.medium IN ('referral')
           AND trafficSource.source IN ('facebook.com',
          'm.facebook.com',
          'lm.facebook.com',
          'web.facebook.com',
          'l.facebook.com',
          't.co',
          'mobilefb.blogspot.com',
          'mobile.facebook.com',
          'linkedin.com',
          'mbasic.facebook.com',
          'apps.facebook.com',
          'meetup.com',
          'beta.facebook.com',
          'plus.url.google.com',
          'blogger.com',
          'business.facebook.com',
          'c.facebook.com',
          'dailymotion.com',
          'fbnotes',
          'groups.google.com',
          'in.linkedin.com',
          'in.pinterest.com',
          'instagram.com',
          'intern.facebook.com',
          'lnkd.in',
          'mtouch.facebook.com',
          'pinterest.com',
          'plus.google.com',
          'prod.facebook.com',
          'quora.com',
          'reddit.com',
         'scribd.com',
          'tinyurl.com',
          'touch.facebook.com',
          'twitter.com',
          'upload.facebook.com',
          'youtube.com')
          OR trafficSource.medium IN ('social')) then 'Social' 
    
     when (
     trafficSource.medium IN ('referral')
    AND trafficSource.source not IN ( 'facebook.com',
      'm.facebook.com',
      'lm.facebook.com',
      'web.facebook.com',
      'l.facebook.com',
      't.co',
      'mobilefb.blogspot.com',
      'mobile.facebook.com',
      'linkedin.com',
      'mbasic.facebook.com',
      'apps.facebook.com',
      'meetup.com',
      'beta.facebook.com',
      'plus.url.google.com',
      'blogger.com',
      'business.facebook.com',
      'c.facebook.com',
      'dailymotion.com',
      'fbnotes',
      'groups.google.com',
      'in.linkedin.com',
      'in.pinterest.com',
      'instagram.com',
      'intern.facebook.com',
      'lnkd.in',
      'mtouch.facebook.com',
      'pinterest.com',
      'plus.google.com',
      'prod.facebook.com',
      'quora.com',
      'reddit.com',
      'scribd.com',
      'tinyurl.com',
      'touch.facebook.com',
      'twitter.com',
      'upload.facebook.com',
      'youtube.com') )                
       then 'Referral'
       
       when (
       trafficSource.medium = '(none)' 
    AND trafficSource.source = '(direct)' 
       )
       then 'Direct'
       
       when (
       trafficSource.adwordsClickInfo.adNetworkType = 'Content'
       or (trafficSource.source in ('criteo','Criteo','facebook','Facebook','YahooNative','timesofindia','tribalfusion','taboola','youtube','instagram','Bookmyshow')
       and trafficSource.medium in ('CPC','cpc','cpm','CPM','CPV','cpv','Flat fee','flat fee','flatfee','Flatfee'))
       )
       then 'Display'
      
      WHEN (
      trafficSource.adwordsClickInfo.customerId = 3038977265 
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      )
      then 'Generic Paid Search'
      
      WHEN (
      trafficSource.adwordsClickInfo.customerId = 2092739606 
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      )
      then 'Brand Paid Search'
      
      WHEN (
      trafficSource.adwordsClickInfo.customerId = 1725894812
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      )
      then 'Merchant Paid Search'

       WHEN (
      trafficSource.adwordsClickInfo.customerId = 7289293664
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      )
      then 'Merchant DTR'
      
      when (
    trafficSource.source in ('aff','omg','dgm','nap','NAP','admitad')
    )
     then 'Affiliates'
      
      else 'Others'
      
    end as dcg,
    
    case 
      when(
      trafficSource.source = 'google' and trafficSource.medium = 'cpc' and trafficSource.campaign contains 'Brand' and trafficSource.adwordsClickInfo.customerId = 2092739606 
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      ) 
      then 'Google Brand Paid Search'
      
      when(
      trafficSource.source = 'google' and trafficSource.medium = 'cpc' and trafficSource.campaign contains 'Generic' and trafficSource.adwordsClickInfo.customerId = 3038977265 
      and trafficSource.adwordsClickInfo.adNetworkType in ('Google Search','Search partners')
      ) 
      then 'Google Generic Paid Search'
      
      when(
      trafficSource.source = 'bing' and trafficSource.medium = 'cpc' and trafficSource.campaign contains 'Brand'
      ) 
      then 'Bing Brand Paid Search'
      
      when(
      trafficSource.source = 'bing' and trafficSource.medium = 'cpc' and trafficSource.campaign contains 'Generic'
      ) 
      then 'Bing Generic Paid Search'
      
      when(
      trafficSource.source = 'google' and trafficSource.medium = 'cpc' and (not trafficSource.campaign contains 'marketing' or trafficSource.campaign  contains 'similar') and trafficSource.adwordsClickInfo.adNetworkType = 'Content'
      ) 
      then 'GDN - Outbound'
      
      when(
      trafficSource.source = 'google' and trafficSource.medium = 'cpc' and trafficSource.campaign contains 'marketing' and not trafficSource.campaign  contains 'similar' and trafficSource.adwordsClickInfo.adNetworkType = 'Content'
      ) 
      then 'GDN - Remarketing'
      
      when(
      trafficSource.source in ('criteo','Criteo') and trafficSource.medium in ('cpc','CPC')
       ) 
      then 'Criteo'
      
      when(
      trafficSource.source in ('facebook','Facebook') and trafficSource.medium in ('cpc','CPC','cpm','CPM')
       ) 
      then 'Facebook'
      
      when(
      trafficSource.source in ('YahooNative') and trafficSource.medium in ('cpc','CPC','cpm','CPM')
       ) 
      then 'YahooNative'
      
      when(
      trafficSource.source in ('timesofindia') and trafficSource.medium in ('cpc','CPC')
       ) 
      then 'TOI'
      
      when(
      trafficSource.source in ('tribalfusion') and trafficSource.medium in ('cpm','CPM')
       ) 
      then 'Tribal Fusion'
      
      end as  campaign_grouping,
      
    trafficSource.campaign as campaign, 
    trafficSource.source as source, 
    trafficSource.medium as medium, 
    trafficSource.keyword as keyword, 
    trafficSource.adContent as content,
    
    hits.product.productSKU as dealid,
    hits.transaction.transactionId  as orderid
    
from (TABLE_DATE_RANGE([157529880.ga_sessions_], TIMESTAMP ('2017-08-28'), current_TIMESTAMP ())) 
 
where hits.transaction.transactionId is not null 
group by --date,
dcg, orderid, dealid, campaign, source, medium, keyword, content, campaign_grouping, date
)
)
where rank1 = 1
--order by rank1 desc

)"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=ga_source_medium2
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_ga_source_medium\""
bq query --maximum_billing_tier 10000 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_ga_source_medium" &
v_first_pid=$!
v_downstream_pids+=" $v_first_pid"
wait $v_first_pid;

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
  --, a.transaction_Id AS transaction_id
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
     a.paymenttype as paymenttype
  , a.discountbymerchant as discountbymerchant,
  a.promo_name as promo_name,
  a.promo_flag as promo_flag,
  a.promo_logic as promo_logic,
  a.promo_description as promo_description, 
   a.promocode_type as promocode_type
   , a.primary_category as primary_category
--   a.promo_discount_amount as promo_discount_amount, 
--   a.promo_discount_percentage as promo_discount_percentage, 
--   a.promo_max_cap as promo_max_cap,
--   a.promo_cashback_percentage as promo_cashback_percentage, 
--   a.promo_cashback_amount as promo_cashback_amount, 
--   a.promo_offer_price_range_from as promo_offer_price_range_from, 
--   a.promo_is_cashback as promo_is_cashback, 
--   a.promo_is_deferential as promo_is_deferential, 
--   a.promo_has_user_transacted as promo_has_user_transacted 
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
  , a.af_is_retargeting as af_is_retargeting
  , a.af_is_reengagement as af_is_reengagement 
  , b.date_time_ist AS aquisition_date_time_ist
  , b.promoCode AS aquisition_promocode
  , b.platform_type AS aquisition_platform_type
  , b.deal_id AS aquisition_deal_id
  , b.category_id AS aquisition_category_id
  , b.merchant_Name AS aquisition_merchant_name
  , b.cm_location AS aquisition_cm_location
  , b.ga_channel as aquisition_ga_channel
  , b.ga_campaign_grouping as aquisition_ga_campaign_grouping
  , b.ga_campaign_name as aquisition_ga_campaign_name
  , b.ga_source as aquisition_ga_source
  ,b.ga_medium as aquisition_ga_medium
  ,b.ga_keyword as aquisition_ga_keyword
  ,b.ga_content as aquisition_ga_content
  ,b.af_attribution_type as aquisition_af_attribution_type
  ,b.af_media_source as aquisition_af_media_source
  ,b.af_campaign_name  as aquisition_af_campaign_name
  ,b.af_platform_type as aquisition_af_platform_type
  ,b.af_app_version as aquisition_af_app_version
  ,b.af_os_version as aquisition_af_os_version
  ,b.af_is_retargeting as aquisition_af_is_retargeting,
 b.af_is_reengagement as aquisition_af_is_reengagement,
   b.af_fb_campaign_id as aquisition_af_fb_campaign_id, 
  b.af_fb_campaign_name as aquisition_af_fb_campaign_name, 
  b.af_fb_adset_id as aquisition_af_fb_adset_id, 
  b.Af_fb_Adgroup_Id as aquisition_Af_fb_Adgroup_Id
    
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
  --, a.transaction_Id AS transaction_id
  , a.Number_of_Vouchers AS number_of_vouchers
  , a.GR AS GR
  , a.deal_owner AS deal_owner
  , a.cm_location AS cm_location
  , a.new_customer_day AS new_customer_day
  , a.new_customer_month as new_customer_month
  , a.last_purchase_date AS last_purchase_date
  , a.first_purchase_date AS first_purchase_date
  , a.cashback_amount as cashback_amount
  --, a.promo_length as promo_length, 
  ,a.promo_logic as promo_logic, 
  a.promo_name as promo_name, 
   a.promo_description as promo_description, 
   a.promocode_type as promocode_type,
--   a.promo_discount_amount as promo_discount_amount, 
--   a.promo_discount_percentage as promo_discount_percentage, 
--   a.promo_max_cap as promo_max_cap,
--   a.promo_cashback_percentage as promo_cashback_percentage, 
--   a.promo_cashback_amount as promo_cashback_amount, 
--   a.promo_offer_price_range_from as promo_offer_price_range_from, 
--   a.promo_is_cashback as promo_is_cashback, 
--   a.promo_is_deferential as promo_is_deferential, 
--   a.promo_has_user_transacted as promo_has_user_transacted,
a.promo_flag as promo_flag,
  a.referral_program_id as  referral_program_id
  , a.paymenttype as paymenttype
  , a.discountbymerchant as discountbymerchant,
  a.primary_category as primary_category
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
 ap.af_re_targeting_conversion_type as af_is_reengagement,

 FROM 
 nb_reports.master_transaction  a
 LEFT JOIN (
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
  FROM nb_reports.ga_source_medium2
  where orderid is not null 
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
  ) AS y ON a.order_Id = y.orderid_ga
  LEFT JOIN  
 (select * from nb_reports.downstream_appsflyer  where rank = 1) as ap on a.order_Id = ap.af_order_id 
 ) a
 left join 
  (
 SELECT  
   a.customer_id AS customer_id,
   a.date_time_ist AS date_time_ist
  , a.promoCode AS promocode
  , a.platform_type AS platform_type
  , a.deal_id AS deal_id
  , a.category_id AS category_id
  , a.merchant_Name AS merchant_name
  , a.cm_location AS cm_location
  ,y.dcg_ga AS ga_channel
  ,y.campaign_grouping AS ga_campaign_grouping
  ,y.campaign_ga AS ga_campaign_name
  ,y.source_ga AS ga_source
  ,y.medium_ga AS ga_medium
  ,y.keyword_ga AS ga_keyword
  ,y.content_ga AS ga_content
  ,ap.af_attribution_type as af_attribution_type
  ,ap.af_media_source as af_media_source
  ,ap.af_campaign_name  as af_campaign_name
  ,ap.af_platform_type as af_platform_type
  ,ap.af_app_version as af_app_version
  ,ap.af_os_version as af_os_version
  ,ap.af_is_retargeting as af_is_retargeting,
 ap.af_re_targeting_conversion_type as af_is_reengagement,
  ap.af_fb_campaign_id as af_fb_campaign_id, 
  ap.af_fb_campaign_name as af_fb_campaign_name, 
  ap.af_fb_adset_id as af_fb_adset_id, 
  ap.Af_fb_Adgroup_Id as Af_fb_Adgroup_Id
 FROM 
 nb_reports.master_transaction  a
 LEFT JOIN (
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
  FROM nb_reports.ga_source_medium2
  where orderid is not null 
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
  ) AS y ON a.order_Id = y.orderid_ga
  LEFT JOIN  
 (select * from nb_reports.downstream_appsflyer where rank = 1) as ap on a.order_Id = ap.af_order_id 
where a.first_transaction = 'TRUE'
Group by 1, 2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,24,25,26,27
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
  --, a.transaction_Id AS transaction_id
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
  --a.promo_length as promo_length, 
  a.promo_logic as promo_logic 
  ,a.promo_name as promo_name, 
   a.promo_description as promo_description, 
   a.promocode_type as promocode_type,
   a.promo_flag as promo_flag,
    a.primary_category as primary_category  
--   a.promo_discount_amount as promo_discount_amount, 
--   a.promo_discount_percentage as promo_discount_percentage, 
--   a.promo_max_cap as promo_max_cap,
--   a.promo_cashback_percentage as promo_cashback_percentage, 
--   a.promo_cashback_amount as promo_cashback_amount, 
--   a.promo_offer_price_range_from as promo_offer_price_range_from, 
--   a.promo_is_cashback as promo_is_cashback, 
--   a.promo_is_deferential as promo_is_deferential, 
--   a.promo_has_user_transacted as promo_has_user_transacted  
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
   a.paymenttype as paymenttype,
  a.discountbymerchant as discountbymerchant ,
 a.af_is_reengagement as af_is_reengagement 
  , b.date_time_ist AS aquisition_date_time_ist
  , b.promoCode AS aquisition_promocode
  , b.platform_type AS aquisition_platform_type
  , b.deal_id AS aquisition_deal_id
  , b.category_id AS aquisition_category_id
  , b.merchant_Name AS aquisition_merchant_name
  , b.cm_location AS aquisition_cm_location
   ,b.ga_channel as aquisition_ga_channel
  ,b.ga_campaign_grouping as aquisition_ga_campaign_grouping
  ,b.ga_campaign_name as aquisition_ga_campaign_name
  ,b.ga_source as aquisition_ga_source
  ,b.ga_medium as aquisition_ga_medium
  ,b.ga_keyword as aquisition_ga_keyword
  ,b.ga_content as aquisition_ga_content
  ,b.af_attribution_type as aquisition_af_attribution_type
  ,b.af_media_source as aquisition_af_media_source
  ,b.af_campaign_name  as aquisition_af_campaign_name
  ,b.af_platform_type as aquisition_af_platform_type
  ,b.af_app_version as aquisition_af_app_version
  ,b.af_os_version as aquisition_af_os_version
  ,b.af_is_retargeting as aquisition_af_is_retargeting,
 b.af_is_reengagement as aquisition_af_is_reengagement,
 b.af_fb_campaign_id as aquisition_af_fb_campaign_id, 
  b.af_fb_campaign_name as aquisition_af_fb_campaign_name, 
  b.af_fb_adset_id as aquisition_af_fb_adset_id, 
  b.Af_fb_Adgroup_Id as aquisition_Af_fb_Adgroup_Id
    
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
  --, a.transaction_Id AS transaction_id
  , a.Number_of_Vouchers AS number_of_vouchers
  , a.GR AS GR
  , a.deal_owner AS deal_owner
  , a.cm_location AS cm_location
  , a.new_customer_day AS new_customer_day
  , a.new_customer_month as new_customer_month
  , a.last_purchase_date AS last_purchase_date
  , a.first_purchase_date AS first_purchase_date,
    a.cashback_amount as cashback_amount,
  --, a.promo_length as promo_length, 
  a.promo_logic as promo_logic, 
  a.promo_name as promo_name, 
   a.promo_description as promo_description, 
   a.promocode_type as promocode_type,
   a.promo_flag as promo_flag,
--   a.promo_discount_amount as promo_discount_amount, 
--   a.promo_discount_percentage as promo_discount_percentage, 
--   a.promo_max_cap as promo_max_cap,
--   a.promo_cashback_percentage as promo_cashback_percentage, 
--   a.promo_cashback_amount as promo_cashback_amount, 
--   a.promo_offer_price_range_from as promo_offer_price_range_from, 
--   a.promo_is_cashback as promo_is_cashback, 
--   a.promo_is_deferential as promo_is_deferential, 
--   a.promo_has_user_transacted as promo_has_user_transacted,
   a.referral_program_id as referral_program_id,
  a.paymenttype as paymenttype,
  a.discountbymerchant as discountbymerchant ,
  a.primary_category as primary_category  
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
  FROM nb_reports.ga_source_medium2
  where orderid is not null 
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
  ) AS y ON a.order_Id = y.orderid_ga
  LEFT JOIN  
 (select * from nb_reports.reengagement_appsflyer where rank = 1) as ap on a.order_Id = ap.af_order_id 
 ) a
 left join 
  (
 SELECT  
   a.customer_id AS customer_id,
   a.date_time_ist AS date_time_ist

  , a.promoCode AS promocode
  , a.platform_type AS platform_type
  , a.deal_id AS deal_id
  , a.category_id AS category_id
  , a.merchant_Name AS merchant_name
  , a.cm_location AS cm_location
  ,y.dcg_ga AS ga_channel
  ,y.campaign_grouping AS ga_campaign_grouping
  ,y.campaign_ga AS ga_campaign_name
  ,y.source_ga AS ga_source
  ,y.medium_ga AS ga_medium
  ,y.keyword_ga AS ga_keyword
  ,y.content_ga AS ga_content
  ,ap.af_attribution_type as af_attribution_type
  ,ap.af_media_source as af_media_source
  ,ap.af_campaign_name  as af_campaign_name
  ,ap.af_platform_type as af_platform_type
  ,ap.af_app_version as af_app_version
  ,ap.af_os_version as af_os_version
  ,ap.af_is_retargeting as af_is_retargeting,
 ap.af_re_targeting_conversion_type as af_is_reengagement,
 ap.af_fb_campaign_id as af_fb_campaign_id, 
  ap.af_fb_campaign_name as af_fb_campaign_name, 
  ap.af_fb_adset_id as af_fb_adset_id, 
  ap.Af_fb_Adgroup_Id as Af_fb_Adgroup_Id
 FROM 
 nb_reports.master_transaction  a
 LEFT JOIN (
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
  FROM nb_reports.ga_source_medium2
  where orderid is not null 
  group by dcg_ga, campaign_grouping , campaign_ga , source_ga, medium_ga, keyword_ga, content_ga, dealid_ga,  orderid_ga
  ) AS y ON a.order_Id = y.orderid_ga
  LEFT JOIN  
 (select * from nb_reports.reengagement_appsflyer where rank = 1) as ap on a.order_Id = ap.af_order_id 
where a.first_transaction = 'TRUE'
Group by 1, 2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,26,27
 
 ) b on a.customer_id = b.customer_id

"

tableName=reengagement
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_reengagement\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_reengagement" &
v_first_pid=$!
v_downstream_pids+=" $!"
wait $v_first_pid;



# reengagement loading. Replace existing
v_query_mapping="select 
y.BAID as BAID,
merchantId,
merchantName,
string(deal_id) as dealId,
dealCategory,
primary_category,
city,
state
 from
 
(
 select  string(merchantId) as merchantid ,
 case when catInfo.isPrimary is true then catInfo.key end as primary_category ,
 redemptionAddress.cityTown  AS CITY,
     redemptionAddress.state as state,
     isChain ,
     name as merchantName,
     string(businessAccountId) as BAID
 from [Atom.merchant] where isPublished is true
 group by 1,2,3,4,5,6,7
 ) y
 
 left join 
 (
select
_id as deal_id, 
BAID, 
merchant_id , 
 Categoryid AS dealCategory,

from [Atom.deal] a
left join (select integer(id) as id, mappings.businessAccount.id as BAID, mappings.merchant.id  as merchant_id  , 
from flatten(flatten([Atom.mapping],mappings.merchant.id),mappings.businessAccount.id)
where type = 'deal' group by 1,2,3) b on a._id = b.id
group by 1,2,3,4
) x
 
 
  on x.merchant_id = y.merchantid 
 group by 1,2,3,4,5,6,7,8

"

tableName=mapping
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_mapping\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_mapping" &
v_first_pid=$!
v_downstream_pids+=" $!"
wait $v_first_pid;




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

echo "Downstream  and Reengagement & mapping Tables status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ## sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


