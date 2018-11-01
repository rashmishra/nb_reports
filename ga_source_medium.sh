v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=nb_reports;
date


# Downstream table loading. Replace existing
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
    
from (TABLE_DATE_RANGE([108795712.ga_sessions_], TIMESTAMP ('2016-04-08'), current_TIMESTAMP ())) 
 
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

tableName=ga_source_medium
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_ga_source_medium\""
bq query --maximum_billing_tier 10000 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_ga_source_medium" &
v_first_pid=$!
v_downstream_pids+=" $v_first_pid"
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

echo "ga_source_medium Tables status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ## sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


