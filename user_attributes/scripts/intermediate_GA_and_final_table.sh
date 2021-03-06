#!/bin/bash


v_task_start_time=`date`;

echo "Task Started at ${v_task_start_time}";




p_exit_upon_error(){

    ## Parameters
    # $1: Task Status (passed/ failed)
    # $2: Sub Task (e.g. Extraction of data, Cloud upload, metadata table creation, final table population)

    v_task_status="$1";
    v_subtask="$2";


    if [ "$v_task_status" == "failed" ] ; then
        v_log_obj_txt+=`echo "\n$(date) $(date) Task ($v_subtask) failed for GA intermediate tables refresh for User Attributes. Hence exiting."`;

        taskEndTime=`date`;

        v_task_end_epoch=`date +%s`
        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

        echo -e "\n$(date) Task ($v_subtask) failed for GA intermediate tables refresh for User Attributes. Hence exiting."

        echo -e "\n$(date): Task ($v_subtask) failed for GA intermediate tables refresh for User Attributes. Hence exiting." | mail -s "FAILED | User Attributes GA intermediate tables computation at ${v_task_start_time}" sairanganath.v@nearbuy.com 
        # harsh.choudhary@nearbuy.com mahesh.sharma@nearbuy.com

        echo -e  "\nTask started at ${v_task_start_time} and ended at ${v_task_end_time}.";

        exit 1;

    fi

}

v_dataset_name="engg_reporting";

## Table (i): user_attributes_deal_pricepoint

v_query="select string(_id) as dealid,
                   sum( offers.units.ps.msp/100) as pricepoint,
                   AVG(offers.units.ps.msp/100) AS avg_deal_msp,
                   EXACT_COUNT_DISTINCT(offers.key) AS no_of_offers,
                   MAX(offers.units.ps.msp/100) AS max_offer_msp,
                   MIN(offers.units.ps.msp/100) AS min_offer_msp,
                   NTH(51, quantiles((offers.units.ps.msp/100),101)) AS median_pricepoint,
                   GROUP_CONCAT(STRING(INTEGER(offers.units.ps.msp/100)), ',' ) as MSPs
            from [Atom.offer]
            WHERE offers.isActive = true
            GROUP BY 1";

      


v_destination_tbl="${v_dataset_name}.user_attributes_deal_pricepoint";

echo -e "bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_deal_pricepoint' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_deal_pricepoint' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table (i): user_attributes_deal_pricepoint




## Table 1 (a): user_attributes_ga_platform_daytime_affinity

v_query="SELECT 
  Customer_ID,
  Min(date( first_session_date )) as firstSessionDate,
  Max( latest_session_date ) as latestSessionDate,
  count(distinct( session_date )) as activeDays,
  Round(count(distinct( Session_ID ))/count(distinct( session_date )),1) as sessionsPerActiveDay,
  Case
    when Sum(case when source ='Android' then 1 else 0 end)/count(source) >=0.7 then 'Android'
    when Sum(case when source='iOS' then 1 else 0 end)/count(source) >=0.7 then 'iOS'
    when Sum(case when source='Web' then 1 else 0 end)/count(source) >=0.7 then 'Web'
    when count(source) is null or count(source)=0 then null
    else 'Cross-Platfrom' end as platformAffinity,
  sum(case when hour(sec_to_timestamp( session_start_time )) BETWEEN 5 AND 17 then 1 else 0 end) as AM_6_PM_6,
  sum(case when hour(sec_to_timestamp( session_start_time )) NOT BETWEEN 5 AND 17 then 1 else 0 end) as PM_6_AM_6,
FROM [big-query-1233:engg_reporting.user_attributes_ga_session_history]
group by 1";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_platform_daytime_affinity";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_ga_platform_daytime_affinity' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_ga_platform_daytime_affinity' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 1 (a): user_attributes_ga_platform_daytime_affinity

## Table 1 (b): user_attributes_ga_active_since_days

v_query="SELECT Customer_ID
       , IF (DATE(latestSessionDate) BETWEEN DATE(DATE_ADD(CURRENT_DATE(), -8, 'DAY')) AND DATE(CURRENT_DATE()), 1, 0 ) AS isActive_last_7_days
       , IF (DATE(latestSessionDate) BETWEEN DATE(DATE_ADD(CURRENT_DATE(), -1, 'MONTH')) AND DATE(CURRENT_DATE()), 1, 0 ) AS isActive_last_30_days
       , IF (DATE(latestSessionDate) BETWEEN DATE(DATE_ADD(CURRENT_DATE(), -2, 'MONTH')) AND DATE(CURRENT_DATE()), 1, 0 ) AS isActive_last_60_days
       , IF (DATE(latestSessionDate) BETWEEN DATE(DATE_ADD(CURRENT_DATE(), -3, 'MONTH')) AND DATE(CURRENT_DATE()), 1, 0 ) AS isActive_last_90_days
FROM ${v_dataset_name}.user_attributes_ga_platform_daytime_affinity
"

v_destination_tbl="${v_dataset_name}.user_attributes_ga_active_since_days";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 1 (b) 'user_attributes_ga_active_since_days' : $v_task_status";


v_subtask="GA intermediate Table 1(b) 'user_attributes_ga_active_since_days' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 1 (b): user_attributes_ga_platform_daytime_affinity

## Table 2: user_attributes_ga_browser_sessions

v_query="select
Customer_ID,
Sum(case when userConnection='2G' then 1 else 0 end ) as twoG,
Sum(case when userConnection='3G' then 1 else 0 end ) as threeG,
Sum(case when userConnection='4G' then 1 else 0 end ) as fourG,
Sum(case when userConnection='WIFI' then 1 else 0 end ) as wifi,
from [engg_reporting.user_attributes_ga_group_B_effective]
GROUP BY 1";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_browser_sessions";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_ga_browser_sessions' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_ga_browser_sessions' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 2: user_attributes_ga_browser_sessions



## Table 3: user_attributes_ga_latest_mob_info

v_query="SELECT customerid 
       , FIRST(city) AS city
       , FIRST(operatingSystem) AS operatingSystem
       , FIRST(operatingSystemVersion) AS operatingSystemVersion
       , FIRST(screenResolution) AS screenResolution
       , FIRST(mobileDeviceInfo) AS mobileDeviceInfo
FROM (select  Customer_ID as customerid,
              INTEGER(date) AS session_date,
              city,
              operatingSystem,
              operatingSystemVersion,
              screenResolution,
              mobileDeviceInfo,
              DENSE_RANK() OVER (PARTITION BY Customer_ID ORDER BY session_date DESC) AS ranka,
FROM [engg_reporting.user_attributes_ga_group_B_effective]
)
WHERE ranka = 1
GROUP BY 1
";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_latest_mob_info";

echo -e "bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_ga_latest_mob_info' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_ga_latest_mob_info' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 3: user_attributes_ga_latest_mob_info





p_exit_upon_error(){

    ## Parameters
    # $1: Task Status (passed/ failed)
    # $2: Sub Task (e.g. Extraction of data, Cloud upload, metadata table creation, final table population)

    v_task_status="$1";
    v_subtask="$2";


    if [ "$v_task_status" == "failed" ] ; then
        v_log_obj_txt+=`echo "\n$(date) $(date) Task ($v_subtask) failed for GA intermediate tables refresh for User Attributes. Hence exiting."`;

        taskEndTime=`date`;

        v_task_end_epoch=`date +%s`
        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

        echo -e "\n$(date) Task ($v_subtask) failed for GA intermediate tables refresh for User Attributes. Hence exiting."

        echo -e "\n$(date): Task ($v_subtask) failed for GA intermediate tables refresh for User Attributes. Hence exiting." | mail -s "FAILED | User Attributes GA intermediate tables computation at ${v_task_start_time}" sairanganath.v@nearbuy.com 
        # harsh.choudhary@nearbuy.com mahesh.sharma@nearbuy.com

        echo -e  "\nTask started at ${v_task_start_time} and ended at ${v_task_end_time}.";

        exit 1;

    fi

}

v_dataset_name="engg_reporting";


## Table 4: user_attributes_recent_search

v_query="SELECT dim.Customer_ID AS customerid
      , keys.latestSearchKeyword AS latestSearchKeyword
      , keys.secLatestSearchKeyword AS secLatestSearchKeyword
      , keys.thirdLatestSearchKeyword AS thirdLatestSearchKeyword
      , COALESCE(perc.percSessionsNearMe, 0.0) AS percSessionsNearMe
FROM  (SELECT Customer_ID 
       FROM [big-query-1233:engg_reporting.user_attributes_ga_group_A_effective] 
       WHERE LENGTH(Customer_ID) BETWEEN 6 AND 20
              AND Customer_ID IS NOT NULL
       GROUP BY 1 
       ) dim
LEFT JOIN  (SELECT Customer_ID
                   , Round((sum(case when upper(locationServices)='NEAR ME' then 1 else 0 end)/count(locationServices))*100,1) as percSessionsNearMe
            FROM [big-query-1233:engg_reporting.user_attributes_ga_group_A_effective]
            WHERE LENGTH(Customer_ID) BETWEEN 6 AND 20
              AND Customer_ID IS NOT NULL
            GROUP BY Customer_ID 
                ORDER BY Customer_ID
            ) perc
        ON dim.Customer_ID = perc.Customer_ID
LEFT JOIN  (SELECT customerid 
                 , FIRST(IF(ranka = 1, searchKeyword, null)) AS latestSearchKeyword
                 , FIRST(IF(ranka = 2, searchKeyword, null)) AS secLatestSearchKeyword
                 , FIRST(IF(ranka = 3, searchKeyword, null)) AS thirdLatestSearchKeyword
          FROM (SELECT Customer_ID as customerid,
                       date as session_date,
                       upper(searchKeyword) as searchKeyword,
                       DENSE_RANK() OVER (PARTITION BY customerid ORDER BY session_date DESC) AS ranka,
                  FROM [big-query-1233:engg_reporting.user_attributes_ga_group_A_effective]
                  WHERE searchKeyword IS NOT NULL
                    AND LENGTH(Customer_ID) BETWEEN 6 AND 20
                    AND Customer_ID IS NOT NULL
          )

          WHERE ranka BETWEEN 1 AND 5
          GROUP BY 1
          ) keys
ON dim.Customer_ID = keys.customerid
";


v_destination_tbl="${v_dataset_name}.user_attributes_recent_search";

echo -e "bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_recent_search' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_recent_search' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 4: user_attributes_recent_search



## Table 5: user_attributes_frequency_search

v_query="SELECT customerid 
       , FIRST(IF(ranka = 1, searchKeyword, null)) AS mostSearchKeyword
       , FIRST(IF(ranka = 2, searchKeyword, null)) AS secMostSearchKeyword
       , FIRST(IF(ranka = 3, searchKeyword, null)) AS thirdMostSearchKeyword
FROM (SELECT   Customer_ID as customerid,
                upper(searchKeyword) as searchKeyword,
                count(upper(searchKeyword)) as keywordCount,
                DENSE_RANK() OVER (PARTITION BY customerid ORDER BY keywordCount DESC) AS ranka,
  FROM  [big-query-1233:engg_reporting.user_attributes_ga_group_A_effective]
  GROUP BY 1,2
)
WHERE ranka BETWEEN 1 AND 5
GROUP BY 1
";



v_destination_tbl="${v_dataset_name}.user_attributes_frequency_search";

echo -e "bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_frequency_search' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_frequency_search' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 5: user_attributes_frequency_search



## Table 6: user_attributes_top_deals_browsed

v_query="SELECT customerid 
       , FIRST(IF(ranka = 1, Merchant_ID, null)) AS mostBrowsedMerchant
       , FIRST(IF(ranka = 2, Merchant_ID, null)) AS secMostBrowsedMerchant
       , FIRST(IF(ranka = 3, Merchant_ID, null)) AS thirdMostBrowsedMerchant
FROM (SELECT  Customer_ID as customerid,
              Merchant_ID,
              count(dealid) as dealViews,
              DENSE_RANK() OVER (PARTITION BY customerid ORDER BY dealViews DESC) AS ranka,
      FROM [big-query-1233:engg_reporting.user_attributes_ga_group_C_effective]
      WHERE action_type='2'
        AND UPPER(dealID) = LOWER(dealID)
      GROUP BY 1, 2
      )
WHERE ranka BETWEEN 1 AND 3
GROUP BY 1
";

      


v_destination_tbl="${v_dataset_name}.user_attributes_top_deals_browsed";

echo -e "bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_top_deals_browsed' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_top_deals_browsed' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 6: user_attributes_top_deals_browsed





## Table 7: user_attributes_user_categorywise_pricepoint

v_query="SELECT
  CustomerId,
  SUM(LOR_dealviews_browsed) AS LOR_dealviews_browsed,
  SUM(LOR_pricepoint_browsed) AS LOR_pricepoint_browsed,
  SUM(SNS_dealviews_browsed) AS SNS_dealviews_browsed,
  SUM(SNS_pricepoint_browsed) AS SNS_pricepoint_browsed,
  SUM(FNB_dealviews_browsed) AS FNB_dealviews_browsed,
  SUM(FNB_pricepoint_browsed) AS FNB_pricepoint_browsed,
  SUM(GTW_dealviews_browsed) AS GTW_dealviews_browsed,
  SUM(GTW_pricepoint_browsed) AS GTW_pricepoint_browsed,
  SUM(TTD_dealviews_browsed) AS TTD_dealviews_browsed,
  SUM(TTD_pricepoint_browsed) AS TTD_pricepoint_browsed,
  SUM(HNF_dealviews_browsed) AS HNF_dealviews_browsed,
  SUM(HNF_pricepoint_browsed) AS HNF_pricepoint_browsed,
  SUM(LOS_dealviews_browsed) AS LOS_dealviews_browsed,
  SUM(LOS_pricepoint_browsed) AS LOS_pricepoint_browsed,
  SUM(MVE_dealviews_browsed) AS MVE_dealviews_browsed,
  SUM(MVE_pricepoint_browsed) AS MVE_pricepoint_browsed,
  SUM(ACT_dealviews_browsed) AS ACT_dealviews_browsed,
  SUM(ACT_pricepoint_browsed) AS ACT_pricepoint_browsed,
  SUM(HEA_dealviews_browsed) AS HEA_dealviews_browsed,
  SUM(HEA_pricepoint_browsed) AS HEA_pricepoint_browsed,
  SUM(HNL_dealviews_browsed) AS HNL_dealviews_browsed,
  SUM(HNL_pricepoint_browsed) AS HNL_pricepoint_browsed,
  SUM(BNS_dealviews_browsed) AS BNS_dealviews_browsed,
  SUM(BNS_pricepoint_browsed) AS BNS_pricepoint_browsed,
  SUM(SNM_dealviews_browsed) AS SNM_dealviews_browsed,
  SUM(SNM_pricepoint_browsed) AS SNM_pricepoint_browsed
from (select  customerid,
              CASE WHEN category = 'LOR' THEN COUNT(c.dealId) END AS LOR_dealviews_browsed,
              CASE WHEN category = 'LOR' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS LOR_pricepoint_browsed,
              CASE WHEN category = 'SNS' THEN COUNT(c.dealId) END AS SNS_dealviews_browsed,
              CASE WHEN category = 'SNS' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS SNS_pricepoint_browsed,
              CASE WHEN category = 'FNB' THEN COUNT(c.dealId) END AS FNB_dealviews_browsed,
              CASE WHEN category = 'FNB' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS FNB_pricepoint_browsed,
              CASE WHEN category = 'GTW' THEN COUNT(c.dealId) END AS GTW_dealviews_browsed,
              CASE WHEN category = 'GTW' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS GTW_pricepoint_browsed,
              CASE WHEN category = 'TTD' THEN COUNT(c.dealId) END AS TTD_dealviews_browsed,
              CASE WHEN category = 'TTD' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS TTD_pricepoint_browsed,
              CASE WHEN category = 'HNF' THEN COUNT(c.dealId) END AS HNF_dealviews_browsed,
              CASE WHEN category = 'HNF' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS HNF_pricepoint_browsed,
              CASE WHEN category = 'LOS' THEN COUNT(c.dealId) END AS LOS_dealviews_browsed,
              CASE WHEN category = 'LOS' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS LOS_pricepoint_browsed,
              CASE WHEN category = 'MVE' THEN COUNT(c.dealId) END AS MVE_dealviews_browsed,
              CASE WHEN category = 'MVE' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS MVE_pricepoint_browsed,
              CASE WHEN category = 'ACT' THEN COUNT(c.dealId) END AS ACT_dealviews_browsed,
              CASE WHEN category = 'ACT' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS ACT_pricepoint_browsed,
              CASE WHEN category = 'HEA' THEN COUNT(c.dealId) END AS HEA_dealviews_browsed,
              CASE WHEN category = 'HEA' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS HEA_pricepoint_browsed,
              CASE WHEN category = 'HNL' THEN COUNT(c.dealId) END AS HNL_dealviews_browsed,
              CASE WHEN category = 'HNL' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS HNL_pricepoint_browsed,
              CASE WHEN category = 'BNS' THEN COUNT(c.dealId) END AS BNS_dealviews_browsed,
              CASE WHEN category = 'BNS' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS BNS_pricepoint_browsed,
              CASE WHEN category = 'SNM' THEN COUNT(c.dealId) END AS SNM_dealviews_browsed,
              CASE WHEN category = 'SNM' THEN NTH(51,QUANTILES(avg_deal_msp,101)) END AS SNM_pricepoint_browsed,
      from  (SELECT 
              Customer_ID as customerid,
              dealID,
              Category
            FROM [big-query-1233:engg_reporting.user_attributes_ga_group_C_effective]
            where action_type='2'
            ) as c
      left join [engg_reporting.user_attributes_deal_pricepoint] as o
          on (c.dealid=o.dealid)
      group by 1, category
      )
group by 1";

      


v_destination_tbl="${v_dataset_name}.user_attributes_user_categorywise_pricepoint";

echo -e "bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_user_categorywise_pricepoint' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_user_categorywise_pricepoint' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 7: user_attributes_user_categorywise_pricepoint




## Table 8: user_attributes_user_most_browsed_catg_pricepoint

v_query="SELECT
      customerid,
      FIRST(IF(ranka = 1, category, NULL)) AS mostBrowsedCat,
      ROUND(FIRST(IF(ranka = 1, pricePoint, NULL)), 3) AS mostBrowsedCatPricepoint,
      FIRST(IF(ranka = 2, category, NULL)) as secMostBrowsedCat,
      ROUND(FIRST(IF(ranka = 2, pricePoint, NULL)), 3) as secMostBrowsedCatPricepoint,
      FIRST(IF(ranka = 3, category, NULL)) as thirdMostBrowsedCat,
      ROUND(FIRST(IF(ranka = 3, pricePoint, NULL)), 3) as thirdMostBrowsedCatPricepoint,
FROM (SELECT
        customerid,
        category,
        count(category) AS catg_count,
        NTH(51,QUANTILES(avg_deal_msp,101)) AS pricePoint,
        DENSE_RANK() OVER (PARTITION BY customerid ORDER BY catg_count DESC) AS ranka
      FROM (SELECT 
              Customer_ID as customerid,
              dealID,
              Category
            FROM [big-query-1233:engg_reporting.user_attributes_ga_group_C_effective]
            WHERE action_type = '2'
              AND UPPER(Category) NOT LIKE '%STOREFRONT%'
            ) as c

      LEFT JOIN [engg_reporting.user_attributes_deal_pricepoint]  as o
         ON (c.dealid=o.dealid)
      GROUP BY 1,2
      )
WHERE ranka BETWEEN 1 AND 3
GROUP BY 1";

      


v_destination_tbl="${v_dataset_name}.user_attributes_user_most_browsed_catg_pricepoint";

echo -e "bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_user_most_browsed_catg_pricepoint' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_user_most_browsed_catg_pricepoint' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 8: user_attributes_user_most_browsed_catg_pricepoint




p_exit_upon_error(){

    ## Parameters
    # $1: Task Status (passed/ failed)
    # $2: Sub Task (e.g. Extraction of data, Cloud upload, metadata table creation, final table population)

    v_task_status="$1";
    v_subtask="$2";


    if [ "$v_task_status" == "failed" ] ; then
        v_log_obj_txt+=`echo "\n$(date) $(date) Task ($v_subtask) failed for GA intermediate tables refresh for User Attributes. Hence exiting."`;

        taskEndTime=`date`;

        v_task_end_epoch=`date +%s`
        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

        echo -e "\n$(date) Task ($v_subtask) failed for GA intermediate tables refresh for User Attributes. Hence exiting."

        echo -e "\n$(date): Task ($v_subtask) failed for GA intermediate tables refresh for User Attributes. Hence exiting." | mail -s "FAILED | User Attributes GA intermediate tables computation at ${v_task_start_time}" sairanganath.v@nearbuy.com 
        # harsh.choudhary@nearbuy.com mahesh.sharma@nearbuy.com

        echo -e  "\nTask started at ${v_task_start_time} and ended at ${v_task_end_time}.";

        exit 1;

    fi

}

v_dataset_name="engg_reporting";


## Table 9: user_attributes_acquisition_source_base

v_query="
SELECT Customer_ID, Session_ID, session_date, action_type, channelGrouping
FROM ( SELECT Customer_ID
              , Session_ID   
              , CAST(DATE(session_date) AS DATE) AS session_date
       FROM [ga_simplified.ga_session_history] 
       GROUP BY 1, 2, 3
       ) dim
LEFT JOIN 
(SELECT sessionid, hits_eCommerceAction_action_type AS action_type, channelGrouping
 FROM [big-query-1233:engg_reporting.user_attributes_ga_group_C_base_current]
      , [big-query-1233:engg_reporting.user_attributes_ga_group_C_base_till_Apr2017]
 WHERE hits_eCommerceAction_action_type IN ('2', '6')
  GROUP BY 1, 2, 3
       ) sess
  ON (dim.Session_ID = sess.sessionid)
GROUP BY Customer_ID, Session_ID, session_date, action_type, channelGrouping";

      


v_destination_tbl="${v_dataset_name}.user_attributes_acquisition_source_base";

echo -e "bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_acquisition_source_base' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_acquisition_source_base' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 9: user_attributes_acquisition_source_base



## Table 10: user_attributes_acquisition_source

v_query="SELECT customerid
       , FIRST(IF(action_type = '2', channelGrouping , NULL)) AS sourceOfAcquisition_session
       , FIRST(IF(action_type = '6', channelGrouping , NULL)) AS sourceOfAcquisition_txn
FROM  (SELECT customer_id as customerid
             , session_date
             , channelGrouping
             , action_type 
             , DENSE_RANK() OVER (PARTITION BY customerid, action_type ORDER BY session_date ASC) as ranka
      FROM [engg_reporting.user_attributes_acquisition_source_base]
      WHERE action_type IN ('2', '6')
      )
WHERE  ranka = 1
GROUP BY customerid";

      


v_destination_tbl="${v_dataset_name}.user_attributes_acquisition_source";

echo -e "bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_acquisition_source' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_acquisition_source' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 10: user_attributes_acquisition_source



## Table 11: user_attributes_recent_search

v_query="select
  a.customer_Id as customerid,
  a.firstSessionDate as firstSessionDate,
  a.latestSessionDate as latestSessionDate,
  a.activeDays as activeDays,
  a.sessionsPerActiveDay as sessionsPerActiveDay,
  a.platformAffinity as platformAffinity,
  a.AM_6_PM_6 as AM_6_PM_6,
  a.PM_6_AM_6 as PM_6_AM_6,
  a2.isActive_last_7_days AS isActive_last_7_days, 
  a2.isActive_last_30_days AS isActive_last_30_days, 
  a2.isActive_last_60_days AS isActive_last_60_days, 
  a2.isActive_last_90_days AS isActive_last_90_days,
  b.twoG as twoG,
  b.threeG as threeG,
  b.fourG as fourG,
  b.wifi as wifi,
  COALESCE(d.percSessionsNearMe, 0.0) as percSessionsNearMe,
  c.city as city,
  c.operatingSystem as operatingSystem, 
  c.operatingSystemVersion as operatingSystemVersion,
  c.screenResolution as screenResolution,
  c.mobileDeviceInfo as mobileDeviceInfo,
  i.sourceOfAcquisition_session as sourceOfAcquisition_session,
  i.sourceOfAcquisition_txn as sourceOfAcquisition_txn,
  d.latestSearchKeyword as latestSearchKeyword,
  d.secLatestSearchKeyword as secLatestSearchKeyword,
  d.thirdLatestSearchKeyword as thirdLatestSearchKeyword,
  e.mostSearchKeyword as mostSearchKeyword,
  e.secMostSearchKeyword as secMostSearchKeyword,
  e.thirdMostSearchKeyword as thirdMostSearchKeyword,
  f.mostBrowsedMerchant as mostBrowsedMerchant,
  f.secMostBrowsedMerchant as secMostBrowsedMerchant,
  f.thirdMostBrowsedMerchant as thirdMostBrowsedMerchant,
  g.mostBrowsedCat as mostBrowsedCat,
  g.mostBrowsedCatPricepoint as mostBrowsedCatPricepoint,
  g.secMostBrowsedCat as secMostBrowsedCat,
  g.secMostBrowsedCatPricepoint as secMostBrowsedCatPricepoint,
  g.thirdMostBrowsedCat as thirdMostBrowsedCat,
  g.thirdMostBrowsedCatPricepoint as thirdMostBrowsedCatPricepoint,
  h.LOR_dealviews_browsed as LOR_dealviews_browsed,
  h.LOR_pricepoint_browsed as LOR_pricepoint_browsed,
  h.SNS_dealviews_browsed as SNS_dealviews_browsed,
  h.SNS_pricepoint_browsed as SNS_pricepoint_browsed,
  h.FNB_dealviews_browsed as FNB_dealviews_browsed,
  h.FNB_pricepoint_browsed as FNB_pricepoint_browsed,
  h.GTW_dealviews_browsed as GTW_dealviews_browsed,
  h.GTW_pricepoint_browsed as GTW_pricepoint_browsed,
  h.TTD_dealviews_browsed as TTD_dealviews_browsed,
  h.TTD_pricepoint_browsed as TTD_pricepoint_browsed, 
  h.HNF_dealviews_browsed as HNF_dealviews_browsed, 
  h.HNF_pricepoint_browsed as HNF_pricepoint_browsed,
  h.LOS_dealviews_browsed as LOS_dealviews_browsed,
  h.LOS_pricepoint_browsed as LOS_pricepoint_browsed,
  h.MVE_dealviews_browsed as MVE_dealviews_browsed, 
  h.MVE_pricepoint_browsed as MVE_pricepoint_browsed,
  h.ACT_dealviews_browsed as ACT_dealviews_browsed,
  h.ACT_pricepoint_browsed as ACT_pricepoint_browsed,
  h.HEA_dealviews_browsed as HEA_dealviews_browsed,
  h.HEA_pricepoint_browsed as HEA_pricepoint_browsed,
  h.HNL_dealviews_browsed as HNL_dealviews_browsed,
  h.HNL_pricepoint_browsed as HNL_pricepoint_browsed,
  h.BNS_dealviews_browsed as BNS_dealviews_browsed,
  h.BNS_pricepoint_browsed as BNS_pricepoint_browsed,
  h.SNM_dealviews_browsed as SNM_dealviews_browsed,
  h.SNM_pricepoint_browsed as SNM_pricepoint_browsed
from
[engg_reporting.user_attributes_ga_platform_daytime_affinity] a

LEFT JOIN [engg_reporting.user_attributes_ga_active_since_days] a2
on (a.Customer_ID=a2.Customer_ID)

left join [engg_reporting.user_attributes_ga_browser_sessions] b
on (a.Customer_ID=b.Customer_ID)

left join [engg_reporting.user_attributes_ga_latest_mob_info] as c
on (a.customer_ID=c.customerid)

left join [engg_reporting.user_attributes_recent_search] as d
on (a.customer_id=d.customerid)

left join [engg_reporting.user_attributes_frequency_search] as e
on (a.customer_id=e.customerid)

left join [engg_reporting.user_attributes_top_deals_browsed] f
on (a.customer_id=f.customerid)

left join [engg_reporting.user_attributes_user_most_browsed_catg_pricepoint] as g
on (a.customer_id=g.customerid)

left join [engg_reporting.user_attributes_categorywise_pricepoint] as h
on (a.customer_id=h.customerid)

left join [engg_reporting.user_attributes_acquisition_source] as  i  
on (a.customer_id=i.customerid) ";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_combined";

echo -e "bq query --maximum_billing_tier 100000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA intermediate Table 'user_attributes_recent_search' : $v_task_status";


v_subtask="GA intermediate Table 'user_attributes_recent_search' ";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 11: user_attributes_recent_search

###---------######---------######---------######---------######---------######---------###
	###---------######---------### ## Final Table ## ###---------######---------###
###---------######---------######---------######---------######---------######---------###
p_exit_upon_error(){

    ## Parameters
    # $1: Task Status (passed/ failed)
    # $2: Sub Task (e.g. Extraction of data, Cloud upload, metadata table creation, final table population)

    v_task_status="$1";
    v_subtask="$2";


    if [ "$v_task_status" == "failed" ] ; then
        v_log_obj_txt+=`echo "\n$(date) $(date) Task ($v_subtask) failed for Final table (GA table Joining with non-GA table) data. Hence exiting."`;

        taskEndTime=`date`;

        v_task_end_epoch=`date +%s`
        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

        echo -e "\n$(date) Task ($v_subtask) failed for Final table (GA table Joining with non-GA table) data. Hence exiting."

        echo -e "\n$(date): Task ($v_subtask) failed for Final table (GA table Joining with non-GA table) data. Hence exiting." | mail -s "FAILED | User Attributes computation at ${v_task_start_time}" sairanganath.v@nearbuy.com 
        # harsh.choudhary@nearbuy.com mahesh.sharma@nearbuy.com

        echo -e  "\nTask started at ${v_task_start_time} and ended at ${v_task_end_time}.";

        exit 1;

    fi

}

v_dataset_name="engg_reporting";


v_query="SELECT 
non_ga.customerid AS customerid
, non_ga.name AS name
, non_ga.gender AS gender
, non_ga.dob_day AS dob_day
, non_ga.dob_month AS dob_month
, non_ga.dob_year AS dob_year
, non_ga.raffleFirstPurchaseDate AS raffleFirstPurchaseDate
, non_ga.nonRaffleFirstPurchaseDate AS nonRaffleFirstPurchaseDate
, non_ga.raffleLastPurchaseDate AS raffleLastPurchaseDate
, non_ga.nonRaffleLastPurchaseDate AS nonRaffleLastPurchaseDate
, non_ga.totalNonRaffleTxn AS totalNonRaffleTxn,
, non_ga.totalRaffleTxn AS totalRaffleTxn,
, non_ga.totalNonRaffleVouchers AS totalNonRaffleVouchers,
, non_ga.totalRaffleVouchers AS totalRaffleVouchers
, non_ga.percDiscountAffinty AS percDiscountAffinty
, non_ga.isBuffet AS isBuffet
, non_ga.favbuffetdeal AS favbuffetdeal
, non_ga.isBrunch AS isBrunch
, non_ga.favbrunchdeal AS favbrunchdeal
, non_ga.isDesserts AS isDesserts
, non_ga.favdessertdeal AS favdessertdeal
, non_ga.isBreakfast AS isBreakfast
, non_ga.isLunch AS isLunch
, non_ga.isDinner AS isDinner
, non_ga.totalGB AS totalGB
, non_ga.totalGR AS totalGR
, non_ga.cashback AS cashback
, non_ga.GB_afterFirstCB AS GB_afterFirstCB
, non_ga.GR_afterFirstCB AS GR_afterFirstCB
, non_ga.weekendPurchase AS weekendPurchase
, non_ga.weekdayPurchase AS weekdayPurchase
, non_ga.unredeemedVouchers AS unredeemedVouchers
, non_ga.redeemtimediff_hours AS redeemtimediff_hours
, non_ga.cancellations AS cancellations
, non_ga.redeemed AS redeemed
, non_ga.refunds AS refunds
, non_ga.expired AS expired
, non_ga.validForOneTx AS validForOneTx
, non_ga.validForTwoTx AS validForTwoTx
, non_ga.validForMultipleTx AS validForMultipleTx
, non_ga.LatestTxnMerchant AS LatestTxnMerchant
, non_ga.latestTxnCategory AS latestTxnCategory
, non_ga.latestTxnPricePoint AS latestTxnPricePoint
, non_ga.secLatestTxnMerchant AS secLatestTxnMerchant
, non_ga.seclatestTxnCategory AS seclatestTxnCategory
, non_ga.secLatestTxnPricePoint AS secLatestTxnPricePoint
, non_ga.thirdLatestTxnMerchant AS thirdLatestTxnMerchant
, non_ga.thirdLatestTxnCategory AS thirdLatestTxnCategory
, non_ga.thirdLatestTxnPricePoint AS thirdLatestTxnPricePoint
, non_ga.MostTxnMerchant AS MostTxnMerchant
, non_ga.secondMostTxnMerchant AS secondMostTxnMerchant
, non_ga.thirdMostTxnMerchant AS thirdMostTxnMerchant
, non_ga.mostTxnCat AS mostTxnCat
, non_ga.mostTxnCatPricePoint AS mostTxnCatPricePoint
, non_ga.secMostTxnCat AS secMostTxnCat
, non_ga.secMostTxnPricePoint AS secMostTxnPricePoint
, non_ga.thirdMostTxnCat AS thirdMostTxnCat
, non_ga.thirdMostTxnPricePoint AS thirdMostTxnPricePoint
, non_ga.latestRedeemCity AS latestRedeemCity
, non_ga.secondLatRedeemCity AS secondLatRedeemCity
, non_ga.thirdLatRedeemCity AS thirdLatRedeemCity
, non_ga.latest_communication_date_PN AS latest_communication_date_PN
, non_ga.PN_delivered AS PN_delivered
, non_ga.PN_opened AS PN_opened
, non_ga.PN_dismissed AS PN_dismissed
, non_ga.PN_failed AS PN_failed
, non_ga.PN_complaint AS PN_complaint
, non_ga.PN_delivered_t_minus_15 AS PN_delivered_t_minus_15
, non_ga.PN_opened_t_minus_15 AS PN_opened_t_minus_15
, non_ga.PN_dismissed_t_minus_15 AS PN_dismissed_t_minus_15
, non_ga.PN_failed_t_minus_15 AS PN_failed_t_minus_15
, non_ga.PN_complaint_t_minus_15 AS PN_complaint_t_minus_15
, non_ga.PN_bounced AS PN_bounced
, non_ga.email_sent AS email_sent
, non_ga.email_open AS email_open
, non_ga.email_click AS email_click
, non_ga.email_sent_t_minus_15 AS email_sent_t_minus_15
, non_ga.email_open_t_minus_15 AS email_open_t_minus_15
, non_ga.email_click_t_minus_15 AS email_click_t_minus_15

, non_ga.mostVisitedPlace AS mostVisitedPlace
, non_ga.mostVisitedPlaceCity AS mostVisitedPlaceCity
, non_ga.mostVisitedTimes AS mostVisitedTimes
, non_ga.totalMerchantsAtMostVisitedPlace AS totalMerchantsAtMostVisitedPlace
, non_ga.sellingMerchantsAtMostVisitedPlace AS sellingMerchantsAtMostVisitedPlace
, non_ga.mostVisitedPlacePolygonId AS mostVisitedPlacePolygonId

, non_ga.secMostVisitedPlace AS secMostVisitedPlace
, non_ga.secMostVisitedPlaceCity AS secMostVisitedPlaceCity
, non_ga.secMostVisitedTimes AS secMostVisitedTimes
, non_ga.totalMerchantsAtSecMostVisitedPlace AS totalMerchantsAtSecMostVisitedPlace
, non_ga.sellingMerchantsAtSecMostVisitedPlace AS sellingMerchantsAtSecMostVisitedPlace
, non_ga.secMostVisitedPlacePolygonId AS secMostVisitedPlacePolygonId

, non_ga.thirdMostVisitedPlace AS thirdMostVisitedPlace
, non_ga.thirdMostVisitedPlaceCity AS thirdMostVisitedPlaceCity
, non_ga.thirdMostVisitedTimes AS thirdMostVisitedTimes
, non_ga.totalMerchantsAtThirdMostVisitedPlace AS totalMerchantsAtThirdMostVisitedPlace
, non_ga.sellingMerchantsAtThirdMostVisitedPlace AS sellingMerchantsAtThirdMostVisitedPlace
, non_ga.thirdMostVisitedPlacePolygonId AS thirdMostVisitedPlacePolygonId


, non_ga.totalAvgRating AS totalAvgRating
, non_ga.timesRated AS timesRated
, non_ga.mostRatedMerchant AS mostRatedMerchant
, non_ga.mostRatedMerchantRating AS mostRatedMerchantRating
, non_ga.highestAvgRatingMerchant AS highestAvgRatingMerchant
, non_ga.highestAvgRating AS highestAvgRating
, non_ga.totalcreditsavailable AS totalcreditsavailable
, non_ga.cohort AS cohort,
ga.firstSessionDate as firstSessionDate,
ga.latestSessionDate as latestSessionDate,
ga.activeDays as activeDays,
ga.sessionsPerActiveDay as sessionsPerActiveDay,
ga.platformAffinity as platformAffinity,
ga.AM_6_PM_6 as AM_6_PM_6,
ga.PM_6_AM_6 as PM_6_AM_6,
ga.twoG as twoG,
ga.threeG as threeG,
ga.fourG as fourG,
ga.wifi as wifi,
ga.percSessionsNearMe as percSessionsNearMe,
ga.city as city,
ga.operatingSystem as operatingSystem,
ga.operatingSystemVersion as operatingSystemVersion,
ga.screenResolution as screenResolution,
ga.mobileDeviceInfo as mobileDeviceInfo,
ga.sourceOfAcquisition_session as sourceOfAcquisition_session,
ga.sourceOfAcquisition_txn as sourceOfAcquisition_txn,
ga.latestSearchKeyword as latestSearchKeyword,
ga.secLatestSearchKeyword as secLatestSearchKeyword,
ga.thirdLatestSearchKeyword as thirdLatestSearchKeyword,
ga.mostSearchKeyword as mostSearchKeyword,
ga.secMostSearchKeyword as secMostSearchKeyword,
ga.thirdMostSearchKeyword as thirdMostSearchKeyword,
ga.mostBrowsedMerchant as mostBrowsedMerchant,
ga.secMostBrowsedMerchant as secMostBrowsedMerchant,
ga.thirdMostBrowsedMerchant as thirdMostBrowsedMerchant,
ga.mostBrowsedCat as mostBrowsedCat,
ga.mostBrowsedCatPricepoint as mostBrowsedCatPricepoint,
ga.secMostBrowsedCat as secMostBrowsedCat,
ga.secMostBrowsedCatPricepoint as secMostBrowsedCatPricepoint,
ga.thirdMostBrowsedCat as thirdMostBrowsedCat,
ga.thirdMostBrowsedCatPricepoint as thirdMostBrowsedCatPricepoint,
ga.LOR_dealviews_browsed as LOR_dealviews_browsed,
ga.LOR_pricepoint_browsed as LOR_pricepoint_browsed,
ga.SNS_dealviews_browsed as SNS_dealviews_browsed,
ga.SNS_pricepoint_browsed as SNS_pricepoint_browsed,
ga.FNB_dealviews_browsed as FNB_dealviews_browsed,
ga.FNB_pricepoint_browsed as FNB_pricepoint_browsed,
ga.GTW_dealviews_browsed as GTW_dealviews_browsed,
ga.GTW_pricepoint_browsed as GTW_pricepoint_browsed,
ga.TTD_dealviews_browsed as TTD_dealviews_browsed,
ga.TTD_pricepoint_browsed as TTD_pricepoint_browsed,
ga.HNF_dealviews_browsed as HNF_dealviews_browsed,
ga.HNF_pricepoint_browsed as HNF_pricepoint_browsed,
ga.LOS_dealviews_browsed as LOS_dealviews_browsed,
ga.LOS_pricepoint_browsed as LOS_pricepoint_browsed,
ga.MVE_dealviews_browsed as MVE_dealviews_browsed,
ga.MVE_pricepoint_browsed as MVE_pricepoint_browsed,
ga.ACT_dealviews_browsed as ACT_dealviews_browsed,
ga.ACT_pricepoint_browsed as ACT_pricepoint_browsed,
ga.HEA_dealviews_browsed as HEA_dealviews_browsed,
ga.HEA_pricepoint_browsed as HEA_pricepoint_browsed,
ga.HNL_dealviews_browsed as HNL_dealviews_browsed,
ga.HNL_pricepoint_browsed as HNL_pricepoint_browsed,
ga.BNS_dealviews_browsed as BNS_dealviews_browsed,
ga.BNS_pricepoint_browsed as BNS_pricepoint_browsed,
ga.SNM_dealviews_browsed as SNM_dealviews_browsed,
ga.SNM_pricepoint_browsed as SNM_pricepoint_browsed,

FROM  [big-query-1233:engg_reporting.user_attributes_non_GA] non_ga
LEFT JOIN [big-query-1233:engg_reporting.user_attributes_ga_combined] ga
on (non_ga.customerid=ga.customerid)";


v_destination_tbl="${v_dataset_name}.user_attributes_final";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating Final Table  'user_attributes_final' : $v_task_status";


v_subtask="Final Table  'user_attributes_final' ";
p_exit_upon_error "$v_task_status" "$v_subtask";


v_task_end_time=`date`;


echo "Task started at ${v_task_start_time} and ended at ${v_task_end_time}.";

exit 0;

