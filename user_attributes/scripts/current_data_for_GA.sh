#!/bin/bash

## Current data fetching for A, B and C group tables


v_task_start_time=`date`;

echo "Task Started at ${v_task_start_time}";



p_exit_upon_error(){

    ## Parameters
    # $1: Task Status (passed/ failed)
    # $2: Sub Task (e.g. Extraction of data, Cloud upload, metadata table creation, final table population)

    v_task_status="$1";
    v_subtask="$2";


    if [ "$v_task_status" == "failed" ] ; then
        v_log_obj_txt+=`echo "\n$(date) $(date) Task ($v_subtask) failed for GA tables current data refresh for User Attributes Table Groups A, B, C and D. Hence exiting."`;

        taskEndTime=`date`;

        v_task_end_epoch=`date +%s`
        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

        echo -e "\n$(date) Task ($v_subtask) failed for GA tables current data refresh for User Attributes Table Groups A, B, C and D. Hence exiting."

        echo -e "\n$(date): Task ($v_subtask) failed for GA tables current data refresh for User Attributes Table Groups A, B, C and D. Hence exiting." | mail -s "FAILED | User Attributes computation at ${v_task_start_time}" sairanganath.v@nearbuy.com 
        # harsh.choudhary@nearbuy.com mahesh.sharma@nearbuy.com

        echo -e  "\nTask started at ${v_task_start_time} and ended at ${v_task_end_time}.";

        exit 1;

    fi

}

v_dataset_name="engg_reporting";


###########################################################################################
###################################### Table group A ######################################
###########################################################################################

##-------## ##-------## 2017 May - Current Date ##-------## ##-------##


### Web
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'Web' AS Platform
       , date
       , hits.page.searchKeyword AS searchKeyword
       , geoNetwork.city  AS locationServices

FROM TABLE_DATE_RANGE([108795712.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
WHERE geoNetwork.city IS NOT NULL
   OR hits.page.searchKeyword IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_A_base_current";

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

echo `date` "Creating GA current data for Table Group A 'user_attributes_ga_group_A_base_current' May 17 - today Web: $v_task_status";


v_subtask="GA current data for Table Group A 'user_attributes_ga_group_A_base_current' May 17 - today Web";
p_exit_upon_error "$v_task_status" "$v_subtask";


### iOS
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'iOS' AS Platform
       , date
       , FIRST(IF(hits.product.customDimensions.index = 90, hits.product.customDimensions.value, NULL)) AS searchKeyword
       , FIRST(IF(hits.product.customDimensions.index = 94, hits.product.customDimensions.value, NULL)) AS locationServices

FROM TABLE_DATE_RANGE([118341991.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
WHERE hits.product.customDimensions.index IN (90, 94)
GROUP BY 1, 2, 3, 4";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_A_base_current";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA current data for Table Group A 'user_attributes_ga_group_A_base_current' May 17 - today iOS: $v_task_status";


v_subtask="GA current data for Table Group A 'user_attributes_ga_group_A_base_current' May 17 - today iOS";
p_exit_upon_error "$v_task_status" "$v_subtask";


### Android
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'Android' AS Platform
       , date
       , FIRST(IF(hits.product.customDimensions.index = 90, hits.product.customDimensions.value, NULL)) AS searchKeyword
       , FIRST(IF(hits.product.customDimensions.index = 94, hits.product.customDimensions.value, NULL)) AS locationServices

FROM TABLE_DATE_RANGE([118356700.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
WHERE hits.product.customDimensions.index IN (90, 94)
GROUP BY 1, 2, 3, 4";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_A_base_current";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA current data for Table Group A 'user_attributes_ga_group_A_base_current' May 17 - today Android: $v_task_status";


v_subtask="GA current data for Table Group A 'user_attributes_ga_group_A_base_current' May 17 - today Android";
p_exit_upon_error "$v_task_status" "$v_subtask";



### mWeb
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'Web' AS Platform
       , date
       , hits.page.searchKeyword AS searchKeyword
       , geoNetwork.city  AS locationServices

FROM TABLE_DATE_RANGE([157529880.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
WHERE geoNetwork.city IS NOT NULL
   OR hits.page.searchKeyword IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_A_base_current";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA current data for Table Group A 'user_attributes_ga_group_A_base_current' May 17 - today mWeb: $v_task_status";






echo "Table Group A Current data garnering completed at " `date`;

###########################################################################################
############################### End of Table group A ######################################
###########################################################################################


###########################################################################################
###################################### Table group B ######################################
###########################################################################################

##-------## ##-------## 2017 May - Current Date ##-------## ##-------##

### Web
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'Web' AS Platform
       , date AS date
       , device.browser AS deviceBrowser
       , geoNetwork.city as city
       , device.operatingSystem AS operatingSystem
       , device.operatingSystemVersion AS operatingSystemVersion
       , device.screenResolution AS screenResolution
       , device.mobileDeviceInfo AS mobileDeviceInfo
       , FIRST(IF(customDimensions.index = 53, customDimensions.value, NULL)) AS userConnection
       , FIRST(IF(customDimensions.index = 7, customDimensions.value, NULL)) AS Customer_ID 

FROM TABLE_DATE_RANGE([108795712.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_B_base_current";

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

echo `date` "Creating GA current data for Table Group B 'user_attributes_ga_group_B_base_current' May 17 - today Web: $v_task_status";


v_subtask="GA current data for Table Group B 'user_attributes_ga_group_B_base_current' May 17 - today Web";
p_exit_upon_error "$v_task_status" "$v_subtask";


### mWeb
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'Web' AS Platform
       , date AS date
       , device.browser AS deviceBrowser
       , geoNetwork.city as city
       , device.operatingSystem AS operatingSystem
       , device.operatingSystemVersion AS operatingSystemVersion
       , device.screenResolution AS screenResolution
       , device.mobileDeviceInfo AS mobileDeviceInfo
       , FIRST(IF(customDimensions.index = 53, customDimensions.value, NULL)) AS userConnection
       , FIRST(IF(customDimensions.index = 7, customDimensions.value, NULL)) AS Customer_ID 

FROM TABLE_DATE_RANGE([157529880.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_B_base_current";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA current data for Table Group B 'user_attributes_ga_group_B_base_current' May 17 - today mWeb: $v_task_status";


v_subtask="GA current data for Table Group B 'user_attributes_ga_group_B_base_current' May 17 - today Web";
p_exit_upon_error "$v_task_status" "$v_subtask";


### iOS
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'iOS' AS Platform
       , date AS date
       , device.browser AS deviceBrowser
       , geoNetwork.city as city
       , device.operatingSystem AS operatingSystem
       , device.operatingSystemVersion AS operatingSystemVersion
       , device.screenResolution AS screenResolution
       , device.mobileDeviceInfo AS mobileDeviceInfo
       , FIRST(IF(customDimensions.index = 53, customDimensions.value, NULL)) AS userConnection
       , FIRST(IF(customDimensions.index = 7, customDimensions.value, NULL)) AS Customer_ID 

FROM TABLE_DATE_RANGE([118341991.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_B_base_current";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA current data for Table Group B 'user_attributes_ga_group_B_base_current' May 17 - today iOS: $v_task_status";


v_subtask="GA current data for Table Group B 'user_attributes_ga_group_B_base_current' May 17 - today iOS";
p_exit_upon_error "$v_task_status" "$v_subtask";


### Android
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'Android' AS Platform
       , date AS date
       , device.browser AS deviceBrowser
       , geoNetwork.city as city
       , device.operatingSystem AS operatingSystem
       , device.operatingSystemVersion AS operatingSystemVersion
       , device.screenResolution AS screenResolution
       , device.mobileDeviceInfo AS mobileDeviceInfo
       , FIRST(IF(customDimensions.index = 53, customDimensions.value, NULL)) AS userConnection
       , FIRST(IF(customDimensions.index = 7, customDimensions.value, NULL)) AS Customer_ID 

FROM TABLE_DATE_RANGE([118356700.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_B_base_current";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA current data for Table Group B 'user_attributes_ga_group_B_base_current' May 17 - today Android: $v_task_status";


v_subtask="GA current data for Table Group B 'user_attributes_ga_group_B_base_current' May 17 - today Android";
p_exit_upon_error "$v_task_status" "$v_subtask";








echo "Table Group B current data garnering completed at " `date`;

###########################################################################################
############################### End of Table group B ######################################
###########################################################################################




###########################################################################################
###################################### Table group C ######################################
###########################################################################################

##-------## ##-------## 2017 May - Current Date ##-------## ##-------##


### Web
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'Web' AS Platform
       , date
       , hits.eCommerceAction.action_type 
       , hits.product.productSKU AS dealID
       , hits.product.v2ProductCategory AS Category
       , geoNetwork.city AS city
       , channelGrouping

FROM TABLE_DATE_RANGE([108795712.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
WHERE hits.eCommerceAction.action_type  IN  ( '2', '6')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_C_base_current";

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

echo `date` "Creating GA current data for Table Group C 'user_attributes_ga_group_C_base_current' May 17 - today Web: $v_task_status";


v_subtask="GA current data for Table Group C 'user_attributes_ga_group_C_base_current' May 17 - today Web";
p_exit_upon_error "$v_task_status" "$v_subtask";


### mWeb
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'Web' AS Platform
       , date
       , hits.eCommerceAction.action_type 
       , hits.product.productSKU AS dealID
       , hits.product.v2ProductCategory AS Category
       , geoNetwork.city AS city
       , channelGrouping

FROM TABLE_DATE_RANGE([157529880.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
WHERE hits.eCommerceAction.action_type  IN  ( '2', '6')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9";

v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_C_base_current";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA current data for Table Group C 'user_attributes_ga_group_C_base_current' May 17 - today Web: $v_task_status";


v_subtask="GA current data for Table Group C 'user_attributes_ga_group_C_base_current' May 17 - today Web";
p_exit_upon_error "$v_task_status" "$v_subtask";


### iOS
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'iOS' AS Platform
       , date
       , hits.eCommerceAction.action_type 
       , hits.product.productSKU AS dealID
       , hits.product.v2ProductCategory AS Category
       , geoNetwork.city AS city
       , channelGrouping

FROM TABLE_DATE_RANGE([118341991.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
WHERE hits.eCommerceAction.action_type  IN  ( '2', '6')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_C_base_current";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA current data for Table Group C 'user_attributes_ga_group_C_base_current' May 17 - today iOS: $v_task_status";


v_subtask="GA current data for Table Group C 'user_attributes_ga_group_C_base_current' May 17 - today iOS";
p_exit_upon_error "$v_task_status" "$v_subtask";


### Android
v_query="SELECT CONCAT(fullVisitorId,STRING(visitId)) AS sessionid
       , visitStartTime AS visitStartTime
       , 'Android' AS Platform
       , date
       , hits.eCommerceAction.action_type 
       , hits.product.productSKU AS dealID
       , hits.product.v2ProductCategory AS Category
       , geoNetwork.city AS city
       , channelGrouping

FROM TABLE_DATE_RANGE([118356700.ga_sessions_], TIMESTAMP(DATE('2017-05-01')), TIMESTAMP(CURRENT_DATE()) )
WHERE hits.eCommerceAction.action_type  IN  ( '2', '6')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_C_base_current";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --append -n 1 --destination_table=$v_destination_tbl "${v_query}"& 
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating GA current data for Table Group C 'user_attributes_ga_group_C_base_current' May 17 - today Android: $v_task_status";


v_subtask="GA current data for Table Group C 'user_attributes_ga_group_C_base_current' May 17 - today Android";
p_exit_upon_error "$v_task_status" "$v_subtask";








echo "Table Group C current data garnering completed at " `date`;

###########################################################################################
############################### End of Table group C ######################################
###########################################################################################

###--------------######--------------######--------------######--------------######--------------###
 ###--------------### End of Current Data for GA. Now fetching effective data ###--------------###
###--------------######--------------######--------------######--------------######--------------###

p_exit_upon_error(){

    ## Parameters
    # $1: Task Status (passed/ failed)
    # $2: Sub Task (e.g. Extraction of data, Cloud upload, metadata table creation, final table population)

    v_task_status="$1";
    v_subtask="$2";


    if [ "$v_task_status" == "failed" ] ; then
        v_log_obj_txt+=`echo "\n$(date) $(date) Task ($v_subtask) failed for GA tables effective data refresh for User Attributes Table Groups A, B, C and D. Hence exiting."`;

        taskEndTime=`date`;

        v_task_end_epoch=`date +%s`
        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

        echo -e "\n$(date) Task ($v_subtask) failed for GA tables effective data refresh for User Attributes Table Groups A, B, C and D. Hence exiting."

        echo -e "\n$(date): Task ($v_subtask) failed for GA tables effective data refresh for User Attributes Table Groups A, B, C and D. Hence exiting." | mail -s "FAILED | User Attributes computation at ${v_task_start_time}" sairanganath.v@nearbuy.com 
        # harsh.choudhary@nearbuy.com mahesh.sharma@nearbuy.com

        echo -e  "\nTask started at ${v_task_start_time} and ended at ${v_task_end_time}.";

        exit 1;

    fi

}

v_dataset_name="engg_reporting";


# Group C Effective Base

v_query="SELECT  dim.Customer_ID AS Customer_ID
        , dim.first_session_date AS first_session_date
        , dim.effective_since_session_date AS effective_since_session_date
        , dim.latest_session_date AS latest_session_date
        , c.sessionid AS sessionid
        , c.visitStartTime AS visitStartTime
        , c.Platform AS Platform 
        , c.date AS date 
        , c.hits_eCommerceAction_action_type as action_type
        , c.dealID AS dealID 
        , c.Category AS Category 
        , c.city AS city 
        , c.channelGrouping AS channelGrouping
        , merc.Merchant_ID AS Merchant_ID
        , FIRST(merc.Merchant_Name) AS Merchant_Name
FROM [engg_reporting.user_attributes_ga_session_history] dim
LEFT JOIN (SELECT *
           FROM engg_reporting.user_attributes_ga_group_C_base_current cur, 
                [engg_reporting.user_attributes_ga_group_C_base_till_Apr2017] old
) c
ON dim.Session_ID = c.sessionid
AND dim.source = c.Platform
LEFT JOIN [engg_reporting.user_attributes_merchant_name] merc
    ON c.dealID = merc.Deal_ID
WHERE DATE(c.date) BETWEEN DATE(dim.effective_since_session_date) AND DATE( dim.latest_session_date)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_C_effective_base";

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

echo `date` "Creating GA effective data for Table Group C 'user_attributes_ga_group_C_effective_base' : $v_task_status";


v_subtask="GA effective data for Table Group C 'user_attributes_ga_group_C_effective_base' ";
p_exit_upon_error "$v_task_status" "$v_subtask";


# Group C
v_query="SELECT Customer_ID, first_session_date, effective_since_session_date
       , latest_session_date, sessionid, visitStartTime
       , Platform, date, action_type, dealID, Category, city, channelGrouping
       , COALESCE(ch.Chain_Merchant_ID, ch.Merchant_ID) AS Merchant_ID, Merchant_Name
FROM ${v_dataset_name}.user_attributes_ga_group_C_effective_base base
LEFT JOIN [${v_dataset_name}.merchant_with_chain_id] ch
    ON base.dealID = ch.Deal_ID";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_C_effective";

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

echo `date` "Creating GA effective data for Table Group C 'user_attributes_ga_group_C_effective' : $v_task_status";


v_subtask="GA effective data for Table Group C 'user_attributes_ga_group_C_effective' ";
p_exit_upon_error "$v_task_status" "$v_subtask";



# GROUP B 

v_query="SELECT  dim.Customer_ID AS Customer_ID
        , dim.first_session_date AS first_session_date
        , dim.effective_since_session_date AS effective_since_session_date
        , dim.latest_session_date AS latest_session_date
        , b.sessionid AS sessionid
        , b.visitStartTime AS visitStartTime
        , b.Platform AS Platform 
        , b.date AS date 
        , b.deviceBrowser as deviceBrowser
        , b.city AS city 
        , b.operatingSystem AS operatingSystem
        , b.operatingSystemVersion AS operatingSystemVersion
        , b.screenResolution AS screenResolution
        , b.mobileDeviceInfo AS mobileDeviceInfo
        , b.userConnection AS userConnection
FROM [engg_reporting.user_attributes_ga_session_history] dim
LEFT JOIN (SELECT *
           FROM engg_reporting.user_attributes_ga_group_B_base_current cur, 
                [engg_reporting.user_attributes_ga_group_B_base_till_Apr2017] old
) b
ON dim.Session_ID = b.sessionid
AND dim.source = b.Platform
AND dim.Customer_ID = b.Customer_ID
WHERE DATE(b.date) BETWEEN DATE(dim.effective_since_session_date) AND DATE( dim.latest_session_date)";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_B_effective";

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

echo `date` "Creating GA effective data for Table GROUP B 'user_attributes_ga_group_B_effective' : $v_task_status";


v_subtask="GA effective data for Table GROUP B 'user_attributes_ga_group_B_effective' ";
p_exit_upon_error "$v_task_status" "$v_subtask";


# Group A 

v_query="SELECT  dim.Customer_ID AS Customer_ID
        , dim.first_session_date AS first_session_date
        , dim.effective_since_session_date AS effective_since_session_date
        , dim.latest_session_date AS latest_session_date
        , a.sessionid AS sessionid
        , a.visitStartTime AS visitStartTime
        , a.Platform AS Platform 
        , a.date AS date 
        , a.searchKeyword AS searchKeyword
        , a.locationServices AS locationServices
FROM [engg_reporting.user_attributes_ga_session_history] dim
LEFT JOIN (SELECT *
           FROM engg_reporting.user_attributes_ga_group_A_base_current cur, 
                [engg_reporting.user_attributes_ga_group_A_base_till_Apr2017] old
) a
ON dim.Session_ID = a.sessionid
AND dim.source = a.Platform
WHERE DATE(a.date) BETWEEN DATE(dim.effective_since_session_date) AND DATE( dim.latest_session_date)";


v_destination_tbl="${v_dataset_name}.user_attributes_ga_group_A_effective";

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

echo `date` "Creating GA effective data for Table Group A 'user_attributes_ga_group_A_effective' : $v_task_status";


v_subtask="GA effective data for Table Group A 'user_attributes_ga_group_A_effective' ";
p_exit_upon_error "$v_task_status" "$v_subtask";


# End of Group A 

###--------------######--------------######--------------######--------------######--------------###
          ###--------------### End of fetching effective data ###--------------###
###--------------######--------------######--------------######--------------######--------------###


v_task_end_time=`date`;

echo "Script started at ${v_task_start_time} and ended at ${v_task_end_time}" 

exit 0
