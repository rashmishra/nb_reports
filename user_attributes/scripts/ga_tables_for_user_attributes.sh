#!/bin/bash

## Script Name: ga_tables_for_user_attributes.sh

## This script is written to automate the queries required to compute the 
## user attributes on a daily basis. Sources to derive these attributes are 
## GA, Customer Profile, OMS, Merchant, Deals and Offers, Ratings and Reviews and user credit summary (etc.)

v_task_start_time=`date`;

echo "Task Started at ${v_task_start_time}";




p_exit_upon_error(){

    ## Parameters
    # $1: Task Status (passed/ failed)
    # $2: Sub Task (e.g. Extraction of data, Cloud upload, metadata table creation, final table population)

    v_task_status="$1";
    v_subtask="$2";


    if [ "$v_task_status" == "failed" ] ; then
        v_log_obj_txt+=`echo "\n$(date) $(date) Task ($v_subtask) failed for GA tables refresh for User Attributes computation. Hence exiting."`;

        taskEndTime=`date`;

        v_task_end_epoch=`date +%s`
        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

        echo -e "\n$(date) Task ($v_subtask) failed for GA tables refresh for User Attributes computation. Hence exiting."

        echo -e "\n$(date): Task ($v_subtask) failed for GA tables refresh for User Attributes computation. Hence exiting." | mail -s "FAILED | User Attributes computation at ${v_task_start_time}" sairanganath.v@nearbuy.com # harsh.choudhary@nearbuy.com mahesh.sharma@nearbuy.com

        echo -e  "\nTask started at ${v_task_start_time} and ended at ${v_task_end_time}.";

        exit 1;

    fi

}

v_dataset_name="engg_reporting";


##########################################################################################
################################# GA BASE TABLES REFRESH #################################
##########################################################################################


## Table 1: dim_ga_customer_last_session
## Pulls effective date range of GA Sessions for each user

v_query="SELECT Customer_ID, MIN(CAST(DATE(first_session_date) AS DATE)) first_session_date
       , MAX(CAST(DATE(MAX( session_date)) AS DATE)) AS latest_session_date
       , MAX( session_date) AS latest_session_date_int
       , CAST(DATE(DATE_ADD(DATE(MAX( session_date)), -91, 'DAY')) AS DATE) as effective_since_session_date
       , STRING(CAST(DATE(DATE_ADD(DATE(MAX( session_date)), -91, 'DAY')) AS DATE)) as effective_since_session_date_int
FROM [ga_simplified.ga_session_history] 
GROUP BY 1";

v_destination_tbl="${v_dataset_name}.dim_ga_customer_last_session";

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

echo `date` "Creating dim_ga_customer_last_session: $v_task_status";


v_subtask="User Attributes Step 1: dim_ga_customer_last_session creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 1: dim_ga_customer_last_session



## Table 2: user_attributes_ga_session_history
                  # Effective ga_session_history (latest 3 months of activity by user)

v_query="SELECT gas.source AS source
       , gas.Customer_ID AS Customer_ID
       , gas.first_session_date AS first_session_date
       , gas.Session_ID AS Session_ID
       , gas.session_date AS session_date
       , gas.session_start_time AS session_start_time
       , dim.effective_since_session_date AS effective_since_session_date
       , dim.latest_session_date AS latest_session_date
FROM [engg_reporting.dim_ga_customer_last_session] dim
INNER JOIN [ga_simplified.ga_session_history] gas
    ON dim.Customer_ID = gas.Customer_ID
WHERE DATE(gas.session_date) BETWEEN DATE(dim.effective_since_session_date) AND DATE(dim.latest_session_date)";

v_destination_tbl="${v_dataset_name}.user_attributes_ga_session_history";

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

echo `date` "Creating user_attributes_ga_session_history: $v_task_status";


v_subtask="User Attributes Step 2: user_attributes_ga_session_history creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 2: user_attributes_ga_session_history



v_task_end_time=`date`;


echo "Task started at ${v_task_start_time} and ended at ${v_task_end_time}.";

exit 0;