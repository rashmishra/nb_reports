ta_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=nb_reports;
date


# downstream_appsflyer tableloading. Replace existing
v_query_downstream_appsflyer="SELECT --23567  --1146566
       a.order_id_new as af_order_id,
       a.imei as af_imei_number,
      a.customer_user_id as af_customer_id,
      a.attribution_type as af_attribution_type,
      a.media_source as af_media_source,
      a.appsflyer_device_id as af_device_id,
      a.fb_adgroup_id as af_fb_adgroup_id,
      a.download_time as af_download_time,
      a.fb_campaign_id as af_fb_campaign_id,
      a.is_retargeting as af_is_retargeting,
      a.re_targeting_conversion_type as af_re_targeting_conversion_type,
      a.android_id as af_android_id,
      a.fb_adset_name as af_fb_adset_name,
      a.campaign as af_campaign_name,
      a.install_time as af_install_time,
      a.platform as af_platform_type,
      a.fb_campaign_name as af_fb_campaign_name,
      a.app_version as af_app_version,
      a.os_version as af_os_version,
      a.fb_adset_id as af_fb_adset_id,
      a.event_type as af_event_type,
      a.af_ad as af_ad_name,
      a.af_ad_id as af_ad_id,
      a.af_ad_type as af_ad_type,
      a.af_adset as af_adset_name,
      a.af_adset_id as af_adset_id,
      a.af_c_id as af_campaign_id,
      a.af_channel as af_channel,
  
  RANK() OVER (PARTITION BY a.order_id_new ORDER BY install_time ASC) rank,
  CASE
    WHEN y.count > 1 THEN 'Reattributed'
    ELSE 'NA'
  END AS af_reaatributed_flag
FROM (
  SELECT
    COALESCE(ap.order_id,ol.orderid,ap.af_receipt_id,ap.af_content_id) AS order_id_new,
        ap.imei as imei,
      ap.customer_user_id as customer_user_id,
      ap.attribution_type as attribution_type,
      ap.media_source as media_source,
      ap.appsflyer_device_id as appsflyer_device_id,
      ap.fb_adgroup_id as fb_adgroup_id,
      ap.download_time as download_time,
      ap.fb_campaign_id as fb_campaign_id,
      ap.is_retargeting as is_retargeting,
      ap.re_targeting_conversion_type as re_targeting_conversion_type,
      ap.android_id as android_id,
      ap.fb_adset_name as fb_adset_name,
      ap.campaign as campaign,
      ap.install_time as install_time,
      ap.platform as platform,
      ap.fb_campaign_name as fb_campaign_name,
      ap.app_version as app_version,
      ap.os_version as os_version,
      ap.fb_adset_id as fb_adset_id,
      ap.event_type as event_type,
      ap.af_ad as af_ad,
      ap.af_ad_id as af_ad_id,
      ap.af_ad_type as af_ad_type,
      ap.af_adset as af_adset,
      ap.af_adset_id as af_adset_id,
      ap.af_c_id as af_c_id,
      ap.af_channel as af_channel,
    
  FROM (
    SELECT
      imei,
      customer_user_id,
      attribution_type,
      media_source,
      appsflyer_device_id,
      fb_adgroup_id,
      download_time,
      fb_campaign_id,
      is_retargeting,
      re_targeting_conversion_type,
      android_id,
      fb_adset_name,
      campaign,
      install_time,
      platform,
      fb_campaign_name,
      app_version,
      os_version,
      fb_adset_id,
      event_type,
      af_ad,
      af_ad_id,
      af_ad_type,
      af_adset,
      af_adset_id,
      af_c_id,
      af_channel,
      INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"order id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) order_id,
      INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"transaction id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) transaction_id,
      INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_content_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_content_id,
      INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_receipt_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_receipt_id
    FROM
      [big-query-1233:appsflyer.apps_flyer]
    WHERE
      event_name IN ('af_purchase',
        'transactions - success',
        'transactions',
        'af_purchase_zero')
     -- AND is_retargeting = TRUE
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      20,
      21,
      22,
      23,
      24,
      25,
      26,
      27,
      28,
      29,
      30,
      31 ) ap
  LEFT JOIN
    [big-query-1233:Atom.transaction] AS ol
  ON
    ap.transaction_id = ol.transactionid )a
LEFT JOIN (
  SELECT
    order_id_new,
    COUNT(*) AS count
  FROM (
    SELECT
      COALESCE(p.order_id,ol.orderid,p.af_receipt_id,p.af_content_id) AS order_id_new,
      p.*
    FROM (
      SELECT
        INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"order id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) order_id,
        INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"transaction id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) transaction_id,
        INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_content_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_content_id,
        INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_receipt_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_receipt_id
      FROM
        [big-query-1233:appsflyer.apps_flyer]
      WHERE
        event_name IN ('af_purchase',
          'transactions - success',
          'transactions',
          'af_purchase_zero')
        ) p
    LEFT JOIN
      [big-query-1233:Atom.transaction] AS ol
    ON
      p.transaction_id = ol.transactionid )
  GROUP BY
    1 )y
ON
  a.order_id_new = y.order_id_new     
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=downstream_appsflyer
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_downstream_appsflyer\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_downstream_appsflyer" &
v_first_pid=$!
v_apsflyer_pids+=" $v_first_pid"
wait $v_first_pid;

# reengagement_appsflyer loading. Replace existing
v_query_reengagement_appsflyer="SELECT 
       a.order_id_new as af_order_id,
       a.imei as af_imei_number,
      a.customer_user_id as af_customer_id,
      a.attribution_type as af_attribution_type,
      a.media_source as af_media_source,
      a.appsflyer_device_id as af_device_id,
      a.fb_adgroup_id as af_fb_adgroup_id,
      a.download_time as af_download_time,
      a.fb_campaign_id as af_fb_campaign_id,
      a.is_retargeting as af_is_retargeting,
      a.re_targeting_conversion_type as af_re_targeting_conversion_type,
      a.android_id as af_android_id,
      a.fb_adset_name as af_fb_adset_name,
      a.campaign as af_campaign_name,
      a.install_time as af_install_time,
      a.platform as af_platform_type,
      a.fb_campaign_name as af_fb_campaign_name,
      a.app_version as af_app_version,
      a.os_version as af_os_version,
      a.fb_adset_id as af_fb_adset_id,
      a.event_type as af_event_type,
      a.af_ad as af_ad_name,
      a.af_ad_id as af_ad_id,
      a.af_ad_type as af_ad_type,
      a.af_adset as af_adset_name,
      a.af_adset_id as af_adset_id,
      a.af_c_id as af_campaign_id,
      a.af_channel as af_channel,
  
  RANK() OVER (PARTITION BY a.order_id_new ORDER BY install_time ASC) rank,
  CASE
    WHEN y.count > 1 THEN 'Reattributed'
    ELSE 'NA'
  END AS af_reaatributed_flag
FROM (
  SELECT
    COALESCE(ap.order_id,ol.orderid,ap.af_receipt_id,ap.af_content_id) AS order_id_new,
        ap.imei as imei,
      ap.customer_user_id as customer_user_id,
      ap.attribution_type as attribution_type,
      ap.media_source as media_source,
      ap.appsflyer_device_id as appsflyer_device_id,
      ap.fb_adgroup_id as fb_adgroup_id,
      ap.download_time as download_time,
      ap.fb_campaign_id as fb_campaign_id,
      ap.is_retargeting as is_retargeting,
      ap.re_targeting_conversion_type as re_targeting_conversion_type,
      ap.android_id as android_id,
      ap.fb_adset_name as fb_adset_name,
      ap.campaign as campaign,
      ap.install_time as install_time,
      ap.platform as platform,
      ap.fb_campaign_name as fb_campaign_name,
      ap.app_version as app_version,
      ap.os_version as os_version,
      ap.fb_adset_id as fb_adset_id,
      ap.event_type as event_type,
      ap.af_ad as af_ad,
      ap.af_ad_id as af_ad_id,
      ap.af_ad_type as af_ad_type,
      ap.af_adset as af_adset,
      ap.af_adset_id as af_adset_id,
      ap.af_c_id as af_c_id,
      ap.af_channel as af_channel,
    
  FROM (
    SELECT
      imei,
      customer_user_id,
      attribution_type,
      media_source,
      appsflyer_device_id,
      fb_adgroup_id,
      download_time,
      fb_campaign_id,
      is_retargeting,
      re_targeting_conversion_type,
      android_id,
      fb_adset_name,
      campaign,
      install_time,
      platform,
      fb_campaign_name,
      app_version,
      os_version,
      fb_adset_id,
      event_type,
      af_ad,
      af_ad_id,
      af_ad_type,
      af_adset,
      af_adset_id,
      af_c_id,
      af_channel,
      INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"order id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) order_id,
      INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"transaction id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) transaction_id,
      INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_content_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_content_id,
      INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_receipt_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_receipt_id
    FROM
      [big-query-1233:appsflyer.apps_flyer]
    WHERE
      event_name IN ('af_purchase',
        'transactions - success',
        'transactions',
        'af_purchase_zero')
      AND is_retargeting = TRUE
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      20,
      21,
      22,
      23,
      24,
      25,
      26,
      27,
      28,
      29,
      30,
      31 ) ap
  LEFT JOIN
    [big-query-1233:Atom.transaction] AS ol
  ON
    ap.transaction_id = ol.transactionid )a
LEFT JOIN (
  SELECT
    order_id_new,
    COUNT(*) AS count
  FROM (
    SELECT
      COALESCE(p.order_id,ol.orderid,p.af_receipt_id,p.af_content_id) AS order_id_new,
      p.*
    FROM (
      SELECT
        INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"order id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) order_id,
        INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"transaction id\":\"[0-9]+\")'),'(\"[0-9]+\")'),'\"'),'\"')) transaction_id,
        INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_content_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_content_id,
        INTEGER(RTRIM(LTRIM(REGEXP_EXTRACT(REGEXP_EXTRACT(event_value,'(\"af_receipt_id\":\"[0-9]+.[0-9]+\")'),'(\"[0-9]+.[0-9]+\")'),'\"'),'\"')) AS af_receipt_id
      FROM
        [big-query-1233:appsflyer.apps_flyer]
      WHERE
        event_name IN ('af_purchase',
          'transactions - success',
          'transactions',
          'af_purchase_zero') and is_retargeting = TRUE
        ) p
    LEFT JOIN
      [big-query-1233:Atom.transaction] AS ol
    ON
      p.transaction_id = ol.transactionid )
  GROUP BY
    1 )y
ON
  a.order_id_new = y.order_id_new
"

tableName=reengagement_appsflyer
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_reengagement_appsflyer\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_reengagement_appsflyer" &
##v_first_pid=$!
v_appsflyer_pids+=" $!"


if wait $v_appsflyer_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_appsflyer_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Downstream appsflyer and Reengagement appsflyer Tables status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


