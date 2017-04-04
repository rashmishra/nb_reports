v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=nb_reports;
date


# Master_Transaction table loading. Replace existing
v_query_master_transactions="SELECT 
 orderid as order_id,
  datestamp as date_time_ist,
  credits_requested,
  customer_id,
  customer_name,
  x.source as platform_type,
  case when x.promocode='' then 'null' else x.promocode end promocode,
  order_status,
  device_id,
  orderline_id,
  deal_id,
  INTEGER(final_price) as final_price,
  Round(margin_percentage,2) as margin_percentage,
  merchant_id,
  merchant_name,
  offer_id,
  offer_title,
  orderline_status,
  INTEGER(unit_price) as unit_price,
  vertical,
  voucher_code,
  redemption_lat,
  redemption_long,
  paymentterm_id,
  flat_commission,
  deal_owner,
  deal_title,
  cm_location,
  city_manager,
  category_id,
  transaction_id,
  payment_flag,
  paymentgateway_reference,
  Round(txn,2) as txn,
  last_purchase_date,
  first_purchase_date,
  buying_city,
  buying_state,
   redemption_city,
    redemption_state,
  new_customer_day,
  new_customer_month,
  first_transaction,
  referralprogramid as referral_program_id,
  y.promo_length as promo_length, 
  y.promo_logic as promo_logic,
  y.promo_name as promo_name, 
  y.promo_description as promo_description,
  y.promocode_type as promocode_type, 
  y.promo_discount_amount as promo_discount_amount, 
  y.promo_discount_percentage as promo_discount_percentage, 
  y.promo_max_cap as promo_max_cap, 
  y.promo_cashback_percentage as promo_cashback_percentage,
  y.promo_cashback_amount as promo_cashback_amount, 
  y.promo_offer_price_range_from as promo_offer_price_range_from, 
  y.promo_is_cashback as promo_is_cashback,
  y.is_deferential as promo_is_deferential,
  y.has_user_transacted as promo_has_user_transacted,
  --redemption_city,
  first_cashback_date,
  --y.source as source, 
  --source2,
    SUM(Number_of_Vouchers) AS number_of_vouchers,
  Round(SUM(GR),2) AS GR,
  Round(SUM(activation_cost),2) AS customer_aquisition_cost,
  Round(SUM(payable_by_PG),2) AS payable_by_pg,
  INTEGER(SUM(TransactionValue)) AS GB,
  Round(SUM(price_After_Promo),2) AS price_after_promo,
  Round(SUM(x.cashback_amount),2) AS cashback_amount,
  --SUM(voucher_redeemed) AS voucher_redeemed,
  
  FROM
 (
  SELECT
    oh.orderid AS orderid,
    oh.created_at AS datestamp,
    Round((creditsrequested/s.count_oid),2) AS credits_requested,
    oh.customerid AS customer_id,
    oh.customername AS customer_name,
    oh.Source AS Source,
    oh.referralprogramid as referralprogramid,
        case when (oh.source = 'web' or oh.source = 'mobile' or oh.source = 'WEB') then 'WEB'
    when (oh.source = 'app_ios' or oh.source = 'app_android') then 'APP' end source2,
    left(oh.promocode,12) AS promocode,
    oh.Status AS order_Status,
    oh.deviceid AS device_id,
    ol.orderlineid AS orderline_id,
    ol.dealid AS deal_id,
    ol.finalprice AS final_price,
    ol.marginpercentage AS margin_percentage,
    ol.merchantid AS merchant_id,
    ol.merchantname AS merchant_name,
    ol.offerid AS offer_id,
    ol.offertitle AS offer_title,
    ol.status AS orderline_status,
    ol.unitprice AS unit_price,
    ol.vertical AS vertical,
    ol.vouchercode AS voucher_code,
    ol.redemptionlat AS redemption_lat,
    ol.redemptionlong AS redemption_long,
    ol.paymenttermid AS paymentterm_id,
    ol.flatcommission AS flat_commission,
    ol.cashbackamount AS cashback_amount,
    --ol.voucher_redeemed AS voucher_redeemed,
    e.deal_owner AS deal_owner,
    e.dealtitle AS Deal_title,
    m.location AS cm_location,
    m.manager AS city_manager,
    CASE
      WHEN ol.orderid > 1991486 THEN ol.categoryid
      ELSE e.category
    END AS category_id,
    T.transactionId AS transaction_Id,
    T.paymentFlag AS payment_flag,
    T.paymentgatewayreference AS paymentGateway_reference,
    1/s.count_oid AS txn,
    p.LPD AS last_purchase_date,
    p.FPD AS first_purchase_date,
    (w.activation_cost)*COUNT (ol.orderid) AS activation_cost,
    l.city AS buying_city,
    l.state AS buying_state,
    lat.city as redemption_city,
    lat.state as redemption_state,
    CASE WHEN p.fpoid = oh.orderid THEN 'TRUE' else 'FALSE' end as first_transaction,
    Case when Date(p.FPD)=DATE(oh.created_at)  then 'TRUE' else 'FALSE' end new_customer_day,
    Case when Month(p.fpd)=month(oh.created_at) and Year(p.fpd)=year(oh.Created_at) then 'New' else 'Old' end new_customer_month,
    COUNT (ol.orderid) AS number_of_vouchers,
    SUM(T.payableAmount/100)/(s.count_oid) AS payable_by_PG,
    (SUM(ol.unitprice)) AS TransactionValue,
    (SUM(ol.finalprice)) AS price_after_promo,
    CASE
      WHEN ol.offerid = w.offer_correct THEN (w.marginGR/100)*(SUM(ol.unitprice))
      ELSE (
      CASE
        WHEN ol.marginPercentage IS NULL THEN ((CASE
            WHEN ol.flatcommission < 0 THEN 0
            ELSE ol.flatcommission END))*COUNT (ol.orderid)
        ELSE ((CASE
            WHEN ol.marginPercentage < 0 THEN 0
            ELSE ol.marginPercentage END))*(SUM(ol.unitprice)) END)
    END AS GR,
    --mc.redemption_city as redemption_city,
    cash.first_cashback_date as first_cashback_date
  FROM (
    SELECT
      orderid,
      MSEC_TO_TIMESTAMP(createdat+19800000) AS created_at,
      creditsrequested/100.0 AS creditsrequested,
      customerid,
      customername,
      source,
      promocode,
      status,
      totalprice/100.0 AS totalprice,
      buyinglat,
      buyinglong,
      deviceid,
      referralprogramid     
    FROM
      Atom.order_header
    WHERE
      ispaid = 't'
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
      12,13 ) oh
  INNER JOIN (
    SELECT
      orderid,
      orderlineid,
      dealid,
      finalprice/100.0 AS finalprice,
      marginpercentage/100.0 AS marginpercentage,
      merchantid,
      merchantname,
      offerid,
      offertitle,
      status,
      unitprice/100.0 AS unitprice,
      vertical,
      vouchercode,
      redemptionlat,
      redemptionlong,
      paymenttermid,
      categoryid,
      flatcommission/100.0 AS flatcommission,
      cashbackamount/100.0 AS cashbackamount,
--       CASE
--         WHEN redemptiondate IS NOT NULL OR status = 15 THEN EXACT_COUNT_DISTINCT(orderlineid)
--       END voucher_redeemed,
    FROM
      Atom.order_line
    WHERE
      ispaid = 't'
--     GROUP BY
--       1,
--       2,
--       3,
--       4,
--       5,
--       6,
--       7,
--       8,
--       9,
--       10,
--       11,
--       12,
--       13,
--       14,
--       15,
--       16,
--       17,
--       18,
--       19,
--       redemptiondate 
) ol
  ON
    ol.orderid = oh.orderid
  LEFT JOIN (
    SELECT
      orderId AS oid,
      COUNT(orderId) AS count_oid
    FROM
      Atom.order_line
    GROUP BY
      1) s
  ON
    s.oid = oh.orderid
  LEFT OUTER JOIN (
    SELECT
      STRING(_id) AS deal_id,
      dCat.id AS category,
      units.dtitle AS Dealtitle,
      dealOwner AS deal_owner
    FROM
      Atom.deal) AS e
  ON
    ol.dealid = e.deal_id
    -- left join (select string(merchantId) as merchantId, name , redemptionAddress.cityTown as redemption_city  from Atom.merchant) mc on mc.merchantid = ol.merchantid
    left join (select customerid, min(date(MSEC_TO_TIMESTAMP(createdat+19800000))) as first_cashback_date from Atom.order_header 
where totalcashbackamount is not null and ispaid = 't' group by 1 ) cash on cash.customerid = oh.customerid
  LEFT OUTER JOIN (
    SELECT
      OFFER_KEY AS offer_correct,
      MARGIN AS margin_correct,
      INTEGER(margin_GR) AS marginGR,
      INTEGER(Activation_cost) AS activation_cost
    FROM
      bi.buy_price2) AS w
  ON
    ol.offerid = w.offer_correct
  LEFT OUTER JOIN (
    SELECT
      orderid,
      transactionid ,
      paymentflag,
      paymentgatewayreference,
      payableamount
    FROM
      Atom.transaction
    WHERE
      transactiontype = 2
      AND status = 23
      AND payableAmount > 0
      AND paymentFlag = 2 ) AS T
  ON
    T.orderid = oh.orderid
  LEFT OUTER JOIN
    bi.sales_rep_mapping1 AS m
  ON
    m.sales_rep = e.deal_owner
  LEFT OUTER JOIN (
    SELECT
      customerId AS email1,
      orderSummary.firstPurchaseDetail.orderId AS fpoid,
      (MSEC_TO_TIMESTAMP(orderSummary.firstPurchaseDetail.purchasedate*1000+19800000)) AS FPD,
      (MSEC_TO_TIMESTAMP(orderSummary.lastPurchaseDetail.purchasedate*1000+19800000)) AS LPD,
      
      orderSummary.lastPurchaseDetail.purchasedate AS lpd1
    FROM
      Atom.customer
    WHERE
      orderSummary.firstPurchaseDetail.orderId IS NOT NULL ) AS p
  ON
    p.email1 = oh.customerid
  LEFT JOIN
    [big-query-1233:latitude_longitude.latitude_longitude_base] l
  ON
    l.latitude= oh.buyinglat
    AND oh.buyinglong=l.longitude
      LEFT JOIN
    [big-query-1233:latitude_longitude.latitude_longitude_base] lat
  ON
    lat.latitude= ol.redemptionlat
    AND ol.redemptionlong=lat.longitude
   
     where date(oh.created_at) != current_date()
  GROUP BY
    orderid,
    datestamp,
    credits_requested,
    customer_id,
    customer_name,
    Source,
    Promocode,
    Order_Status,
    Device_ID,
    orderline_id,
    deal_id,
    final_price,
    margin_percentage,
    merchant_id,
    merchant_name,
    offer_id,
    offer_title,
    orderline_status,
    unit_price,
    vertical,
    voucher_code,
    redemption_lat,
    redemption_long,
    paymentterm_id,
    flat_commission,
    cashback_amount,
    --voucher_redeemed,
    deal_owner,
    Deal_title,
    cm_location,
    city_manager,
    ol.orderid,
    orderline_status,
    ol.categoryid,
    e.category,
    category_id,
    transaction_Id,
    payment_Flag,
    paymentGateway_Reference,
    s.count_oid,
    txn,
    last_purchase_date,
    first_purchase_date,
    w.activation_cost,
    buying_city,
    buying_state,
    p.fpoid,
    oh.orderid,
    --isnew,
    ol.orderid,
    ol.offerid,
    w.offer_correct,
    w.marginGR,
    ol.marginPercentage,
    ol.flatcommission,
    new_customer_month,
    new_customer_day,
    source2,--redemption_city,
    first_cashback_date,
    redemption_city,
    redemption_state,
    first_transaction,
    referralprogramid
    ) x
    
LEFT join
    (
    select 
     promocode_id, length(ic_promocode_id) as promo_length, 
     ic_title promo_logic, promocode as promo_name, ic_description as promo_description, promo_code_type as promocode_type, discount_amount as promo_discount_amount, 
  discount_percent as promo_discount_percentage, max_cap as promo_max_cap, 
  cashback_percent as promo_cashback_percentage, 
  cashback_amount as promo_cashback_amount, 
  offer_price_range_from as promo_offer_price_range_from, 
  is_cashback as promo_is_cashback, source,is_deferential,has_user_transacted
    
    from nb_reports.v_promo
    group by 
    promocode_id, promo_length, 
  promo_logic,promo_name, promo_description,promocode_type, promo_discount_amount, 
  promo_discount_percentage, promo_max_cap, promo_cashback_percentage,promo_cashback_amount, promo_offer_price_range_from, promo_is_cashback, source,is_deferential,has_user_transacted
    
    ) y  on x.promocode = y.promocode_id and x.source2 = y.source
    
    Group by 1, 2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,52,53,54,55,56,57,58,59
          
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=master_transaction
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_master_transactions\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_master_transactions" &
v_first_pid=$!
v_txn_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_txn_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_txn_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Master Transaction Table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com ##sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


