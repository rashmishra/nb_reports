v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=nb_reports;
date


# Master_Transaction table loading. Replace existing
v_query_master_transactions="SELECT 
 x.orderid as order_id,
  datestamp as date_time_ist,
  paid_at as paid_time_ist,
  sum(credits_requested*z.vouchers) as credits_requested ,
  customer_id,
  customer_name,
  x.source as platform_type,
  case when x.promocode='' then 'null' else x.promocode end promocode,
  CASE WHEN (y.promocode_type = 'PERCENTAGE_CB' OR y.promocode_type = 'FLAT_CB') and promo_description = 'cerebro' THEN 'UCB'
WHEN y.promocode_type = 'PERCENTAGE_CB' OR y.promocode_type = 'FLAT_CB' THEN 'Universal Cashback'
WHEN y.promocode_type = 'PERCENTAGE'  OR y.promocode_type = 'FLAT' THEN 'Promo'
WHEN length(x.promocode) < 9 and length(x.promocode) > 3
THEN 'Referral'
ELSE
'Non-Promo'
END AS promo_flag,
  order_status,
  device_id,
  orderline_id,
  x.deal_id as deal_id,
  INTEGER(z.finalprice) as final_price,
  Round(margin_percentage,2) as margin_percentage,
  merchant_id,
  merchant_name,
  offer_id,
  offer_title,
  orderline_status,
  INTEGER(unit_price*z.vouchers) as unit_price,
  vertical,
  voucher_code,
  redemption_lat,
  redemption_long,
  paymentterm_id,
  flat_commission,
  deal_owner,
  deal_title,
  cm_location,
  business_head ,
  a_business_head,
  category_id,
--   transaction_id,
--   payment_flag,
--   paymentgateway_reference,
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
--   y.promo_length as promo_length, 
   y.promo_logic as promo_logic,
   y.promo_name as promo_name, 
   y.promo_description as promo_description,
   y.promocode_type as promocode_type, 
--   y.promo_discount_amount as promo_discount_amount, 
--   y.promo_discount_percentage as promo_discount_percentage, 
--   y.promo_max_cap as promo_max_cap, 
--   y.promo_cashback_percentage as promo_cashback_percentage,
--   y.promo_cashback_amount as promo_cashback_amount, 
--   y.promo_offer_price_range_from as promo_offer_price_range_from, 
--   y.promo_is_cashback as promo_is_cashback,
--   y.is_deferential as promo_is_deferential,
--   y.has_user_transacted as promo_has_user_transacted,
  first_cashback_date,
 CASE WHEN c.Deal_City = 'National' THEN 'National' else e.City end as deal_city,
 CASE WHEN d.Deal_State = 'National' THEN 'National' else e.state end  as deal_state,
  merchantcode as merchant_code,
  paymenttype as paymenttype,
  e.Primary_Category as primary_category,
    SUM(Number_of_Vouchers) AS number_of_vouchers,
  Round(SUM(GR),2) AS GR,
 -- Round(SUM(GRx),2) AS GRx,
  Round(SUM(activation_cost),2) AS customer_aquisition_cost,
  Round((SUM(TransactionValue)-sum(credits_requested*z.vouchers)),2) AS payable_by_pg,
  INTEGER(SUM(TransactionValue)) AS GB,
  Round(SUM(price_After_Promo),2) AS price_after_promo,
  Round(SUM(z.cashback_amount),2) AS cashback_amount,
  Round(SUM(A.discountbymerchant),2) as discountbymerchant
  
  FROM
 (
  SELECT
    oh.orderid AS orderid,
    oh.created_at AS datestamp,
    oh.paid_at as paid_at,
    Round((creditsrequested/s.count_oid),2) AS credits_requested,
    oh.customerid AS customer_id,
    oh.customername AS customer_name,
    oh.Source AS Source,
    oh.referralprogramid as referralprogramid,
        case when oh.source in('web','mobile','WEB','mobile-web') then 'WEB'
    when oh.source in ('app_ios','app_android') then 'APP' end source2,
    left(oh.promocode,12) as promocode,
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
    case when ol.status = 14 then 'Active' when ol.status = 15 then 'Redeemed' when ol.status = 16 then 'Refund' when ol.status = 17 then 'Cancelled' end AS orderline_status,
    ol.unitprice AS unit_price,
    ol.vertical AS vertical,
    ol.vouchercode AS voucher_code,
    ol.redemptionlat AS redemption_lat,
    ol.redemptionlong AS redemption_long,
    ol.paymenttermid AS paymentterm_id,
    ol.flatcommission AS flat_commission,
    ol.cashbackamount AS cashback_amount,
    e.deal_owner AS deal_owner,
    e.dealtitle AS Deal_title,
    m.location AS cm_location,
    m.business_head AS business_head ,
    m.a_business_head as a_business_head,
    CASE
      WHEN ol.orderid > 1991486 THEN ol.categoryid
      ELSE e.category
    END AS category_id,
--     T.transactionId AS transaction_Id,
--     T.paymentFlag AS payment_flag,
--     T.paymentgatewayreference AS paymentGateway_reference,
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
   -- SUM(T.payableAmount/100)/(s.count_oid) AS payable_by_PG,
    (SUM(ol.unitprice)) AS TransactionValue,
    (SUM(ol.finalprice)) AS price_after_promo,
    
--       case when ol.flatcommission is null then
--     (case when ol.marginpercentage <0 then 0 else ol.marginpercentage end)*(sum(ol.unitprice)- sum(ifnull(bom.discountbymerchant,0)))

--         ELSE (case when ol.flatcommission <0 then 0 else ol.flatcommission end) *count(ol.orderid)
--     END AS GR,
     CASE
      WHEN ol.offerid = w.offer_correct THEN (w.marginGR/100)*(SUM(ol.unitprice))
      ELSE (case when ol.flatcommission is null then
    (case when ol.marginpercentage <0 then 0 else ol.marginpercentage end)*(sum(ol.unitprice)- sum(ifnull(bom.discountbymerchant,0)))

        ELSE (case when ol.flatcommission <0 then 0 else ol.flatcommission end) *count(ol.orderid) END)
    END AS GR,
    cash.first_cashback_date as first_cashback_date,
    ol.merchantcode as merchantcode,
    oh.paymenttype as paymenttype
  FROM (
    SELECT
      orderid,
      MSEC_TO_TIMESTAMP(createdat+19800000) AS created_at,
      MSEC_TO_TIMESTAMP(paidat +19800000) AS paid_at,
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
      referralprogramid,
      paymenttype
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
      12,13,14,15 ) oh
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
      merchantcode
    FROM
      Atom.order_line
    WHERE
      ispaid = 't'
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
--   LEFT OUTER JOIN (
--     SELECT
--       orderid,
--       transactionid ,
--       paymentflag,
--       paymentgatewayreference,
--       payableamount
--     FROM
--       Atom.transaction
--     WHERE
--       transactiontype = 2
--       AND status = 23
--       AND payableAmount > 0
--       AND paymentFlag = 2 ) AS T
--   ON
--     T.orderid = oh.orderid
  LEFT OUTER JOIN
    nb_reports.sales_rep_mapping AS m
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
    
    left join 
    (
 select
     tbl1.orderid AS orderid,
     (discountbymerchant)/100  as discountbymerchant
    FROM [Atom.order_bom]   tbl1
INNER JOIN [Atom.order_header]  tbl2 ON tbl1.orderid = tbl2.orderid
group by 1,2
    ) bom on oh.orderid = bom.orderid
   
     where date(oh.created_at) != current_date()
  GROUP BY
    orderid,
    datestamp,
    paid_at,
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
    deal_owner,
    Deal_title,
    cm_location,
    business_head ,
    a_business_head,
    ol.orderid,
    orderline_status,
    ol.categoryid,
    e.category,
    category_id,
--     transaction_Id,
--     payment_Flag,
--     paymentGateway_Reference,
    s.count_oid,
    txn,
    last_purchase_date,
    first_purchase_date,
    w.activation_cost,
    buying_city,
    buying_state,
    p.fpoid,
    oh.orderid,
    ol.orderid,
    ol.offerid,
    w.offer_correct,
    w.marginGR,
    ol.marginPercentage,
    ol.flatcommission,
    new_customer_month,
    new_customer_day,
    source2,
    first_cashback_date,
    redemption_city,
    redemption_state,
    first_transaction,
    referralprogramid,
    merchantcode,
    paymenttype,
    bom.discountbymerchant
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
    left join
    (
    
    select  sum(cashbackamount )/100 as cashback_amount, sum(finalprice)/100 as finalprice, count(orderlineid) as vouchers, orderlineid   from [Atom.order_line] where ispaid = 't' group by orderlineid  
    ) z on z.orderlineid = x.orderline_id
    left join 
    (
    select
     tbl1.orderid AS orderid,
     sum(discountbymerchant)/100  as discountbymerchant
    FROM [Atom.order_bom]   tbl1
INNER JOIN [Atom.order_header]  tbl2 ON tbl1.orderid = tbl2.orderid
group by 1
    ) A on A.orderid = x.orderid
    left join
(select
string(deal_id) as deal_id,
case when count >= 2 then 'National' else 'Single_City' end as Deal_City
from 
(
select 
_id as deal_id ,
exact_count_distinct(c.city) as count
from [Atom.deal] a 
left join ( select integer(id) as deal_id, mappings.merchant.id as merchant_id
from [Atom.mapping] where type = 'deal' group by 1,2) b on a._id = b.deal_id
left join (select string(Merchantid) as merchantid , redemptionAddress.cityTown  as city from [Atom.merchant] 
where isPublished is true group by 1,2) c on b.merchant_id = c.merchantid

group by 1
)
group by 1,2
)
 c on x.deal_id = c.deal_id
left join
(select
string(deal_id) as deal_id,
case when count >= 2 then 'National' else 'Single_state' end as Deal_State
from 
(
select 
_id as deal_id ,
exact_count_distinct(c.state) as count
from [Atom.deal] a  
left join ( select integer(id) as deal_id, mappings.merchant.id as merchant_id
from [Atom.mapping] where type = 'deal' group by 1,2) b on a._id = b.deal_id
left join (select string(Merchantid) as merchantid , redemptionAddress.state   as state from [Atom.merchant] 
where isPublished is true group by 1,2) c on b.merchant_id = c.merchantid

group by 1
)
group by 1,2
)
 d on x.deal_id = d.deal_id
 
 left join (select string(Merchantid) as merchantid , redemptionAddress.state   as state, redemptionAddress.cityTown as City,
 Case when catInfo.isPrimary is true then catInfo.key end as Primary_Category from [Atom.merchant] 
where isPublished is true  and catInfo.isPrimary is true group by 1,2,3,4) e on x.merchant_id = e.merchantid
 
 
    
    Group by 1, 2,  3, 
   -- 4, 
   5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48 , 49, 50 , 51,52,53,54--,55,56,57,58,59,60,61,62,63
              
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


