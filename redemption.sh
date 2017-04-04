v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=nb_reports;
date


# redemption table loading. Replace existing
v_query_redemption="SELECT
  oh.orderid as order_id,
  ol.orderlineID as orderline_id,
  (MSEC_TO_TIMESTAMP(ol.createdat+19800000)) AS purchase_date,
  (MSEC_TO_TIMESTAMP(ol.redemptiondate+19800000)) AS redemption_date,
  (Ol.redemptiondate-ol.createdat)/1000 AS purchase_to_redeem_time_diff_in_sec,
  --Round((((ol.redemptiondate+19800000)/(60 * 60 * 1000))-((ol.createdat+19800000)/(60 * 60 * 1000))),2) as RedeemTimeDiff,
  ol.DealId as deal_id,
  ol.Title as deal_title,
  ol.offerId as offer_id,
  ol.OfferTitle as offer_title,
  ol.VoucherId as voucher_id,
  ol.VoucherCode as voucher_code,
  case when oh.promocode = ''  then 'null' else left(oh.promocode,12) end promocode,
  Voucherstatus as voucher_status,
  CustomerName as customer_name,
  customerid as customer_id,
  MerchantName as merchant_name,
  merchantid as merchant_id,
  Unitprice/ 100 AS offer_price,
  MarginPercentage as margine_percentage,
  flatcommission/100 as flat_commission,
  ol.redemptionbyrole AS redemption_type,
  ol.cashbackamount/100 as cashback_amount,
  CASE
    WHEN oh.orderid > 1991486 THEN ol.categoryid
    ELSE e.category
  END AS category_id,
  e.dealOwner as deal_owner,
  s.location as city_manager_location, s.manager as city_manager
FROM
  Atom.order_header oh
INNER JOIN
  Atom.order_line ol
ON
  oh.orderid = ol.orderid
LEFT OUTER JOIN (
  SELECT
    STRING(_id) AS deal_id,
    dCat.id AS category,
    dealOwner
  FROM
    Atom.deal) AS e
ON
  ol.dealid = e.deal_id
  
  left join bi.sales_rep_mapping1 s on s.sales_rep = e.dealOwner
WHERE
  ol.redemptiondate IS NOT NULL
  and oh.ispaid = 't'
  
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
        
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=redemption
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_redemption\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_redemption" &
v_first_pid=$!
v_redem_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_redem_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_redem_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Redemption Table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


