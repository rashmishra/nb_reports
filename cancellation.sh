v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi

v_dataset_name=nb_reports;
date


# redemption table loading. Replace existing
v_query_cancellation="SELECT 
        YEAR(MSEC_TO_TIMESTAMP(ol.cancelledAt+19800000)) AS cancellation_year
      , MONTH((MSEC_TO_TIMESTAMP(ol.cancelledAt+19800000))) AS cancellation_month
      , DAY(MSEC_TO_TIMESTAMP(ol.cancelledAt+19800000)) AS cancellation_day
, DATE(MSEC_TO_TIMESTAMP(ol.cancelledAt+19800000)) AS cancellation_date
      , YEAR(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchased_year
      , MONTH(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchased_month
      , DAY(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchased_day
      , DATE(MSEC_TO_TIMESTAMP(ol.createdat + 19800000)) AS purchased_date
      , ol.orderid as cancelled_order_id
      , ol.orderlineid AS cancelled_orderline_id

      , ol.dealid as deal_id
      , ol.title as deal_title
      , ol.offerid AS offer_id
      , ol.offertitle AS offer_title
      , ol.categoryid AS category_id
      , ol.merchantid as merchant_id
      , ol.merchantname as merchant_name
      , (ol.unitprice) AS cancelled_gb
      , (ol.unitprice - ol.finalprice) AS promo_amount_reversal
      , ol.promoCode AS promocode
      , ol.flatcommission AS flat_commission
      , ol.marginpercentage AS margin_percent

,Round(GR,2) as GR
             , ol.cashbackamount as cashback_amount
      , m.manager AS city_manager
      , ol.dealowner AS deal_owner
      , e.Customer_comments as customer_comment
      , e.Cancellation_source AS cancellation_source
      , m.location as city_manager_location
      
FROM (SELECT orderlineid AS orderlineid, ol.orderid AS orderid
             , ol.createdat,ol.cancelledAt as cancelledAt
             , unitprice/100 AS unitprice, flatcommission/100 AS flatcommission
             , marginpercentage/100 AS marginpercentage, finalprice/100 AS finalprice
             , dealid, offerid AS offerid,COALESCE(categoryid,deal.cat_id) as categoryid
             , offertitle AS offertitle, ol.merchantname AS merchantname
             , ol.merchantid AS merchantid, title,deal.dealOwner as dealOwner,
             oh.promocode as promocode, ol.cashbackamount as cashbackamount,
             CASE WHEN marginpercentage IS NULL THEN ((CASE WHEN flatcommission < 0 THEN 0 ELSE flatcommission END)/100)*1 
             ELSE ((CASE WHEN marginpercentage < 0 THEN 0 ELSE marginpercentage END) / 100.0)*(unitprice/100.0) END AS GR
        FROM Atom.order_line ol
        INNER JOIN (SELECT STRING(_id) AS Deal_ID, categoryId AS cat_id , dealOwner
                    FROM Atom.deal GROUP BY 1,2,3) deal ON deal.Deal_ID = ol.dealid
                    inner join Atom.order_header oh on oh.orderid = ol.orderid
        WHERE ol.status=17
          AND ol.isPaid ='t'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,17,18,dealOwner,GR
             ) ol
LEFT JOIN  (SELECT t.orderid AS txn_orderid 
 , ole.eventid AS eventid
                   , t.eventid AS txn_eventid
                   , ole.orderlineid AS orderlineid
                   , ole.Customer_comments AS Customer_comments   
                   , ole.cancellation_source AS Cancellation_source
            FROM (SELECT orderid, status, COALESCE(eventid, -1) as eventid
                         , transactiontype, updatedat, paymentflag
                         , paymentgatewayreference, failurereason, txnreason, paymentmode 
                  FROM Atom.transaction
                  WHERE status = 23
                    AND paymentflag <> 2
                  ) t
            LEFT JOIN (SELECT ole.eventid AS eventid
                             , ole.orderlineid as orderlineid
                             , ev.comments AS Customer_comments
                             , ev.source AS cancellation_source
                      FROM  [Atom.order_line_event] ole
                      INNER JOIN Atom.event ev ON ole.eventid = ev.eventid
                      GROUP BY 1, 2, 3, 4
                      )ole ON t.eventid = ole.eventid 
            GROUP BY 1, 2, 3, 4, 5, 6
            ) e ON ol.orderlineid = e.orderlineid
               AND ol.orderid = e.txn_orderid

LEFT OUTER JOIN
    BI_Automation.sales_rep_mapping AS m
  ON
    m.sales_rep = ol.dealOwner
GROUP BY cancellation_year, cancellation_month, cancellation_day, cancelled_orderline_id, cancelled_order_id, deal_id, deal_title
         , offer_id, category_id, merchant_name, offer_title, cancelled_gb, promo_amount_reversal
         , promocode, flat_commission, margin_percent, GR, purchased_year, purchased_month
         , purchased_day, city_manager, deal_owner, customer_comment, cancellation_source, merchant_id, cashback_amount,GR,city_manager_location,
         purchased_date,cancellation_date
         
         order by 1 desc
        
"
##echo -e "Query: \n $v_query_Master_Transaction table";

tableName=cancellation
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_query_cancellation\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_cancellation" &
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"



if wait $v_can_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "cancellation Table status:$v_table_status`date`" | mail -s "$v_table_status" rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


