#!/bin/bash

## Script Name: base_update_script.sh

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
        v_log_obj_txt+=`echo "\n$(date) $(date) Task ($v_subtask) failed for User Attributes computation. Hence exiting."`;

        taskEndTime=`date`;

        v_task_end_epoch=`date +%s`
        v_task_end_ts=`echo $(date -d "@$v_task_end_epoch" +"%Y-%m-%d %r %Z")`;

        echo -e "\n$(date) Task ($v_subtask) failed for User Attributes computation. Hence exiting."

        echo -e "\n$(date): Task ($v_subtask) failed for User Attributes computation. Hence exiting." | mail -s "FAILED | User Attributes computation at ${v_task_start_time}" sairanganath.v@nearbuy.com # harsh.choudhary@nearbuy.com mahesh.sharma@nearbuy.com

        echo -e  "\nTask started at ${v_task_start_time} and ended at ${v_task_end_time}.";

        exit 1;

    fi

}

v_dataset_name="engg_reporting";



## Table 1: latest_three_txn
									# Category (Transaction)
									# Primary SKU (Transaction)
									# Price point (Transaction)

v_query="SELECT  customerId,
        NTH(1,dealId) AS latestDealTxn,
        NTH(1,categoryId) AS latestCatTxn,
        NTH(1,pricePoint) AS latestTxnPricePoint,
        NTH(2,dealId) AS secLatestDealTxn,
        NTH(2,categoryId) AS secLatestCatTxn,
        NTH(2,pricePoint) AS secLatestTxnPricePoint,
        NTH(3,dealId) AS thirdLatestDealTxn,
        NTH(3,categoryId) AS thirdLatestCatTxn,
        NTH(3,pricePoint) AS thirdLatestTxnPricePoint
FROM (SELECT  a.customerId AS customerId,
              b.dealId AS dealId,
              b.categoryId as categoryId,
              txn_time,
              NTH(51,QUANTILES(finalPrice,101)) as pricePoint
      FROM (SELECT   orderid,
                      customerid,
                      MSEC_TO_TIMESTAMP(createdat+19800000) AS txn_time
            FROM  Atom.order_header
            WHERE ispaid='t'
            AND totalPrice>0
            ) AS a
      LEFT JOIN (SELECT orderid,
                        dealid,
                        offerid,
                        finalprice/100 as finalprice,
                        categoryid
                 FROM Atom.order_line
                 WHERE ispaid='t'
                   AND finalprice>0
                 ) AS b
      ON  a.orderid = b.orderid
      WHERE b.dealid<> '14324'
      AND b.dealid NOT IN (select STRING(deal_id) from dbdev.dtr_deals_live)
      GROUP BY 1,2,3,4
      ORDER BY 4 DESC
)
GROUP BY customerId";

v_destination_tbl="${v_dataset_name}.latest_three_txn";

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

echo `date` "Creating latest_three_txn: $v_task_status";


v_subtask="User Attributes Step 1: latest_three_txn creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 1: latest_three_txn

## Table 2: most_txn_category


v_query="SELECT  customerId,
        NTH(1,categoryId) AS mostTxnCat,
        ROUND(NTH(1,pricePoint),2) AS mostTxnCatPricePoint,
        NTH(2,categoryId) AS secMostTxnCat,
        ROUND(NTH(2,pricePoint),2) AS secMostTxnPricePoint,
        NTH(3,categoryId) AS thirdMostTxnCat,
        ROUND(NTH(3,pricePoint),2) AS thirdMostTxnPricePoint
FROM (SELECT
        a.customerId AS customerId,
        b.categoryId AS categoryId,
        COUNT(offerId) AS catCount,
        NTH(51,QUANTILES(finalprice,101)) AS pricePoint
      FROM (SELECT  orderid,
                    customerid
            FROM [big-query-1233:Atom.order_header]
            WHERE ispaid='t'
            AND totalprice>0 
            ) AS a

      LEFT JOIN (SELECT orderid,
                        dealid,
                        offerid,
                        finalprice/100 as finalprice,
                        categoryid
                  FROM [big-query-1233:Atom.order_line]
                  WHERE ispaid='t'
                    AND finalprice>0
                ) AS b
          ON a.orderid = b.orderid
      WHERE b.dealid<> '14324'
        AND b.dealid NOT IN (select STRING(deal_id) from dbdev.dtr_deals_live)
      GROUP BY 1,2
      ORDER BY 3 DESC
     )
GROUP BY 1";
v_destination_tbl="${v_dataset_name}.most_txn_category";

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

echo `date` "Creating most_txn_category: $v_task_status";


v_subtask="User Attributes Step 2: most_txn_category creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 2: most_txn_category


## Table 3: most_txns_deal


v_query="SELECT
  customerId,
  NTH(1,dealId) AS MostTxnDeal,
  NTH(2,dealId) AS secondMostTxnDeal,
  NTH(3,dealId) AS thirdMostTxnDeal
FROM (SELECT a.customerId AS customerId,
            b.dealId AS dealId,
            COUNT(dealId) AS dealCount
      FROM (SELECT  orderid,
                    customerid
            FROM [big-query-1233:Atom.order_header]
            WHERE ispaid='t'
              AND totalprice>0
           ) AS a

LEFT JOIN (SELECT   orderid,
                    dealid,
                    offerid,
                    finalprice/100 as finalprice,
                    categoryid
            FROM [big-query-1233:Atom.order_line]
            WHERE ispaid='t'
              AND finalprice>0
           ) AS b
  ON a.orderid = b.orderid
WHERE b.dealid<> '14324'
  AND b.dealid NOT IN (select STRING(deal_id) from dbdev.dtr_deals_live)
GROUP BY 1,2
ORDER BY 3 DESC
   )
GROUP BY customerId";
v_destination_tbl="${v_dataset_name}.most_txns_deal";

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

echo `date` "Creating most_txns_deal: $v_task_status";


v_subtask="User Attributes Step 3: most_txns_deal creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 3: most_txns_deal


## Table 4: latest_redeem_city


v_query="SELECT  customerId,
        NTH(1,redeemCity) AS latestRedeemCity,
        NTH(2,redeemCity) AS secondLatRedeemCity,
        NTH(3,redeemCity) AS thirdLatRedeemCity
FROM(SELECT 
        a.customerId AS customerId,
        c.redeemCity AS redeemCity,
        txn_time
      FROM (SELECT  orderid,
                    customerid,
                    MSEC_TO_TIMESTAMP(createdat+19800000) AS txn_time
            FROM [big-query-1233:Atom.order_header]
            WHERE ispaid='t'
              AND totalPrice>0
          ) AS a
    LEFT JOIN (SELECT   orderid,
                        dealid,
                        offerid,
                        merchantid,
                        finalprice/100 as finalprice,
                        categoryid
              FROM [big-query-1233:Atom.order_line]
              WHERE ispaid='t'
                AND finalprice>0
            ) AS b
         ON a.orderid = b.orderid
     LEFT JOIN (SELECT  STRING( merchantId ) as merchantId,
                        redemptionAddress.cityTown AS redeemCity
                FROM  [big-query-1233:Atom.merchant]
                where isActive is true 
                  AND isDeleted is false
                  AND isPublished is true 
              ) AS c
          ON b.merchantid = c.merchantId
      WHERE b.dealid<> '14324'
        AND b.dealid NOT IN (select STRING(deal_id) from dbdev.dtr_deals_live)
      GROUP BY 1,2,3
      ORDER BY txn_time DESC
) base
WHERE LENGTH(customerId)>5
GROUP BY customerId";
v_destination_tbl="${v_dataset_name}.latest_redeem_city";

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

echo `date` "Creating latest_redeem_city: $v_task_status";


v_subtask="User Attributes Step 4: latest_redeem_city creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 4: latest_redeem_city

## Table 5: txn_valid_for


v_query="SELECT  CustomerID AS customerId,
        SUM(validForOneTx) AS validForOneTx,
        SUM(validForTwoTx) AS validForTwoTx,
        SUM(validForMultipleTx) AS validForMultipleTx
FROM  (SELECT
          CustomerID,
          CASE WHEN c.validFor =1 THEN COUNT(DISTINCT(a.orderid)) END AS validForOneTx,
          CASE WHEN c.validFor =2 THEN COUNT(DISTINCT(a.orderid)) END AS validForTwoTx,
          CASE WHEN c.validFor >2 THEN COUNT(DISTINCT(a.orderid)) END AS validForMultipleTx,  
        FROM (SELECT  orderid,
                      customerid
              FROM  [big-query-1233:Atom.order_header]
              WHERE ispaid='t'
                AND totalprice>0
              GROUP BY 1,2
             ) AS a
        LEFT JOIN (SELECT orderid,
                          dealid,
                          offerid
                  FROM   [big-query-1233:Atom.order_line]
                  WHERE ispaid='t'
                    AND finalprice>0
                  GROUP BY 1,2,3
                  ) AS b
            ON a.orderid = b.orderid
        LEFT JOIN (SELECT  STRING(_id) as dealId,
                           offers.units.ov.validFor as validFor
                  FROM [big-query-1233:Atom.offer]
                  WHERE offers.isActive = true
                  ) AS c
            ON b.dealId = c.dealId
        WHERE  b.dealid<> '14324'
          AND b.dealid NOT IN (select STRING(deal_id) from dbdev.dtr_deals_live)
        GROUP BY CustomerID, c.validfor
        )
GROUP BY customerId";
v_destination_tbl="${v_dataset_name}.txn_valid_for";

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

echo `date` "Creating txn_valid_for: $v_task_status";


v_subtask="User Attributes Step 5: txn_valid_for creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 5: txn_valid_for

## Table 6: txn_summary


v_query="SELECT
  a.customerid as customerid,
  a.name as name,
  a.gender as gender,
  a.dob.day as dob_day,
  a.dob.month as dob_month,
  a.dob.year as dob_year,
  b.firstPurchaseDate as firstPurchaseDate,
  b.lastPurchaseDate as lastPurchaseDate,
  b.totalTxn as totalTxn,
  b.distinctOffersBought as distinctOffersBought,
  b.voucherperTxn as voucherperTxn,
  b.totalVouchers as totalVouchers,
  b.txnFrequency as txnFrequency,
  b.percDiscountAffinty as percDiscountAffinty
FROM
(SELECT 
  customerId,
  name,
  gender,
  dob.day,
  dob.month,
  dob.year
FROM
  [big-query-1233:Atom.customer]
WHERE 
  isValidated=true) as a
  
LEFT JOIN
(SELECT 
  z.customerId as customerId,
  DATE(MIN(PurchaseDate)) AS firstPurchaseDate,
  DATE(MAX(PurchaseDate)) AS lastPurchaseDate,
  COUNT(UNIQUE(x.orderid)) as totalTxn,
  COUNT(UNIQUE(offerid)) AS distinctOffersBought,
  COUNT(offerid) as offersBought,
  ROUND(COUNT(orderlineid)/COUNT(DISTINCT(x.orderid)),2) AS voucherperTxn,
  COUNT(orderlineid) AS totalVouchers,
  ROUND(COUNT(UNIQUE(x.orderId))/(DATEDIFF(MAX(PurchaseDate),MIN(PurchaseDate))+1),2) AS txnFrequency,
  ROUND((SUM(CASE WHEN z.promocode is not null THEN 1 ELSE 0 END)/COUNT(x.orderId))*100,2) AS percDiscountAffinty
FROM
(SELECT
  orderlineid,
  orderid,
  dealid,
  offerid,
  voucherid,
  MSEC_TO_TIMESTAMP(redemptiondate+19800000 ) AS redeemDate,
  MSEC_TO_TIMESTAMP(createdat+19800000 ) AS PurchaseDate
FROM [big-query-1233:Atom.order_line]
WHERE ispaid='t'
AND finalprice >0
AND dealid<> '14324'
AND dealid NOT IN (select STRING(deal_id) from dbdev.dtr_deals_live)
GROUP BY 1,2,3,4,5,6,7
) AS x

LEFT JOIN ( SELECT  orderid,
  promocode,
  customerid
FROM [big-query-1233:Atom.order_header]
WHERE ispaid='t'
AND totalprice>0
GROUP BY
1,2,3) AS z
ON
  (x.orderid = z.orderid)
WHERE LENGTH(z.customerId)>5 
AND  x.dealid<> '14324'
AND x.dealid NOT IN (select STRING(deal_id) from dbdev.dtr_deals_live)
GROUP BY
1) as b
ON (a.customerId=b.customerId)
";
v_destination_tbl="${v_dataset_name}.txn_summary";

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

echo `date` "Creating txn_summary: $v_task_status";


v_subtask="User Attributes Step 6: txn_summary creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 6: txn_summary


## Table 7: gb_gr_cashback


v_query="SELECT 
  x.customerid as customerid,
  x.totalgb as totalGB,
  x.GR as GR,
  x.cashback as cashback,
  y.totalgb_afterCB as GB_afterCB,
  y.GR_afterCB as GR_afterCB,
  y.cashback_afterCB as cashback_afterCB
FROM 
(SELECT customerId, sum(totalgb) as totalgb, case when SUM(GR) is null then 0 else Round(sum(GR),2) end as GR, Case when sum(cashback) is null then 0 else sum(cashback) end as cashback
FROM 
(SELECT customerId,categoryId,sum(totalPrice/100) as totalgb , round(sum(totalcashbackamount/100),0) as cashback ,
CASE
       WHEN ol.marginPercentage IS NULL THEN ((CASE
           WHEN ol.flatcommission < 0 THEN 0
           ELSE (ol.flatcommission/100)*COUNT(ol.orderid) END))
       ELSE (((CASE
           WHEN ol.marginPercentage < 0 THEN 0
           ELSE ol.marginPercentage/100 END))*(SUM(ol.unitprice)/100)) END GR
FROM Atom.order_header oh
inner join 
(SELECT merchantId,categoryId,orderId,
marginPercentage,flatcommission,unitprice,

FROM Atom.order_line  WHERE ispaid = 't' AND dealid <> '14324' AND finalprice > 0 
GROUP BY 1,2,3,4,5,6) ol
on oh.orderId=ol.orderId
WHERE oh.ispaid='t' AND oh.totalprice > 0
GROUP BY 1, 2, ol.marginPercentage, ol.flatcommission 
order by 4 desc
)
GROUP BY 1) as x

left join
(SELECT customerId, sum(totalgb) as totalgb_afterCB, case when SUM(GR) is null then 0 else Round(sum(GR),2) end as GR_afterCB, Case when sum(cashback) is null then 0 else sum(cashback) end as cashback_afterCB
FROM 
(SELECT a.customerid as customerid,sum(totalPrice/100) as totalgb , round(sum(totalcashbackamount/100),0) as cashback ,
CASE
       WHEN c.marginPercentage IS NULL THEN ((CASE
           WHEN c.flatcommission < 0 THEN 0
           ELSE (c.flatcommission/100)*COUNT(c.orderid) END))
       ELSE (((CASE
           WHEN c.marginPercentage < 0 THEN 0
           ELSE c.marginPercentage/100 END))*(SUM(c.unitprice)/100)) END GR
FROM 
(select
  orderid, customerid,MSEC_TO_TIMESTAMP(createdat+19800000) as createdate, totalprice, totalcashbackamount
FROM [big-query-1233:Atom.order_header]
WHERE ispaid='t'
 AND totalprice>0) a

left join
(select
  customerid,
  NTH(1,createdate) as firstcashbackdate,
FROM 
(select
  orderid, customerid,MSEC_TO_TIMESTAMP(createdat+19800000) as createdate, totalcashbackamount
FROM [big-query-1233:Atom.order_header]
WHERE ispaid='t'
 AND totalprice>0
 AND totalcashbackamount is not null
 order by 3
)
GROUP BY 1
) b
on (a.customerid=b.customerid)

left join (
SELECT merchantId,categoryId,orderId
       , marginPercentage,flatcommission,unitprice
FROM Atom.order_line  
WHERE ispaid = 't' 
  AND dealid <> '14324' AND finalprice > 0 
GROUP BY 1,2,3,4,5,6
) c
on a.orderId=c.orderId
WHERE a.createdate >= b.firstcashbackdate
GROUP BY 1,c.marginPercentage,c.flatcommission
)
GROUP BY 1
) as y
on (x.customerid=y.customerid)";

v_destination_tbl="${v_dataset_name}.gb_gr_cashback";

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

echo `date` "Creating gb_gr_cashback: $v_task_status";


v_subtask="User Attributes Step 7: gb_gr_cashback creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 7: gb_gr_cashback



## Table 8: user_credits_available


v_query="SELECT
  userid AS customerid,
  SUM(totalcreditsavailable/100) AS creditsAvailable
FROM [big-query-1233:Atom.user_credit_summary]
GROUP BY 1";
v_destination_tbl="${v_dataset_name}.user_credits_available";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}" &
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating user_credits_available: $v_task_status";


v_subtask="User Attributes Step 8: user_credits_available creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 8: user_credits_available


## Table 9: post_purchase


v_query="select 
  a.customerid as customerid,
  weekendPurchase,
  weekdayPurchase,
  weekendPurchase_weekendRedeem,
  weekendPurchase_weekdayRedeem,
  weekdayPurchase_weekendRedeem,
  weekdayPurchase_weekdayRedeem,
  unredeemedVouchers,
  ROUND(b.redeemtimediff_mintues, 3) as redeemtimediff_mintues
from
(SELECT
  customerid,
  SUM(weekendPurchase) AS weekendPurchase,
  SUM(weekdayPurchase) AS weekdayPurchase,
  SUM(weekendPurchase_weekendRedeem) AS weekendPurchase_weekendRedeem,
  SUM(weekendPurchase_weekdayRedeem) AS weekendPurchase_weekdayRedeem,
  SUM(weekdayPurchase_weekendRedeem) AS weekdayPurchase_weekendRedeem,
  SUM(weekdayPurchase_weekdayRedeem) AS weekdayPurchase_weekdayRedeem,
  SUM(unredeemedVouchers) AS unredeemedVouchers,
--  AVG(redeemtimediff) AS redeemtimediff_mintues,
--  AVG(distanceinkm) AS redeemDistance
FROM
(SELECT 
  c.customerId as customerId,
  CASE WHEN DAYOFWEEK(createDate) in (1,7) THEN EXACT_COUNT_DISTINCT(b.orderid) END AS weekendPurchase,
  CASE WHEN DAYOFWEEK(createDate) in (2,3,4,5,6) THEN EXACT_COUNT_DISTINCT(b.orderid) END AS weekdayPurchase,
  CASE WHEN DAYOFWEEK(createDate) in (1,7) AND DAYOFWEEK(redeemdate) in (1,7) THEN EXACT_COUNT_DISTINCT(b.orderid) END AS weekendPurchase_weekendRedeem,
  CASE WHEN DAYOFWEEK(createDate) in (1,7) AND DAYOFWEEK(redeemdate) in (2,3,4,5,6) THEN EXACT_COUNT_DISTINCT(b.orderid) END AS weekendPurchase_weekdayRedeem,
  CASE WHEN DAYOFWEEK(createDate) in (2,3,4,5,6) AND DAYOFWEEK(redeemdate) in (1,7) THEN EXACT_COUNT_DISTINCT(b.orderid) END AS weekdayPurchase_weekendRedeem,
  CASE WHEN DAYOFWEEK(createDate) in (2,3,4,5,6) AND DAYOFWEEK(redeemdate) in (2,3,4,5,6) THEN EXACT_COUNT_DISTINCT(b.orderid) END AS weekdayPurchase_weekdayRedeem,
  SUM(CASE WHEN redeemdate is null THEN 1 ELSE 0 END) AS unredeemedVouchers,
--  FLOOR((redeemdate - createdate)/1000000/60) AS redeemtimediff,
--  ROUND((SQRT(((69.1 * (redemptionlat - buyinglat)) * (69.1 * (redemptionlat - buyinglat)))+ ((69.1 * (redemptionlong - buyinglong) * COS(buyinglat / 57.3)) * (69.1 * (redemptionlong - buyinglong) * COS(buyinglat / 57.3)))) * 1.60934),2) AS distanceinkm
FROM
(SELECT
  orderid,
  voucherid,
  MSEC_TO_TIMESTAMP(redemptiondate+19800000 ) AS redeemDate,
  MSEC_TO_TIMESTAMP( createdat+19800000 ) AS createdate,
  redemptionlat,
  redemptionlong,
FROM [big-query-1233:Atom.order_line]
WHERE ispaid='t'
AND finalprice >0
GROUP BY 1,2,3,4,5,6
) AS b
LEFT JOIN(
SELECT
  orderid,
  customerid,
  buyinglat,
  buyinglong
FROM [big-query-1233:Atom.order_header]
WHERE ispaid='t'
AND totalprice>0
GROUP BY 1,2,3,4
) AS c
ON b.orderid = c.orderid
WHERE LENGTH(c.customerid)>5
GROUP BY
1,redeemdate,createDate--,distanceinkm
)
GROUP BY
1
) as a

left join (
select
  customerid,
  avg(FLOOR((redeemdate - createdate)/1000000/60)) as redeemtimediff_mintues
from
(SELECT
  orderid,
  voucherid,
  MSEC_TO_TIMESTAMP(redemptiondate+19800000 ) AS redeemDate,
  MSEC_TO_TIMESTAMP( createdat+19800000 ) AS createdate,
FROM [big-query-1233:Atom.order_line]
WHERE ispaid='t'
AND finalprice >0
and isautoredeem<>'t'
AND categoryid <> 'GTW'
GROUP BY 1,2,3,4
) AS b
  
LEFT JOIN(
SELECT
  orderid,
  customerid
FROM [big-query-1233:Atom.order_header]
WHERE ispaid='t'
AND totalprice>0
GROUP BY 1, 2
) AS c
ON b.orderid = c.orderid
GROUP BY 1
) as b
on a.customerid=b.customerid";
v_destination_tbl="${v_dataset_name}.post_purchase";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}" &
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating post_purchase: $v_task_status";


v_subtask="User Attributes Step 9: post_purchase creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 9: post_purchase


## Table 10: user_rating_history


v_query="SELECT
  customerId AS int_customerId,
  CAST(customerId AS STRING) AS customerId,
  ROUND(SUM(timesMerchantrated * avgMerchantRating)/SUM(timesMerchantRated),2) AS avgRating,
  NTH(1,merchantId) AS mostRatedMerchant,
  SUM(timesMerchantRated) AS timesRatingGiven,
  ROUND(NTH(1,avgMerchantRating),2) AS mostRatedMerchantRating,
  highestRatedMerchant,
  highestAvgRating  
FROM
(SELECT
  fromMemberId AS customerId,
  toMemberId AS merchantId,
  COUNT( toMemberId) timesMerchantRated,
  AVG( rating) AS avgMerchantRating
FROM
(SELECT
  fromMemberId,
  toMemberId,
  rating
FROM [big-query-1233:Atom.ratings_and_reviews]
  )
WHERE LENGTH(STRING(fromMemberId))>8
GROUP BY 1,2
ORDER BY 3 desc
) AS a

LEFT JOIN (
SELECT
  fromMemberId,
  NTH(1,highestRated) as highestRatedMerchant,
  NTH(1,highestAvgRating) as highestAvgRating
FROM (
SELECT
  fromMemberId,
  toMemberId as highestRated,
  AVG(rating) as highestAvgRating,
  COUNT(rating)
FROM
  [big-query-1233:Atom.ratings_and_reviews]
WHERE LENGTH(STRING(fromMemberId))>8
GROUP BY 1,2
ORDER BY 3 desc,4 desc
)
GROUP BY 1
) AS b
ON a.customerId=b.fromMemberId
GROUP BY 1, 2,7, 8";

v_destination_tbl="${v_dataset_name}.user_rating_history";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}" &
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating user_rating_history: $v_task_status";


v_subtask="User Attributes Step 10: user_rating_history creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 10: user_rating_history


## Table 11: txn_status


v_query="SELECT
  b.customerid as customerid,
  SUM(CASE WHEN  voucherstatus = 'Cancelled' OR orderline_status = 17 THEN 1 ELSE 0 END) AS cancellations,
  SUM(CASE WHEN (redemptiondate is not null OR voucherstatus = 'Redeemed') and (paymentstatus IS NULL OR  paymentstatus <> 5) THEN 1 ELSE 0 END) AS redeemed,
  SUM(CASE WHEN paymentstatus=5 AND  voucherstatus = 'Redeemed' THEN 1 ELSE 0 END) AS refunds,
  SUM(CASE WHEN expiredat is null THEN 0 ELSE 1 END) AS expired,
  SUM(CASE WHEN voucherstatus = 'Active' THEN 1 ELSE 0 END ) AS active
  
FROM
(SELECT orderid,
        voucherid,
        ispaid,
        iscancelled,
        redemptiondate,
        paymentstatus,
        expiredat,
        status AS orderline_status,
        voucherstatus
FROM [big-query-1233:Atom.order_line]
WHERE dealid <> '14324'
  AND dealid NOT IN (select STRING(deal_id) from dbdev.dtr_deals_live)
  AND finalprice > 0
  AND ispaid = 't'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
) AS a

LEFT JOIN (SELECT orderid,
                   customerid
            FROM [big-query-1233:Atom.order_header]
            WHERE ispaid = 't'
         ) as b
ON a.orderid = b.orderid
WHERE b.customerid is not null
GROUP BY customerid";
v_destination_tbl="${v_dataset_name}.txn_status";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}" &
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating txn_status: $v_task_status";


v_subtask="User Attributes Step 11: txn_status creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 11: txn_status


## Table 12: customer_cohort


v_query="SELECT 
  cid AS int_customerId,
  CAST(cid AS STRING) AS customerId,
  cohort
FROM [big-query-1233:customer_cohort.customer_cohort_user_cohort]
GROUP BY 1, 2, 3";

v_destination_tbl="${v_dataset_name}.customer_cohort";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}" &
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating customer_cohort: $v_task_status";


v_subtask="User Attributes Step 12: customer_cohort creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 12: customer_cohort




## Table 13 (a): mall_market_street_info


v_query="SELECT mallName 
                  , a polygonId, total
                  , selling, b mallMarket
                  , cityName, round(100*selling/total,2) as penetration 
           FROM (SELECT redemptionAddress.mallDetail._id a 
                        , count(*) total
                        , count(t) selling
                        , MAX( x) mallName
                        , MAX(y) cityName
                        , 'MALL' b 
                 FROM (SELECT _id
                              ,MAX(deals._id) t
                              , redemptionAddress.mallDetail._id
                              , MAX(redemptionAddress.mallDetail.name) x
                              , MAX(redemptionAddress.city.name) y
                        FROM [big-query-1233:Atom.transformed_merchants] where 
                        isDeleted = false AND isActive = true AND isPublished = true AND isDuplicate = false AND ischain =false
                        AND transformedMerchantId.catId='FNB'
                        AND redemptionAddress.mallDetail._id  is not null
                        GROUP BY _id ,redemptionAddress.mallDetail._id 
                        )

                  GROUP BY a
                  )
                  ,
                  (
                  SELECT redemptionAddress.marketDetail._id a 
                         , count(*) total, count(t) selling, MAX( x) mallName
                         , MAX(y) cityName, 'MARKET' b 
                  FROM (SELECT _id,MAX(deals._id) t,  redemptionAddress.marketDetail._id, MAX(redemptionAddress.marketDetail.name) x,
                          MAX(redemptionAddress.city.name) y
                          FROM [big-query-1233:Atom.transformed_merchants] 
                          WHERE isDeleted = false AND isActive = true AND isPublished = true 
                            AND isDuplicate = false AND ischain =false
                            AND transformedMerchantId.catId='FNB'
                            AND redemptionAddress.marketDetail._id  is not null
                          GROUP BY _id ,redemptionAddress.marketDetail._id 
                        )
                  GROUP BY a
                  )
                  ,
                  (
                  SELECT redemptionAddress.streetDetail._id a ,count(*) total,count(t) selling,MAX( x) mallName, MAX(y) cityName, 'STREET' b 
                  FROM (SELECT _id,MAX(deals._id) t,  redemptionAddress.streetDetail._id
                               , MAX(redemptionAddress.streetDetail.name) x
                               , MAX(redemptionAddress.city.name) y
                        FROM [big-query-1233:Atom.transformed_merchants] 
                        WHERE isDeleted = false AND isActive = true AND isPublished = true 
                          AND isDuplicate = false AND ischain =false
                          AND transformedMerchantId.catId='FNB'
                          AND redemptionAddress.streetDetail._id  is not null
                        GROUP BY _id ,redemptionAddress.streetDetail._id 
                      )
                  GROUP BY a
                  )
order by 6,5,7,3 desc,4 desc";



v_destination_tbl="${v_dataset_name}.mall_market_street_info";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}" &
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating mall_market_street_info: $v_task_status";


v_subtask="User Attributes Step 13 (a): mall_market_street_info creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 13 (a): mall_market_street_info




## Table 13 (b): most_visited


v_query="SELECT  customerid AS int_customerid , 
        CAST(customerid AS STRING) AS customerid,
        mostVisitedPlace,
        mostVisitedPlaceCity,
        times1,
        m1.polygonId AS polygonId1,
        m1.total as totalMerchants1,
        m1.selling as sellingMerchants1,
        secMostVisitedPlace,
        secMostVisitedPlaceCity,
        times2,
        m2.polygonId AS polygonId2,
        m2.total as totalMerchants2,
        m2.selling as sellingMerchants2,
        thirdMostVisitedPlace,
        thirdMostVisitedPlaceCity,
        times3,
        m3.polygonId AS polygonId3,
        m3.total as totalMerchants3,
        m3.selling as sellingMerchants3,
FROM (SELECT   customerid , 
                NTH(1, hotspot) AS mostVisitedPlace,
                NTH(1, hotspotCity) AS mostVisitedPlaceCity,
                NTH(1, visits) AS times1,
                NTH(2, hotspot) AS secMostVisitedPlace,
                NTH(2, hotspotCity) AS secMostVisitedPlaceCity,
                NTH(2, visits) AS times2,
                NTH(3, hotspot) AS thirdMostVisitedPlace,
                NTH(3, hotspotCity) AS thirdMostVisitedPlaceCity,
                NTH(3, visits) AS times3
      FROM (SELECT ul.customerId as customerid, mms.name as hotspot
                    , mms.city as hotspotCity
                    , count(hotspotEntered) as visits
            FROM cerebro.user_location as ul
            join Atom.mall_market_street as mms on ul.hotspotEntered = mms._id
            where ul.hotspotEntered is not null AND ul.distanceFromHotspot < 25
            GROUP BY 1,2,3
      order by visits desc
            )
      GROUP BY 1
) as x

LEFT JOIN [engg_reporting.mall_market_street_info] as m1
on (x.mostVisitedPlace=m1.mallName
  AND x.mostVisitedPlaceCity=m1.cityName)
LEFT JOIN  [engg_reporting.mall_market_street_info] AS m2
on (x.secMostVisitedPlace=m2.mallName
  AND x.secMostVisitedPlaceCity=m2.cityName)
LEFT JOIN  [engg_reporting.mall_market_street_info] as m3
on (x.thirdMostVisitedPlace=m3.mallName
  AND x.thirdMostVisitedPlaceCity=m3.cityName)
GROUP BY int_customerid, 
          customerid, 
          mostVisitedPlace, 
          mostVisitedPlaceCity, 
          times1, 
          polygonId1, 
          totalMerchants1, 
          sellingMerchants1, 
          secMostVisitedPlace,
          secMostVisitedPlaceCity, 
          times2, 
          polygonId2, 
          totalMerchants2, 
          sellingMerchants2, 
          thirdMostVisitedPlace, 
          thirdMostVisitedPlaceCity, 
          times3, 
          polygonId3, 
          totalMerchants3, 
          sellingMerchants3";

v_destination_tbl="${v_dataset_name}.most_visited";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}" &
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating most_visited: $v_task_status";


v_subtask="User Attributes Step 13: most_visited creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 13 (b): most_visited



## Table 14: engagement_behaviour


v_query="SELECT
  a.customerid as customerid,
  b.PN_scheduled as PN_scheduled,
  b.PN_delivered as PN_delivered,
  b.PN_opened as PN_opened,
  b.PN_failed as PN_failed,
  b.sms_scheduled as sms_scheduled,
  b.sms_delivered as sms_delivered,
  b.sms_clicked as sms_clicked,
  b.sms_failed as sms_failed,
  b.inApp_scheduled as inApp_scheduled,
  b.inApp_delivered as inApp_delivered,
  b.inApp_opened as inApp_opened,
  b.inApp_failed as inApp_failed,
  c.email_bulkSent as email_bulkSent,
  c.email_eventSent as email_eventSent,
  c.email_bulkbounce as email_bulkbounce,
  c.email_open as email_open,
  c.email_click as email_click,
  c.email_unsubscribe as email_unsubscribe
FROM (SELECT  customerId,
              name,
              gender,
              primaryemailaddress
    FROM [big-query-1233:Atom.customer]
    WHERE isValidated=true
    ) as a

LEFT JOIN (SELECT  userid as customerid,
                  COUNT(case when lifecyclestatus=10 and communicationMedium=2 then 1 end ) as PN_scheduled,
                  COUNT(case when lifecyclestatus=40 and communicationMedium=2 then 1 end ) as PN_delivered,
                  COUNT(case when lifecyclestatus=60 and communicationMedium=2 then 1 end ) as PN_opened,
                  COUNT(case when lifecyclestatus=80 and communicationMedium=2 then 1 end ) as PN_failed,
                  COUNT(case when lifecyclestatus=10 and communicationMedium=3 then 1 end ) as sms_scheduled,
                  COUNT(case when lifecyclestatus=40 and communicationMedium=3 then 1 end ) as sms_delivered,
                  COUNT(case when lifecyclestatus=70 and communicationMedium=3 then 1 end ) as sms_clicked,
                  COUNT(case when lifecyclestatus=80 and communicationMedium=3 then 1 end ) as sms_failed,
                  COUNT(case when lifecyclestatus=10 and communicationMedium=4 then 1 end ) as inApp_scheduled,
                  COUNT(case when lifecyclestatus=40 and communicationMedium=4 then 1 end ) as inApp_delivered,
                  COUNT(case when lifecyclestatus=60 and communicationMedium=4 then 1 end ) as inApp_opened,
                  COUNT(case when lifecyclestatus=80 and communicationMedium=4 then 1 end ) as inApp_failed,
          FROM [big-query-1233:Atom.message] 
          group by 1
          ) as b
ON a.customerid = b.customerid
left join (SELECT
              uid,
              count(case when Event_Type_ID=1 then 1 end) as email_bulkSent,
              count(case when Event_Type_ID=2 then 1 end) as email_eventSent,
              count(case when Event_Type_ID=3 then 1 end) as email_bulkbounce,
              count(case when Event_Type_ID=10 then 1 end) as email_open,
              count(case when Event_Type_ID=20 then 1 end) as email_click,
              count(case when Event_Type_ID=50 then 1 end) as email_unsubscribe
            FROM (TABLE_DATE_RANGE([big-query-1233:cheetah.cheetah_], TIMESTAMP(DATE_ADD(TIMESTAMP(CURRENT_DATE()),-365,'DAY')), TIMESTAMP(CURRENT_DATE())))
            GROUP BY 1
            ) as c
on a.primaryemailaddress=c.uid";

v_destination_tbl="${v_dataset_name}.engagement_behaviour";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}" &
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating engagement_behaviour: $v_task_status";


v_subtask="User Attributes Step 14: engagement_behaviour creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 14: engagement_behaviour


## Table 15: user_txn_attributes


v_query="Select x.customerid as customerid,
        x.buffet as buffet,
        z.favbuffetdeal as favbuffetdeal,
        x.brunch as brunch,
        p.favbrunchdeal as favbrunchdeal,
        x.desserts as desserts,
        r.favdessertdeal as favdessertdeal,
        x.nonVeg_veg as nonVeg_Veg,
        q.favnonVeg_vegdeal as favnonVeg_vegdeal,
        x.alcoholic as alcoholic,
        s.favalcoholicdeal as favalcoholicdeal,
        x.unlimitedDeals as unlimitedDeals,
        u.favunlimiteddeal as favunlimiteddeal,
        x.withkids as withkids,
        t.favwithkidsdeal as favwithkidsdeal,
        breakfast,
        lunch,
        dinner,
        italianCuisine,
        southIndianCuisine
from (SELECT
      customerid as customerid,
      if(sum(buffet)>0,'Eats Buffet','No Buffet orders') as buffet,
      if(sum(breakfast)>0,'Breakfast','No Breakfast') as breakfast,
      if(sum(lunch)>0,'Lunch','No Lunch') as lunch,
      if(sum(dinner)>0,'Dinner','No Dinner') as dinner,
      if(sum(brunch)>0,'Brunch','No Brunch') as brunch,
      if(sum(nonveg)>0,'Non-Veg',if(sum(veg)>0   AND sum(nonveg)=0,'Veg','Can\'t Say')) as nonVeg_Veg,
      if(sum(alcoholic)>0,'Drinks Alcohol',if(sum(alcoholic)=0   AND sum(nonAlcoholicDrinks)>0,'Only Non-alcoholic drinks','No Drinks')) as alcoholic,
      if(sum(unlimited)>0,'Unlimited deals','No Unlimited deals') as unlimitedDeals,
      if(sum(italianCuisine)>0,'Orders Italian','No Italian Dish') as italianCuisine,
      if(sum(southIndianCuisine)>0,'Orders South Indian dish','No South Indian Dish') as southIndianCuisine,
      if(sum(dessert)>0,'Orders Dessert','No Dessert') as desserts,
      if(sum(withKids)>0,'Went with kids','Can\'t Say') as withKids
      FROM (SELECT 
              orderlineid,
              orderid,
              dealid,
              offerid,
              voucherid,
              offertitle,
              CASE WHEN UPPER(offertitle) like ('%BUFFET%') THEN 1 ELSE 0 END AS buffet,
              CASE WHEN UPPER(offertitle) like ('%BREAKFAST%')   AND categoryid='FNB' THEN 1 ELSE 0 END AS breakfast,
              CASE WHEN UPPER(offertitle) like ('%LUNCH%')   AND categoryid='FNB' THEN 1 ELSE 0 END AS lunch,
              CASE WHEN UPPER(offertitle) like ('%DINNER%')   AND categoryid='FNB' THEN 1 ELSE 0 END AS dinner,
              CASE WHEN UPPER(offertitle) like ('%BRUNCH%')   AND categoryid='FNB' THEN 1 ELSE 0 END AS brunch,
              CASE WHEN UPPER(offertitle) like ('%NON-VEG%') or UPPER(offertitle) like ('%NON VEG%') or UPPER(offertitle) like ('%CHICKEN%') THEN 1 ELSE 0 END AS nonVeg,
              CASE WHEN UPPER(offertitle) like ('%VEG%') THEN 1 ELSE 0 END AS veg,
              CASE WHEN upper(offertitle) like ('%COCKTAIL%') or UPPER(offertitle) like ('%BEER%') or UPPER(offertitle) like ('%PITCHER%') or UPPER(offertitle) like ('%WINE%') or UPPER(offertitle) like ('%WHISKEY%') or UPPER(offertitle) like ('%IMFL%') or UPPER(offertitle) like ('%PITCHER%') or UPPER(offertitle) like ('%PINT%') or UPPER(offertitle) like ('%TEQUILA%') or UPPER(offertitle) like ('%DOMESTIC DRINKS%')  THEN 1 ELSE 0 END AS alcoholic,
              CASE WHEN upper(offertitle) like ('%MOCKTAIL%') or UPPER(offertitle) like ('%SOFT DRINKS%') or UPPER(offertitle) like ('%SOFTDRINKS%') THEN 1 ELSE 0 END AS nonAlcoholicDrinks,
              CASE WHEN upper(offertitle) like ('%UNLIMITED%') THEN 1 ELSE 0 END AS unlimited,
              CASE WHEN upper(offertitle) like ('%PIZZA%') or upper(offertitle) like ('%PASTA%') THEN 1 ELSE 0 END AS italianCuisine,
              CASE WHEN upper(offertitle) like ('%DOSA%') or UPPER(offertitle) like ('%IDLI%') or UPPER(offertitle) like ('%SAMBHAR%') THEN 1 ELSE 0 END AS southIndianCuisine,
              CASE WHEN upper(offertitle) like ('%DESSERT%') or UPPER(offertitle) like ('%SWEET%') THEN 1 ELSE 0 END AS dessert,
              CASE WHEN upper(offertitle) like ('%CHILD%') or UPPER(offertitle) like ('%KID%') THEN 1 ELSE 0 END AS withKids,
            FROM Atom.order_line
            WHERE ispaid='t'
              AND finalprice>0
            ) as a

      LEFT JOIN(
      SELECT
        orderid,
        customerid as customerid
      FROM
        Atom.order_header
      WHERE ispaid='t'
        AND totalprice>0) as b
      ON
        (a.orderid=b.orderid)
      WHERE length(customerid)>5
      GROUP BY 1
      ) as x
  
Left Join (
SELECT
  customerid,
  buffet,
  NTH(1,dealid) as favbuffetdeal
FROM (SELECT 
        customerid,
        CASE WHEN buffet=1 THEN 'Eats Buffet' END AS buffet,
        dealid,
        COUNT(dealid) as txncount
      FROM (SELECT 
              orderlineid,
              orderid,
              dealid,
              offerid,
              voucherid,
              offertitle,
              CASE WHEN UPPER(offertitle) like ('%BUFFET%') THEN 1 ELSE 0 END AS buffet,
            FROM Atom.order_line
            WHERE ispaid='t'
              AND finalprice>0
            ) as a

      LEFT JOIN  (SELECT orderid,
                         customerid as customerid
                FROM Atom.order_header
                WHERE ispaid='t'
                  AND totalprice>0
                  ) as b
      ON (a.orderid=b.orderid)
      GROUP BY 1, 3, buffet
      ORDER BY 4 desc
      )
GROUP BY 1,2
) as z
on (x.customerid=z.customerid
    AND  x.buffet=z.buffet)
  
Left Join (
SELECT
  customerid,
  brunch,
  NTH(1,dealid) as favbrunchdeal
FROM
(SELECT 
  customerid,
  CASE WHEN brunch=1 then 'Brunch'
       end as brunch,
  dealid,
  COUNT(dealid) as txncount
FROM (SELECT  orderlineid,
              orderid,
              dealid,
              offerid,
              voucherid,
              offertitle,
              CASE WHEN UPPER(offertitle) like ('%BRUNCH%')   AND categoryid='FNB' THEN 1 ELSE 0 END AS brunch,
    FROM Atom.order_line
    WHERE ispaid='t'
      AND finalprice>0
    ) as a

LEFT JOIN (SELECT orderid,
                  customerid as customerid
          FROM Atom.order_header
          WHERE ispaid='t'
            AND totalprice>0
          ) as b
ON (a.orderid=b.orderid)
GROUP BY 1, 3, brunch
ORDER BY 4 desc
)
GROUP BY 1,2
) as p
on
  (x.customerid=p.customerid
    AND  x.brunch=p.brunch)
Left Join (SELECT
              customerid,
              nonVeg_Veg,
              NTH(1,dealid) as favnonVeg_Vegdeal
            FROM
            (SELECT 
              customerid,
              CASE WHEN nonVeg=1 then 'Non-Veg'
                   when nonVeg=0   AND Veg=1 then 'Veg' 
                   /*when nonVeg=0   AND veg=0 then 'Can\'t Say'*/ end as nonVeg_Veg,
              dealid,
              COUNT(dealid) as txncount
            FROM (SELECT  orderlineid,
                          orderid,
                          dealid,
                          offerid,
                          voucherid,
                          offertitle,
                          CASE WHEN UPPER(offertitle) like ('%NON-VEG%') or UPPER(offertitle) like ('%NON VEG%') or UPPER(offertitle) like ('%CHICKEN%') 
                                  THEN 1 
                                  ELSE 0 END AS nonVeg,
                          CASE WHEN UPPER(offertitle) like ('%VEG%') THEN 1 ELSE 0 END AS veg,
                  FROM Atom.order_line
                  WHERE ispaid='t'
                    AND finalprice>0
                  ) as a

            LEFT JOIN(SELECT  orderid,
                              customerid as customerid
                      FROM Atom.order_header
                      WHERE ispaid='t'
                        AND totalprice>0) as b
                      ON (a.orderid=b.orderid)
                      GROUP BY 1,3,nonVeg,veg,nonVeg_veg
                      ORDER BY 4 desc
              )
            GROUP BY 1,2
          ) as q
on
  (x.customerid=q.customerid
    AND  x.nonVeg_Veg=q.nonVeg_Veg)

Left Join (
SELECT
  customerid,
  dessert,
  NTH(1,dealid) as favdessertdeal
FROM (SELECT 
        customerid,
        CASE WHEN dessert=1 then 'Orders Dessert'
             /*when dessert=0 then 'No Dessert'*/ end as dessert,
        dealid,
        COUNT(dealid) as txncount
      FROM (SELECT 
              orderlineid,
              orderid,
              dealid,
              offerid,
              voucherid,
              offertitle,
              CASE WHEN upper(offertitle) like ('%DESSERT%') or UPPER(offertitle) like ('%SWEET%') THEN 1 ELSE 0 END AS dessert,
            FROM Atom.order_line
            WHERE ispaid='t'
              AND finalprice>0
            ) as a

      LEFT JOIN(SELECT  orderid,
                        customerid as customerid
                FROM Atom.order_header
                WHERE ispaid='t'
                  AND totalprice>0
               ) as b
      ON (a.orderid=b.orderid)
      GROUP BY 1,3,dessert
      ORDER BY 4 desc
      )
GROUP BY 1,2
) as r
on
  (x.customerid=r.customerid
    AND  x.desserts=r.dessert)

Left Join (
SELECT customerid,
        alcoholic,
        NTH(1,dealid) as favalcoholicdeal
FROM (SELECT 
        customerid,
        CASE WHEN alcoholic=1 then 'Drinks Alcohol'
             when alcoholic=0   AND nonalcoholicdrinks=1 then 'Only Non-alcoholic drinks'
             /*when alcoholic=0   AND nonalcoholicdrinks=0 then 'No Drinks'*/ end as alcoholic,
        dealid,
        COUNT(dealid) as txncount
      FROM (SELECT 
              orderlineid,
              orderid,
              dealid,
              offerid,
              voucherid,
              offertitle,
              CASE WHEN upper(offertitle) like ('%COCKTAIL%') or UPPER(offertitle) like ('%BEER%') or UPPER(offertitle) like ('%PITCHER%') or UPPER(offertitle) like ('%WINE%') THEN 1 ELSE 0 END AS alcoholic,
              CASE WHEN upper(offertitle) like ('%MOCKTAIL%') or UPPER(offertitle) like ('%SOFT DRINKS%') or UPPER(offertitle) like ('%SOFTDRINKS%') THEN 1 ELSE 0 END AS nonAlcoholicDrinks,
            FROM Atom.order_line
            WHERE ispaid='t'
              AND finalprice>0
            ) as a

      LEFT JOIN (SELECT orderid,
                        customerid as customerid
                FROM Atom.order_header
                WHERE ispaid='t'
                  AND totalprice>0
                ) as b
      ON (a.orderid=b.orderid)
      GROUP BY 1,3,alcoholic
      ORDER BY 4 desc
      )
GROUP BY 1,2
) as s
on
  (x.customerid=s.customerid
    AND  x.alcoholic=s.alcoholic)

Left Join (
SELECT
  customerid,
  withKids,
  NTH(1,dealid) as favwithKidsdeal
FROM
    (SELECT customerid,
            CASE WHEN withKids=1 then 'Went with kids' END AS withKids,
            dealid,
            COUNT(dealid) as txncount
    FROM (SELECT 
            orderlineid,
            orderid,
            dealid,
            offerid,
            voucherid,
            offertitle,
            CASE WHEN upper(offertitle) like ('%CHILD%') or UPPER(offertitle) like ('%KID%') THEN 1 ELSE 0 END AS withKids,
          FROM Atom.order_line
          WHERE ispaid='t'
            AND finalprice>0
          ) as a

    LEFT JOIN (SELECT
                  orderid,
                  customerid as customerid
                FROM
                  Atom.order_header
                WHERE ispaid='t'
                  AND totalprice>0) as b
    ON  (a.orderid=b.orderid)
    GROUP BY 1,3,withKids
    ORDER BY 4 desc
    )
GROUP BY 1,2
) as t
on
  (x.customerid=t.customerid
    AND  x.withKids=t.withKids)

Left Join (SELECT customerid,
                    unlimited,
                    NTH(1,dealid) as favunlimiteddeal
            FROM (SELECT  customerid,
                          CASE WHEN unlimited=1 then 'Unlimited deals'
                               /*when unlimited=0 then 'No Unlimited deals'*/ end as unlimited,
                          dealid,
                          COUNT(dealid) as txncount
                  FROM
                  (SELECT   orderlineid,
                            orderid,
                            dealid,
                            offerid,
                            voucherid,
                            offertitle,
                            CASE WHEN upper(offertitle) like ('%UNLIMITED%') THEN 1 ELSE 0 END AS unlimited,
                  FROM Atom.order_line
                  WHERE ispaid='t'
                    AND finalprice>0
                  ) as a

                  LEFT JOIN(SELECT
                              orderid,
                              customerid as customerid
                            FROM
                              Atom.order_header
                            WHERE ispaid='t'
                              AND totalprice>0) as b
                            ON
                              (a.orderid=b.orderid)
                            GROUP BY 1,3,unlimited
                            ORDER BY 4 desc
                          )
            GROUP BY 1,2
) as u
on
  (x.customerid=u.customerid
  AND  x.unlimitedDeals=u.unlimited)";

v_destination_tbl="${v_dataset_name}.user_txn_attributes";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}" &
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating user_txn_attributes: $v_task_status";


v_subtask="User Attributes Step 15: user_txn_attributes creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 15: user_txn_attributes



## Table 16: user_attributes_non_GA
v_query="select 
  a.customerid as customerid,
  a.name as name,
  a.gender as gender,
  a.dob_day as dob_day,
  a.dob_month as dob_month,
  a.dob_year as dob_year,
  a.firstPurchaseDate as firstPurchaseDate,
  a.lastPurchaseDate as lastPurchaseDate,
  a.totalTxn as totalTxn,
  a.distinctOffersBought as distinctOffersBought,
  a.voucherperTxn as voucherperTxn,
  a.totalVouchers as totalVouchers,
  a.txnFrequency as txnFrequency,
  a.percDiscountAffinty as percDiscountAffinty,
  b.buffet as buffet,
  b.favbuffetdeal as favbuffetdeal,
  b.brunch as brunch,
  b.favbrunchdeal as favbrunchdeal,
  b.desserts as desserts,
  b.favdessertdeal as favdessertdeal,
  b.nonVeg_Veg as nonVeg_Veg,
  b.favnonVeg_vegdeal as favnonVeg_vegdeal,
  b.alcoholic as alcoholic,
  b.favalcoholicdeal as favalcoholicdeal,
  b.unlimitedDeals as unlimitedDeals,
  b.favunlimiteddeal as favunlimiteddeal,
  b.withkids as withkids,
  b.favwithkidsdeal as favwithkidsdeal,
  b.breakfast as breakfast,
  b.lunch as lunch,
  b.dinner as dinner,
  b.italianCuisine as italianCuisine,
  b.southIndianCuisine as southIndianCuisine,
  c.totalGB as totalGB,
  c.GR as totalGR,
  c.cashback AS cashback,
  c.GB_afterCB as GB_afterFirstCB,
  c.GR_afterCB as GR_afterFirstCB,
  d.weekendPurchase as weekendPurchase,
  d.weekdayPurchase as weekdayPurchase,
  d.weekendPurchase_weekendRedeem as weekendPurchase_weekendRedeem,
  d.weekendPurchase_weekdayRedeem as weekendPurchase_weekdayRedeem,
  d.weekdayPurchase_weekendRedeem as weekdayPurchase_weekendRedeem,
  d.weekdayPurchase_weekdayRedeem as weekdayPurchase_weekdayRedeem,
  d.unredeemedVouchers as unredeemedVouchers,
  d.redeemtimediff_mintues as redeemtimediff_mintues,
  e.cancellations AS cancellations,
  e.redeemed as redeemed,
  e.refunds as refunds,
  e.expired AS expired,
  k.validForOneTx as validForOneTx,
  k.validForTwoTx as validForTwoTx,
  k.validForMultipleTx as validForMultipleTx,
  f.latestDealTxn as latestTxnDeal,
  f.latestCatTxn as latestTxnCategory,
  f.latestTxnPricePoint AS latestTxnPricePoint,
  f.secLatestDealTxn as secLatestTxnDeal,
  f.secLatestCatTxn as seclatestTxnCategory,
  f.secLatestTxnPricePoint as secLatestTxnPricePoint,
  f.thirdLatestDealTxn as thirdLatestTxnDeal,
  f.thirdLatestCatTxn as thirdLatestTxnCategory,
  f.thirdLatestTxnPricePoint AS thirdLatestTxnPricePoint,
  g.MostTxnDeal AS MostTxnDeal,
  g.secondMostTxnDeal AS secondMostTxnDeal,
  g.thirdMostTxnDeal AS thirdMostTxnDeal,
  h.mostTxnCat AS mostTxnCat,
  h.mostTxnCatPricePoint AS mostTxnCatPricePoint,
  h.secMostTxnCat AS secMostTxnCat,
  h.secMostTxnPricePoint AS secMostTxnPricePoint,
  h.thirdMostTxnCat AS thirdMostTxnCat,
  h.thirdMostTxnPricePoint AS thirdMostTxnPricePoint,
  i.latestRedeemCity AS latestRedeemCity,
  i.secondLatRedeemCity AS secondLatRedeemCity,
  i.thirdLatRedeemCity AS thirdLatRedeemCity,
  (j.PN_scheduled+ j.PN_delivered+ j.PN_opened+ j.PN_failed) as PN_scheduled,
  (j.PN_delivered+ j.PN_opened) as PN_delivered,
  j.PN_opened as PN_opened,
  j.PN_failed as PN_failed,
  ( j.sms_scheduled+ j.sms_delivered+ j.sms_clicked+ j.sms_failed ) as sms_scheduled,
  (j.sms_delivered+ j.sms_clicked) as sms_delivered,
  j.sms_clicked as sms_clicked,
  j.sms_failed as sms_failed,
  (j.inApp_scheduled+ j.inApp_delivered+ j.inApp_opened + j.inApp_failed ) as inApp_scheduled,
  (j.inApp_delivered+ j.inApp_opened) as inApp_delivered,
  j.inApp_opened AS inApp_opened,
  j.inApp_failed AS inApp_failed,
  ( j.email_eventSent + j.email_open+ j.email_click ) as email_sent,
  (j.email_open+ j.email_click) as email_open,
  j.email_click AS email_click,
  j.email_unsubscribe AS email_unsubscribe,
  
  l.mostVisitedPlace AS mostVisitedPlace,
  l.mostVisitedPlaceCity AS mostVisitedPlaceCity,
  l.times1 as mostVisitedTimes,
  l.totalMerchants1 AS totalMerchantsAtMostVisitedPlace, 
  l.sellingMerchants1 AS sellingMerchantsAtMostVisitedPlace, 
  l.polygonId1 AS mostVisitedPlacePolygonId,

  l.secMostVisitedPlace as secMostVisitedPlace,
  l.secMostVisitedPlaceCity AS secMostVisitedPlaceCity,
  l.times2 as secMostVisitedTimes,
  l.totalMerchants2 AS totalMerchantsAtSecMostVisitedPlace, 
  l.sellingMerchants2 AS sellingMerchantsAtSecMostVisitedPlace, 
  l.polygonId2 AS secMostVisitedPlacePolygonId,

  
  l.thirdMostVisitedPlace as thirdMostVisitedPlace,
  l.thirdMostVisitedPlaceCity AS thirdMostVisitedPlaceCity,
  l.times3 as thirdMostVisitedTimes,
  l.totalMerchants3 AS totalMerchantsAtThirdMostVisitedPlace, 
  l.sellingMerchants3 AS sellingMerchantsAtThirdMostVisitedPlace, 
  l.polygonId3 AS thirdMostVisitedPlacePolygonId,


  m.avgRating as totalAvgRating,
  m.timesRatingGiven as timesRated,
  m.mostRatedMerchant AS mostRatedMerchant,
  m.mostRatedMerchantRating AS mostRatedMerchantRating,
  m.highestRatedMerchant as highestAvgRatingMerchant,
  m.highestAvgRating AS highestAvgRating,
  o.totalcreditsavailable as totalcreditsavailable,
  n.cohort AS cohort
from
  [engg_reporting.txn_summary] a
  
left join
  [engg_reporting.user_txn_attributes] b
  on (a.customerid=b.customerid)
  
left join
  [engg_reporting.gb_gr_cashback] c
  on (a.customerid=c.customerid)
  
left join
  [engg_reporting.post_purchase] d
  on (a.customerid=d.customerid)

left join
  [engg_reporting.txn_status] e
  on (a.customerid=e.customerid)
  
left join
  [engg_reporting.txn_valid_for] k
  on (a.customerid=k.customerid)

left join
  [engg_reporting.latest_three_txn] f
  on (a.customerid=f.customerid)

left join
  [engg_reporting.most_txns_deal] g
  on (a.customerid=g.customerid)
  
left join
  [engg_reporting.most_txn_category] h
  on (a.customerid=h.customerid)
  
left join
  [engg_reporting.latest_redeem_city] i
  on (a.customerid=i.customerid)
  
left join
  [engg_reporting.engagement_behaviour] j
  on (a.customerid=j.customerid)

left join 
  [engg_reporting.most_visited] l
  on (a.customerid=l.customerid)

left join(
select
  string(customerId) as customerid,
  avgRating,
  mostRatedMerchant,
  timesRatingGiven,
  mostRatedMerchantRating,
  highestRatedMerchant,
  highestAvgRating
from
  [engg_reporting.user_rating_history]) m
  on (a.customerid=m.customerid)
  
left join(
select
  string(customerid) as customerid,
  cohort,
from
  [engg_reporting.customer_cohort]) n
  on (a.customerid=n.customerid)

left join
  [Atom.user_credit_summary] o
  on (a.customerid=o.userid)";



v_destination_tbl="${v_dataset_name}.user_attributes_non_GA";

echo -e "bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl \"${v_query}\";"


/home/ubuntu/google-cloud-sdk/bin/bq query --maximum_billing_tier 1000 --allow_large_results=1 --replace -n 1 --destination_table=$v_destination_tbl "${v_query}" &
v_pid=$!


if wait $v_pid; then
    echo "Process $v_pid Status: success";
    v_task_status="success";
else 
    echo "Process $v_pid Status: failed";
    v_task_status="failed";
fi

echo `date` "Creating user_attributes_non_GA: $v_task_status";


v_subtask="User Attributes Step 16: user_attributes_non_GA (the LEFT JOIN of all non-GA tables) creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 16: user_attributes_non_GA




## Table 17: user_attributes_merchant_name


v_query="SELECT  p.dealId AS dealId
        , m.merchantName AS merchantName
        , m.merchantId as merchantID
FROM (SELECT
        id AS dealId,
        CASE
          WHEN mappings.chain.id IS NOT NULL THEN mappings.chain.id
          ELSE mappings.merchant.id
        END AS merchantId,
      FROM FLATTEN(FLATTEN(Atom.mapping, mappings.merchant),mappings.chain)
    ) AS p
JOIN (
        SELECT  STRING( merchantId ) AS merchantId
                , name AS merchantName
        FROM [big-query-1233:Atom.merchant] 
  ) AS m
ON  m.merchantId = p.merchantId
GROUP BY 1,  2,  3";


v_destination_tbl="${v_dataset_name}.user_attributes_merchant_name";

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

echo `date` "Creating Final Table  'user_attributes_merchant_name' : $v_task_status";


v_subtask="Final Table  'user_attributes_merchant_name' ";
p_exit_upon_error "$v_task_status" "$v_subtask";


## Completed Table 17

v_task_end_time=`date`;


echo "Task started at ${v_task_start_time} and ended at ${v_task_end_time}.";

exit 0;
