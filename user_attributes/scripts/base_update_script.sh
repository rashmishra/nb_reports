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



## Table 0: merchant_with_chain_id
# Chain Merchant IDs for a Merchant at Deal level


v_query="SELECT id AS Deal_ID, mappings.chain.id AS Chain_Merchant_ID
       , mappings.merchant.id AS Merchant_ID
FROM FLATTEN([Atom.mapping] , mappings.chain.id)
WHERE type = 'deal'
GROUP BY Chain_Merchant_ID, Merchant_ID, Deal_ID";


v_destination_tbl="${v_dataset_name}.merchant_with_chain_id";

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

echo `date` "Creating merchant_with_chain_id: $v_task_status";


v_subtask="User Attributes Step 0: merchant_with_chain_id creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 0: merchant_with_chain_id



## Table 1 (a): price_point_base
                                    # Category (Transaction)
                                    # Primary SKU (Transaction)
                                    # Price point (Transaction)

v_query="SELECT  a.customerId AS customerId,
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
      GROUP BY 1,2,3,4";

v_destination_tbl="${v_dataset_name}.price_point_base";

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

echo `date` "Creating price_point_base: $v_task_status";


v_subtask="User Attributes Step 1 (a): price_point_base creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 1 (a): price_point_base

## Table 1 (b): latest_three_txn
                                    # Category (Transaction)
                                    # Primary SKU (Transaction)
                                    # Price point (Transaction)

v_query="SELECT  customerId,
        NTH(1,Effective_Merchant_ID) AS latestMerchantTxn,
        NTH(1,categoryId) AS latestCatTxn,
        NTH(1,pricePoint) AS latestTxnPricePoint,
        NTH(2,Effective_Merchant_ID) AS secLatestMerchantTxn,
        NTH(2,categoryId) AS secLatestCatTxn,
        NTH(2,pricePoint) AS secLatestTxnPricePoint,
        NTH(3,Effective_Merchant_ID) AS thirdLatestMerchantTxn,
        NTH(3,categoryId) AS thirdLatestCatTxn,
        NTH(3,pricePoint) AS thirdLatestTxnPricePoint
FROM (SELECT  customerId, dealId, categoryId, txn_time, pricePoint
        , COALESCE( Chain_Merchant_ID, Merchant_ID) AS Effective_Merchant_ID
      FROM ${v_dataset_name}.price_point_base pp
      LEFT JOIN [engg_reporting.merchant_with_chain_id] ch
          ON pp.dealId = ch.Deal_ID
      ORDER BY txn_time DESC
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


v_subtask="User Attributes Step 1 (b): latest_three_txn creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 1 (b): latest_three_txn

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


## Table 3 (a): most_txns_merchant_base


v_query="SELECT a.customerId AS customerId,
            COALESCE(ch.Chain_Merchant_ID, ch.Merchant_ID ) AS Effective_Merchant_ID,
            COUNT(dealId) AS dealCount
      FROM (SELECT  orderid,
                    customerid
            FROM [big-query-1233:Atom.order_header]
            WHERE ispaid='t'
              AND totalprice>0
           ) AS a

LEFT JOIN (SELECT   orderid,
                    dealid,
                    categoryid
            FROM [big-query-1233:Atom.order_line]
            WHERE ispaid='t'
              AND finalprice>0
           ) AS b
  ON a.orderid = b.orderid
LEFT JOIN [${v_dataset_name}.merchant_with_chain_id] ch
  ON b.dealid = ch.Deal_ID
WHERE b.dealid<> '14324'
  AND b.dealid NOT IN (select STRING(deal_id) from dbdev.dtr_deals_live)
GROUP BY 1,2";

v_destination_tbl="${v_dataset_name}.most_txns_merchant_base";

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

echo `date` "Creating most_txns_merchant_base: $v_task_status";


v_subtask="User Attributes Step 3 (a): most_txns_merchant_base creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 3 (a): most_txns_merchant_base


## Table 3 (b): most_txns_deal


v_query="SELECT
  customerId,
  NTH(1,Effective_Merchant_ID) AS MostTxnMerchant,
  NTH(2,Effective_Merchant_ID) AS secondMostTxnMerchant,
  NTH(3,Effective_Merchant_ID) AS thirdMostTxnMerchant
FROM (SELECT customerId
             , Effective_Merchant_ID
             , dealCount
      FROM [${v_dataset_name}.most_txns_merchant_base]
ORDER BY dealCount DESC
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


v_subtask="User Attributes Step 3 (b): most_txns_deal creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 3 (b): most_txns_deal


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

v_query="SELECT  a.customerid AS customerid
        , a.name AS name
        , a.gender AS gender
        , a.dob.day AS dob_day
        , a.dob.month AS dob_month
        , a.dob.year AS dob_year
        , b.raffleFirstPurchaseDate AS raffleFirstPurchaseDate
        , b.nonRaffleFirstPurchaseDate AS nonRaffleFirstPurchaseDate
        , b.nonRaffleLastPurchaseDate AS nonRaffleLastPurchaseDate
        , b.raffleLastPurchaseDate AS raffleLastPurchaseDate
        , b.totalNonRaffleTxn AS totalNonRaffleTxn
        , b.totalRaffleTxn AS totalRaffleTxn
        , b.totalNonRaffleVouchers AS totalNonRaffleVouchers
        , b.totalRaffleVouchers AS totalRaffleVouchers
        , b.percDiscountAffinty AS percDiscountAffinty
FROM (SELECT customerId
             , name
             , gender
             , dob.day
             , dob.month
             , dob.year
    FROM [big-query-1233:Atom.customer]
    WHERE isValidated=true
    ) AS a
LEFT JOIN (SELECT z.customerId AS customerId,
                  DATE(MIN(nonRafflePurchaseDate)) AS nonRaffleFirstPurchaseDate,
                  DATE(MAX(nonRafflePurchaseDate)) AS nonRaffleLastPurchaseDate,
                  DATE(MIN(rafflePurchaseDate)) AS raffleFirstPurchaseDate,
                  DATE(MAX(rafflePurchaseDate)) AS raffleLastPurchaseDate,
                  COUNT(UNIQUE(nonRaffleOrderId)) AS totalNonRaffleTxn,
                  COUNT(UNIQUE(raffleOrderId)) AS totalRaffleTxn,
                  COUNT(nonRaffleOfferId) AS nonRaffleoffersBought,
                  COUNT(raffleOfferId) AS raffleOffersBought,
                  COUNT(nonRaffleOrderlineId) AS totalNonRaffleVouchers,
                  COUNT(raffleOrderlineId) AS totalRaffleVouchers,
                  ROUND((SUM(CASE WHEN z.promocode IS NOT NULL
                                 THEN 1 
                               ELSE 0 
                             END)/COUNT(nonRaffleOrderId))*100
                         ,2) AS percDiscountAffinty
          FROM (SELECT orderlineid
                      , orderid
                      , dealid
                      , voucherid
                      , CASE WHEN finalprice > 0 THEN MSEC_TO_TIMESTAMP(redemptiondate + 19800000 ) ELSE NULL END AS nonRaffleRedeemDate
                      , CASE WHEN finalprice = 0 THEN MSEC_TO_TIMESTAMP(redemptiondate + 19800000 ) ELSE NULL END AS raffleRedeemDate
                      , CASE WHEN finalprice > 0 THEN MSEC_TO_TIMESTAMP(createdat + 19800000 ) ELSE NULL END AS nonRafflePurchaseDate
                      , CASE WHEN finalprice = 0 THEN MSEC_TO_TIMESTAMP(createdat + 19800000 ) ELSE NULL END AS rafflePurchaseDate
                      , CASE WHEN finalprice = 0 THEN offerid ELSE NULL END AS raffleOfferId
                      , CASE WHEN finalprice > 0 THEN offerid ELSE NULL END AS nonRaffleOfferId
                      , CASE WHEN finalprice = 0 THEN orderlineid ELSE NULL END AS raffleOrderlineId
                      , CASE WHEN finalprice > 0 THEN orderlineid ELSE NULL END AS nonRaffleOrderlineId
              FROM [big-query-1233:Atom.order_line]
              WHERE ispaid = 't'
              GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
              ) AS x
          LEFT JOIN (SELECT orderid
                            , promocode
                            , customerid
                            , CASE WHEN totalprice = 0 THEN orderid ELSE NULL END AS raffleOrderId
                            , CASE WHEN totalprice > 0 THEN orderid ELSE NULL END AS nonRaffleOrderId
                      FROM [big-query-1233:Atom.order_header]
                      WHERE ispaid='t'
                      GROUP BY 1, 2, 3, 4, 5
                    ) AS z
              ON (x.orderid = z.orderid)
          WHERE LENGTH(z.customerId)>5 
            AND x.dealid<> '14324'
            AND x.dealid NOT IN (SELECT STRING(deal_id) 
                                   FROM dbdev.dtr_deals_live)
          GROUP BY 1
          ) AS b
    ON (a.customerId=b.customerId)";
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
  unredeemedVouchers,
  ROUND(b.redeemtimediff_hours, 1) as redeemtimediff_hours
from
(SELECT
  customerid,
  SUM(weekendPurchase) AS weekendPurchase,
  SUM(weekdayPurchase) AS weekdayPurchase,
  SUM(unredeemedVouchers) AS unredeemedVouchers,
FROM
(SELECT 
  c.customerId as customerId,
  CASE WHEN DAYOFWEEK(createDate) in (1,6,7) THEN EXACT_COUNT_DISTINCT(b.orderid) END AS weekendPurchase,
  CASE WHEN DAYOFWEEK(createDate) in (2,3,4,5) THEN EXACT_COUNT_DISTINCT(b.orderid) END AS weekdayPurchase,
  SUM(CASE WHEN redeemdate is null THEN 1 ELSE 0 END) AS unredeemedVouchers
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
  AVG(FLOOR((redeemdate - createdate)/1000000/60)/60) as redeemtimediff_hours
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


## Table 13 (b): mms_visits

v_query="SELECT customerid
                   , hotspot
                   , hotspotCity
                   , COUNT(hotspotEntered) AS visits
            FROM (SELECT ul.customerId as customerid
                         , mms.name as hotspot
                         , mms.city as hotspotCity
                         , DATE(MSEC_TO_TIMESTAMP(ul.time + 19800000)) AS date_entered
                         , hotspotEntered
                    FROM cerebro.user_location as ul
                    INNER JOIN Atom.mall_market_street as mms on ul.hotspotEntered = mms._id
                    WHERE ul.hotspotEntered is not null AND ul.distanceFromHotspot < 25
                    GROUP BY 1, 2, 3, 4, 5
            )
            GROUP BY 1, 2, 3";


v_destination_tbl="${v_dataset_name}.mms_visits";

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

echo `date` "Creating mms_visits: $v_task_status";


v_subtask="User Attributes Step 13 (b): mms_visits creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 13 (b): mms_visits



## Table 13 (c): mms_most_visits

v_query="SELECT   customerid , 
                NTH(1, hotspot) AS mostVisitedPlace,
                NTH(1, hotspotCity) AS mostVisitedPlaceCity,
                NTH(1, visits) AS times1,
                NTH(2, hotspot) AS secMostVisitedPlace,
                NTH(2, hotspotCity) AS secMostVisitedPlaceCity,
                NTH(2, visits) AS times2,
                NTH(3, hotspot) AS thirdMostVisitedPlace,
                NTH(3, hotspotCity) AS thirdMostVisitedPlaceCity,
                NTH(3, visits) AS times3
      FROM (SELECT customerid
                   , hotspot
                   , hotspotCity
                   , visits
            FROM ${v_dataset_name}.mms_visits
            ORDER BY visits DESC
            )
      GROUP BY 1";


v_destination_tbl="${v_dataset_name}.mms_most_visits";

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

echo `date` "Creating mms_most_visits: $v_task_status";


v_subtask="User Attributes Step 13 (c): mms_most_visits creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 13 (c): mms_most_visits

## Table 13 (d): most_visited

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
FROM [${v_dataset_name}.mms_most_visits] as x
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


v_subtask="User Attributes Step 13 (d): most_visited creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 13 (d): most_visited



## Table 14 (a): engagement_PN_dim

v_query="SELECT customerid
       , latest_communication_date_PN
       , CAST(DATE(DATE_ADD(latest_communication_date_PN, -15, 'DAY')) AS DATE) AS t_minus_15
       , CAST(DATE(DATE_ADD(latest_communication_date_PN, -2, 'MONTH')) AS DATE) AS t_minus_60
FROM (SELECT  userid as customerid
        , CAST(DATE( MSEC_TO_TIMESTAMP(MAX( createdAt) + 19800000)) AS DATE) AS latest_communication_date_PN
FROM [big-query-1233:Atom.message] 
WHERE communicationMedium=2
  AND lifecyclestatus IN (40, 50, 60)
GROUP BY 1
)";

v_destination_tbl="${v_dataset_name}.engagement_PN_dim";

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

echo `date` "Creating engagement_PN_dim: $v_task_status";


v_subtask="User Attributes Step 14 (a): engagement_PN_dim creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 14 (a): engagement_PN_dim


## Table 14 (b): engagement_PN_active_base

v_query="SELECT dim.customerid AS customerid
                  , dim.latest_communication_date_PN AS latest_communication_date_PN
                  , CAST(DATE(dim.t_minus_15) AS DATE) AS latest_communication_date_PN_minus_15_days
                  , CAST(DATE(dim.t_minus_60) AS DATE) AS latest_communication_date_PN_minus_60_days
                  , COUNT(CASE WHEN lifecyclestatus=10 THEN 1 ELSE NULL END ) as PN_scheduled
                  , COUNT(CASE WHEN lifecyclestatus=40 THEN 1 ELSE NULL END ) as PN_delivered
                  , COUNT(CASE WHEN lifecyclestatus=50 THEN 1 ELSE NULL END ) as PN_dismissed
                  , COUNT(CASE WHEN lifecyclestatus=60 THEN 1 ELSE NULL END ) as PN_opened
                  , COUNT(CASE WHEN lifecyclestatus=80 THEN 1 ELSE NULL END ) as PN_failed
                  , COUNT(CASE WHEN lifecyclestatus=90 THEN 1 ELSE NULL END ) as PN_complaint
                  , COUNT(CASE WHEN lifecyclestatus=100 THEN 1 ELSE NULL END ) as PN_bounced
                  
                  , SUM(CASE WHEN DATE(MSEC_TO_TIMESTAMP( msg.createdAt + 19800000 )) >= DATE(dim.t_minus_15) AND lifecyclestatus=10 then 1 ELSE 0 end ) as PN_scheduled_t_minus_15
                  , SUM(CASE WHEN DATE(MSEC_TO_TIMESTAMP( msg.createdAt + 19800000 )) >= DATE(dim.t_minus_15) AND lifecyclestatus=40 then 1 ELSE 0 end ) as PN_delivered_t_minus_15
                  , SUM(CASE WHEN DATE(MSEC_TO_TIMESTAMP( msg.createdAt + 19800000 )) >= DATE(dim.t_minus_15) AND lifecyclestatus=50 then 1 ELSE 0 end ) as PN_dismissed_t_minus_15
                  , SUM(CASE WHEN DATE(MSEC_TO_TIMESTAMP( msg.createdAt + 19800000 )) >= DATE(dim.t_minus_15) AND lifecyclestatus=60 then 1 ELSE 0 end ) as PN_opened_t_minus_15
                  , SUM(CASE WHEN DATE(MSEC_TO_TIMESTAMP( msg.createdAt + 19800000 )) >= DATE(dim.t_minus_15) AND lifecyclestatus=80 then 1 ELSE 0 end ) as PN_failed_t_minus_15
                  , SUM(CASE WHEN DATE(MSEC_TO_TIMESTAMP( msg.createdAt + 19800000 )) >= DATE(dim.t_minus_15) AND lifecyclestatus=90 then 1 ELSE 0 end ) as PN_complaint_t_minus_15
                  , SUM(CASE WHEN DATE(MSEC_TO_TIMESTAMP( msg.createdAt + 19800000 )) >= DATE(dim.t_minus_15) AND lifecyclestatus=100 then 1 ELSE 0 end ) as PN_bounced_t_minus_15
          FROM [big-query-1233:${v_dataset_name}.engagement_PN_dim] dim
          INNER JOIN [big-query-1233:Atom.message] msg
            ON dim.customerid = msg.userid
          WHERE communicationMedium = 2
            AND DATE(MSEC_TO_TIMESTAMP( createdAt + 19800000 )) >= DATE(dim.t_minus_60)
          GROUP BY customerid, latest_communication_date_PN, latest_communication_date_PN_minus_15_days, latest_communication_date_PN_minus_60_days";

v_destination_tbl="${v_dataset_name}.engagement_PN_active_base";

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

echo `date` "Creating engagement_PN_active_base: $v_task_status";


v_subtask="User Attributes Step 14 (b): engagement_PN_active_base creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 14 (b): engagement_PN_active_base


## Table 14 (c): engagement_behaviour


v_query="SELECT
  a.customerid as customerid,
  b.latest_communication_date_PN AS latest_communication_date_PN,
  b.PN_scheduled AS PN_scheduled,
  b.PN_delivered AS PN_delivered,
  b.PN_dismissed AS PN_dismissed,
  b.PN_opened AS PN_opened,
  b.PN_failed AS PN_failed,
  b.PN_complaint AS PN_complaint,
  b.PN_bounced AS  PN_bounced,
  b.PN_scheduled_t_minus_15 AS PN_scheduled_t_minus_15,
  b.PN_delivered_t_minus_15 AS PN_delivered_t_minus_15,
  b.PN_dismissed_t_minus_15 AS PN_dismissed_t_minus_15,
  b.PN_opened_t_minus_15 AS PN_opened_t_minus_15,
  b.PN_failed_t_minus_15 AS PN_failed_t_minus_15,
  b.PN_complaint_t_minus_15 AS PN_complaint_t_minus_15,
  b.PN_bounced_t_minus_15 AS  PN_bounced_t_minus_15,
  c.email_open as email_open,
  c.email_click as email_click,
  c.email_eventSent AS email_eventSent,
  d.email_open_t_minus_15 as email_open_t_minus_15,
  d.email_click_t_minus_15 as email_click_t_minus_15,
  d.email_eventSent_t_minus_15 AS email_eventSent_t_minus_15,
FROM (SELECT  customerId,
              name,
              gender,
              primaryemailaddress
    FROM [big-query-1233:Atom.customer]
    WHERE isValidated=true
    ) as a
LEFT JOIN ${v_dataset_name}.engagement_PN_active_base b
    ON a.customerid = b.customerid
LEFT JOIN (SELECT uid
              , COUNT(CASE WHEN Event_Type_ID=10 THEN 1 END) AS email_open
              , COUNT(CASE WHEN Event_Type_ID=2 THEN 1 END) AS email_eventSent
              , COUNT(CASE WHEN Event_Type_ID=20 THEN 1 END) AS email_click
            FROM (TABLE_DATE_RANGE([big-query-1233:cheetah.cheetah_], TIMESTAMP(DATE_ADD(TIMESTAMP(CURRENT_DATE()), -61,'DAY')), TIMESTAMP(CURRENT_DATE())))
            GROUP BY 1
            ) as c
on a.primaryemailaddress=c.uid
LEFT JOIN (SELECT uid
              , COUNT(CASE WHEN Event_Type_ID=10 THEN 1 END) AS email_open_t_minus_15
              , COUNT(CASE WHEN Event_Type_ID=2 THEN 1 END) AS email_eventSent_t_minus_15
              , COUNT(CASE WHEN Event_Type_ID=20 THEN 1 END) AS email_click_t_minus_15
            FROM (TABLE_DATE_RANGE([big-query-1233:cheetah.cheetah_], TIMESTAMP(DATE_ADD(TIMESTAMP(CURRENT_DATE()), -16,'DAY')), TIMESTAMP(CURRENT_DATE())))
            GROUP BY 1
            ) as d
on a.primaryemailaddress=d.uid";

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


v_subtask="User Attributes Step 14 (c): engagement_behaviour creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 14(c): engagement_behaviour


## Table 15 (a): dining_preferences
v_query="SELECT customerid
        , a.orderid AS orderid
        , dealid
        , dessert
        , brunch
        , buffet
        , breakfast
        , lunch
        , dinner
FROM (SELECT orderid,
        dealid,
        CASE WHEN upper(offertitle) like ('%DESSERT%') or UPPER(offertitle) like ('%SWEET%') THEN 1 ELSE 0 END AS dessert,
        CASE WHEN UPPER(offertitle) like ('%BRUNCH%')   AND categoryid='FNB' THEN 1 ELSE 0 END AS brunch,
        CASE WHEN UPPER(offertitle) like ('%BUFFET%') THEN 1 ELSE 0 END AS buffet,
        CASE WHEN UPPER(offertitle) like ('%BREAKFAST%')   AND categoryid='FNB' THEN 1 ELSE 0 END AS breakfast,
        CASE WHEN UPPER(offertitle) like ('%LUNCH%')   AND categoryid='FNB' THEN 1 ELSE 0 END AS lunch,
        CASE WHEN UPPER(offertitle) like ('%DINNER%')   AND categoryid='FNB' THEN 1 ELSE 0 END AS dinner
      FROM Atom.order_line
      WHERE ispaid='t'
        AND finalprice>0
      GROUP BY orderid, dealid, dessert, brunch, buffet, breakfast, lunch, dinner
      ) as a
LEFT JOIN(SELECT  orderid,
                  customerid as customerid
          FROM Atom.order_header
          WHERE ispaid='t'
            AND totalprice>0
         ) as b
    ON (a.orderid=b.orderid)";

v_destination_tbl="${v_dataset_name}.dining_preferences";

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

echo `date` "Creating dining_preferences: $v_task_status";


v_subtask="User Attributes Step 15 (a): dining_preferences creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 15 (a): dining_preferences

## Table 15 (b): user_txn_attributes


v_query="Select x.customerid as customerid,
        x.buffet as buffet,
        z.favbuffetdeal as favbuffetdeal,
        x.brunch as brunch,
        p.favbrunchdeal as favbrunchdeal,
        x.desserts as desserts,
        r.favdessertdeal as favdessertdeal,
        breakfast,
        lunch,
        dinner
FROM (SELECT  customerid as customerid,
              if(sum(buffet)>0, 1, 0) as buffet,
              if(sum(breakfast)>0,  1, 0) as breakfast,
              if(sum(lunch)>0, 1, 0) as lunch,
              if(sum(dinner)>0,  1, 0) as dinner,
              if(sum(brunch)>0, 1, 0) as brunch,
              if(sum(dessert)>0, 1, 0) as desserts
      FROM ${v_dataset_name}.dining_preferences
      GROUP BY customerid
      ) x
LEFT JOIN (SELECT customerid,
                  buffet,
                  NTH(1,dealid) as favbuffetdeal
            FROM (SELECT  customerid,
                          buffet,
                          dealid,
                          EXACT_COUNT_DISTINCT(orderid) as txncount
                  FROM ${v_dataset_name}.dining_preferences  
                  WHERE buffet=1
                  GROUP BY customerid, dealid, buffet
                  ORDER BY txncount desc
                  ) buf
            GROUP BY 1,2
            ) as z
    ON (x.customerid=z.customerid)
LEFT JOIN (SELECT customerid,
                  brunch,
                  NTH(1,dealid) as favbrunchdeal
            FROM (SELECT  customerid,
                          brunch,
                          dealid,
                          EXACT_COUNT_DISTINCT(orderid) as txncount
                        FROM ${v_dataset_name}.dining_preferences  
                        WHERE brunch=1
                        GROUP BY customerid, dealid, brunch
                        ORDER BY txncount desc
                        ) brn
            GROUP BY 1,2
            ) as p
ON (x.customerid=p.customerid)
LEFT JOIN (SELECT customerid,
                  dessert,
                  NTH(1,dealid) as favdessertdeal
            FROM (SELECT  customerid,
                          dessert,
                          dealid,
                          EXACT_COUNT_DISTINCT(orderid) as txncount
                  FROM ${v_dataset_name}.dining_preferences  
                  WHERE dessert=1
                  GROUP BY customerid, dealid, dessert
                  ORDER BY txncount desc
                  ) des
            GROUP BY 1,2
            ) as r
ON (x.customerid=r.customerid)";

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


v_subtask="User Attributes Step 15 (b): user_txn_attributes creation";
p_exit_upon_error "$v_task_status" "$v_subtask";

## Completed Table 15 (b): user_txn_attributes



## Table 16: user_attributes_non_GA
v_query="select 
  a.customerid as customerid,
  a.name as name,
  a.gender as gender,
  a.dob_day as dob_day,
  a.dob_month as dob_month,
  a.dob_year as dob_year,
  a.raffleFirstPurchaseDate AS raffleFirstPurchaseDate,
  a.nonRaffleFirstPurchaseDate AS nonRaffleFirstPurchaseDate,
  a.raffleLastPurchaseDate AS raffleLastPurchaseDate,
  a.nonRaffleLastPurchaseDate AS nonRaffleLastPurchaseDate,
  a.totalNonRaffleTxn AS totalNonRaffleTxn, 
  a.totalRaffleTxn AS totalRaffleTxn, 
  a.totalNonRaffleVouchers AS totalNonRaffleVouchers, 
  a.totalRaffleVouchers AS totalRaffleVouchers,
  a.percDiscountAffinty as percDiscountAffinty,
  b.buffet as isBuffet,
  b.favbuffetdeal as favbuffetdeal,
  b.brunch as isBrunch,
  b.favbrunchdeal as favbrunchdeal,
  b.desserts as isDesserts,
  b.favdessertdeal as favdessertdeal,
  b.breakfast as isBreakfast,
  b.lunch as isLunch,
  b.dinner as isDinner,
  c.totalGB as totalGB,
  c.GR as totalGR,
  c.cashback AS cashback,
  c.GB_afterCB as GB_afterFirstCB,
  c.GR_afterCB as GR_afterFirstCB,
  d.weekendPurchase as weekendPurchase,
  d.weekdayPurchase as weekdayPurchase,
  d.unredeemedVouchers as unredeemedVouchers,
  d.redeemtimediff_hours as redeemtimediff_hours,
  e.cancellations AS cancellations,
  e.redeemed as redeemed,
  e.refunds as refunds,
  e.expired AS expired,
  k.validForOneTx as validForOneTx,
  k.validForTwoTx as validForTwoTx,
  k.validForMultipleTx as validForMultipleTx,
  f.latestMerchantTxn as latestTxnMerchant,
  f.latestCatTxn as latestTxnCategory,
  f.latestTxnPricePoint AS latestTxnPricePoint,
  f.secLatestMerchantTxn as secLatestTxnMerchant,
  f.secLatestCatTxn as seclatestTxnCategory,
  f.secLatestTxnPricePoint as secLatestTxnPricePoint,
  f.thirdLatestMerchantTxn as thirdLatestTxnMerchant,
  f.thirdLatestCatTxn as thirdLatestTxnCategory,
  f.thirdLatestTxnPricePoint AS thirdLatestTxnPricePoint,
  g.MostTxnMerchant AS MostTxnMerchant,
  g.secondMostTxnMerchant AS secondMostTxnMerchant,
  g.thirdMostTxnMerchant AS thirdMostTxnMerchant,
  h.mostTxnCat AS mostTxnCat,
  h.mostTxnCatPricePoint AS mostTxnCatPricePoint,
  h.secMostTxnCat AS secMostTxnCat,
  h.secMostTxnPricePoint AS secMostTxnPricePoint,
  h.thirdMostTxnCat AS thirdMostTxnCat,
  h.thirdMostTxnPricePoint AS thirdMostTxnPricePoint,
  i.latestRedeemCity AS latestRedeemCity,
  i.secondLatRedeemCity AS secondLatRedeemCity,
  i.thirdLatRedeemCity AS thirdLatRedeemCity,
  j.latest_communication_date_PN AS latest_communication_date_PN,
  (j.PN_delivered+ j.PN_opened + j.PN_dismissed) as PN_delivered,
  j.PN_opened AS PN_opened, 
  j.PN_dismissed AS j.PN_dismissed,
  j.PN_failed as PN_failed,
  j.PN_complaint AS PN_complaint,
  (j.PN_delivered_t_minus_15 + j.PN_opened_t_minus_15 + j.PN_dismissed_t_minus_15) as PN_delivered_t_minus_15,
  j.PN_opened_t_minus_15 AS PN_opened_t_minus_15,
  j.PN_dismissed_t_minus_15 AS PN_dismissed_t_minus_15,
  j.PN_failed_t_minus_15 as PN_failed_t_minus_15,
  j.PN_complaint_t_minus_15 AS PN_complaint_t_minus_15,
  ( j.email_eventSent + j.email_open+ j.email_click ) as email_sent,
  (j.email_open+ j.email_click) as email_open,
  j.email_click AS email_click,
  ( j.email_eventSent_t_minus_15 + j.email_open_t_minus_15 + j.email_click_t_minus_15 ) as email_sent_t_minus_15,
  (j.email_open_t_minus_15 + j.email_click_t_minus_15) as email_open_t_minus_15,
  j.email_click_t_minus_15 AS email_click_t_minus_15,
  
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


v_query="SELECT  p.dealId AS Deal_ID
        , m.merchantName AS Merchant_Name
        , m.merchantId as Merchant_ID
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
        WHERE isPublished = true
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


## Completed Table 17: user_attributes_merchant_name

v_task_end_time=`date`;


echo "Task started at ${v_task_start_time} and ended at ${v_task_end_time}.";

exit 0;
