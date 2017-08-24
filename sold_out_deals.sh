v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi
v_dataset_name=nb_reports;
date


# sold_out table loading. Replace existing
v_sold_out="SELECT inActiveDealId,inactive_deal_city,merchantID,activeDealId,active_deal_city,activeLocality,flag
FROM 
(SELECT inActiveDealId, inActiveDealCat inactive_deal_city, merchantID, activeDealId
       , activeDealCat active_deal_city, activePermalink activeLocality, flag
FROM 
(SELECT fq_tbl989.inActiveDealId AS inActiveDealId, fq_tbl989.inActiveDealCat AS inActiveDealCat
       , fq_tbl987.cityName as inactive_deal_city, fq_tbl989.merchantId as merchantID
       , fq_tbl989.activeDealId AS activeDealId, fq_tbl989.activeDealCat AS activeDealCat
       , fq_tbl987.cityName as active_deal_city, fq_tbl989.activePermalink AS activePermalink
       , fq_tbl989.activeLocality AS activeLocality, fq_tbl989.flag AS flag 
    FROM ( -- fq_tbl989
           SELECT fq_tbl100.inActiveDealId AS inActiveDealId, fq_tbl100.inActiveDealCat AS inActiveDealCat
                     , fq_tbl100.inactive_deal_city AS inactive_deal_city, fq_tbl100.merchantId AS merchantId
                     , fq_tbl100.activeDealId as activeDealId
                     , fq_tbl100.activeDealCat AS activeDealCat, fq_tbl100.active_deal_city AS active_deal_city
                     , fq_tbl100.activePermalink AS activePermalink, fq_tbl100.activeLocality AS activeLocality
                     , fq_tbl101.flag AS flag 
            FROM (-- fq_tbl100
                    SELECT inActiveDealId, inActiveDealCat, fq_tbl1.city as inactive_deal_city, fq_tbl1.merchantId as merchantId
                            , activeDealId, activeDealCat, fq_tbl2.city as active_deal_city
                            , fq_tbl1.activePermalink as activePermalink, locality as activeLocality  
                    FROM  (SELECT _id as inActiveDealId
                                    , c.merchantId
                                    , CASE WHEN c.redemptionAddress.cityTown ='' 
                                         THEN NULL ELSE c.redemptionAddress.cityTown 
                                      END AS city
                                    , categoryId as inActiveDealCat
                            FROM [big-query-1233:Atom.deal] a
                            INNER JOIN (SELECT integer(id) as dealid, integer(mappings.merchant.id) as merchantid  
                                        FROM FLATTEN(Atom.mapping,mappings.merchant.id) WHERE type ='deal' 
                                      ) b ON a._id = b.dealid
                            INNER JOIN Atom.merchant c ON b.merchantid = c.merchantid
                            WHERE (a.isActive IS FALSE or units.dval.dToDt < (now()/1000-24*60*60*1000) )
                              AND state ='live'
                              AND categoryId <> 'GTW'
                            GROUP BY 1,2,3,4
                            ) fq_tbl2
                    LEFT JOIN (SELECT dealID as activeDealId, merchantId, city as city, fq_tbl2.units.seo.permaLink as activePermalink
                                      , categoryId as activeDealCat, fq_tbl1.locality as locality 
                                 FROM (SELECT merchantId,city,locality
                                               ,first_value(activeDealId) over (partition by merchantId,city,locality ORDER BY total_vouchers desc) as dealId 
                                     from
                                        (SELECT _id as activeDealId,c.merchantId as merchantId,d.voucher_count as total_vouchers
                                                , CASE WHEN c.redemptionAddress.cityTown ='' 
                                                      THEN NULL ELSE c.redemptionAddress.cityTown 
                                                  END AS city
                                                , c.redemptionAddress.attribute_2 as locality 
                                        FROM [big-query-1233:Atom.deal] a
                                        INNER JOIN (SELECT integer(id) as dealid, integer(mappings.merchant.id) as merchantid  
                                        from FLATTEN(Atom.mapping,mappings.merchant.id) WHERE type ='deal') b ON a._id = b.dealid
                                        INNER JOIN Atom.merchant c ON b.merchantid = c.merchantid
                                        LEFT JOIN (SELECT integer(dealId) as dealId,count(orderlineID) as voucher_count,
                                        integer(merchantId) as merchantID from Atom.order_line WHERE ispaid='t' 
                                        AND dealId IS NOT NULL
                                        GROUP BY 1,3
                                        ORDER BY 2 DESC) d
                                        on d.dealId=a._id
                                        AND d.merchantId=c.merchantId
                                        WHERE a.isActive IS TRUE 
                                        AND state ='live'
                                        AND units.dval.dToDt > (now()/1000-24*60*60*1000)
                                        AND categoryId <>'GTW'
                                        GROUP BY 1,2,3,4,5
                                        ORDER BY 2 desc
                                        )
                                        ) fq_tbl1
                                INNER JOIN Atom.deal fq_tbl2 ON fq_tbl1.dealId= fq_tbl2._id
                                GROUP BY 1,2,3,4,5,6
                                ORDER BY 2 desc
                                )  fq_tbl1
                            on fq_tbl1.merchantId=fq_tbl2.merchantId
                    WHERE activeDealId IS NOT NULL
                    GROUP BY 1,2,3,4,5,6,7,8,9
                    ORDER BY 1 desc
                    ) fq_tbl100
            LEFT JOIN  (SELECT activeDealId, city, 'multiple' as flag 
                         FROM  (SELECT dealID as activeDealId,merchantId,city as city,fq_tbl2.units.seo.permaLink as activePermalink
                                        , categoryId as activeDealCat,fq_tbl1.locality as locality 
                                 FROM (SELECT merchantId,city,locality
                                                , first_value(activeDealId) over (partition by merchantId,city,locality ORDER BY total_vouchers desc) as dealId 
                                         FROM  (SELECT _id as activeDealId,c.merchantId as merchantId,d.voucher_count as total_vouchers
                                                        , case when c.redemptionAddress.cityTown ='' 
                                                               then null else c.redemptionAddress.cityTown end as city
                                                        , c.redemptionAddress.attribute_2 as locality 
                                                FROM [big-query-1233:Atom.deal] a
                                                INNER JOIN (SELECT integer(id) as dealid, integer(mappings.merchant.id) as merchantid  
                                                            from FLATTEN(Atom.mapping,mappings.merchant.id) WHERE type ='deal'
                                                           ) b ON a._id = b.dealid
                                                INNER JOIN Atom.merchant c ON b.merchantid = c.merchantid
                                                LEFT JOIN (SELECT integer(dealId) as dealId,count(orderlineID) as voucher_count
                                                                  , integer(merchantId) as merchantID 
                                                            from Atom.order_line WHERE ispaid='t' 
                                                            AND dealId IS NOT NULL
                                                            GROUP BY 1,3
                                                            -- ORDER BY 2 desc
                                                            ) d
                                                     ON d.dealId=a._id
                                                    AND d.merchantId=c.merchantId
                                                WHERE a.isActive IS TRUE 
                                                AND state ='live'
                                                AND units.dval.dToDt > (now()/1000-24*60*60*1000)
                                                AND categoryId <>'GTW'
                                                GROUP BY 1,2,3,4,5
                                            )
                                        ) fq_tbl1
                                INNER JOIN Atom.deal fq_tbl2 ON fq_tbl1.dealId= fq_tbl2._id
                                GROUP BY 1,2,3,4,5,6
                                ORDER BY 2 desc
                                ) fq_tbl1334
                                GROUP BY 1,2 
                                having count(*)>1
                        ) fq_tbl101
                 ON fq_tbl101.activeDealId=fq_tbl100.activeDealId
                AND fq_tbl101.city=fq_tbl100.active_deal_city
            ) fq_tbl989 
    INNER JOIN (SELECT  merchantID, cityName 
                FROM  (SELECT merchantID,coverage 
                       from flatten(Atom.merchant,coverage)
                       WHERE coverage IS NOT NULL
                      ) fq_tbl1000 
                INNER JOIN (SELECT redemptionAddress.city._id as cityID ,redemptionAddress.city.name as cityName 
                            FROM [Atom.transformed_merchants] 
                            WHERE redemptionAddress.city._id IS NOT NULL 
                            GROUP BY 1,2
                            ) fq_tbl101 
                ON  fq_tbl101.cityId=fq_tbl1000.coverage
                GROUP BY 1,2
                ) fq_tbl987
          ON fq_tbl989.merchantId=fq_tbl987.merchantId
)
) fq
, 

(SELECT inActiveDealId, inActiveDealCat inactive_deal_city, merchantID, activeDealId
       , activeDealCat active_deal_city, activePermalink activeLocality, flag
FROM 
(SELECT sq_tbl100.inActiveDealId AS inActiveDealId, sq_tbl100.inActiveDealCat AS inActiveDealCat
        , sq_tbl100.inactive_deal_city AS inactive_deal_city, sq_tbl100.merchantId AS merchantId
        , sq_tbl100.activeDealId as activeDealId, sq_tbl100.activeDealCat AS activeDealCat
        , sq_tbl100.active_deal_city AS active_deal_city, sq_tbl100.activePermalink AS activePermalink
        , sq_tbl100.activeLocality AS activeLocality, sq_tbl101.flag AS flag 
 FROM ( -- sq_tbl100
         SELECT inActiveDealId, inActiveDealCat, sq_tbl1.city as inactive_deal_city
              , sq_tbl1.merchantId as merchantId, activeDealId,activeDealCat
              , sq_tbl2.city as active_deal_city, sq_tbl1.activePermalink as activePermalink, locality as activeLocality  
         FROM  (SELECT _id as inActiveDealId, c.merchantId
                        , CASE WHEN c.redemptionAddress.cityTown ='' 
                             THEN NULL 
                          ELSE c.redemptionAddress.cityTown 
                          END AS city
                        , categoryId as inActiveDealCat
                FROM [big-query-1233:Atom.deal] a
                INNER JOIN (SELECT integer(id) as dealid, integer(mappings.merchant.id) as merchantid  
                            FROM FLATTEN(Atom.mapping,mappings.merchant.id) 
                            WHERE type ='deal'
                            ) b on a._id = b.dealid
                INNER JOIN Atom.merchant c on b.merchantid = c.merchantid
                WHERE (a.isActive IS FALSE or units.dval.dToDt < (now()/1000-24*60*60*1000))
                  AND state ='live'
                  AND categoryId <> 'GTW'
                GROUP BY 1,2,3,4
                ) sq_tbl2
        LEFT JOIN  (SELECT dealID as activeDealId, merchantId, city as city, sq_tbl2.units.seo.permaLink as activePermalink
                           , categoryId as activeDealCat, sq_tbl1.locality as locality 
                    FROM
                    (SELECT merchantId,city,locality,first_value(activeDealId) over (partition by merchantId,city,locality ORDER BY total_vouchers desc) as dealId 
                     FROM (SELECT _id as activeDealId, c.merchantId as merchantId, d.voucher_count as total_vouchers
                                   , case when c.redemptionAddress.cityTown ='' 
                                        then null 
                                    else c.redemptionAddress.cityTown end as city
                                  , c.redemptionAddress.attribute_2 as locality 
                            FROM [big-query-1233:Atom.deal] a
                            INNER JOIN (SELECT integer(id) as dealid, integer(mappings.merchant.id) as merchantid  
                                        FROM FLATTEN(Atom.mapping,mappings.merchant.id) where type ='deal'
                                       ) b on a._id = b.dealid
                            INNER JOIN Atom.merchant c on b.merchantid = c.merchantid
                            LEFT JOIN (SELECT integer(dealId) as dealId,count(orderlineID) as voucher_count
                                              , integer(merchantId) as merchantID 
                                       FROM Atom.order_line 
                                       WHERE ispaid='t' 
                                         AND dealId is not null
                                       GROUP BY 1,3
                                       ORDER BY 2 desc
                                       ) d
                                 ON d.dealId=a._id
                                AND d.merchantId=c.merchantId
                            WHERE a.isActive IS TRUE 
                              AND state ='live'
                              AND units.dval.dToDt > (now()/1000-24*60*60*1000)
                              AND categoryId <>'GTW'
                            GROUP BY 1,2,3,4,5
                            ORDER BY 2 desc
                            )
                    ) sq_tbl1
                    INNER JOIN Atom.deal sq_tbl2 on sq_tbl1.dealId= sq_tbl2._id
                    GROUP BY 1,2,3,4,5,6
                    ORDER BY 2 desc
                    )  sq_tbl1
        on sq_tbl1.merchantId=sq_tbl2.merchantId
        where activeDealId is not null
        GROUP BY 1,2,3,4,5,6,7,8,9
        ORDER BY 1 DESC
        ) sq_tbl100
LEFT JOIN  (SELECT activeDealId, city, \"multiple\" as flag 
             FROM (SELECT dealID as activeDealId,merchantId,city as city,sq_tbl2.units.seo.permaLink as activePermalink,categoryId as activeDealCat,sq_tbl1.locality as locality 
                    FROM (SELECT merchantId,city,locality,first_value(activeDealId) over (partition by merchantId,city,locality ORDER BY total_vouchers desc) as dealId 
                             FROM  (SELECT _id as activeDealId,c.merchantId as merchantId,d.voucher_count as total_vouchers,case when c.redemptionAddress.cityTown ='' 
                                then null else c.redemptionAddress.cityTown end as city,c.redemptionAddress.attribute_2 as locality 
                                FROM [big-query-1233:Atom.deal] a
                                INNER JOIN (SELECT integer(id) as dealid, integer(mappings.merchant.id) as merchantid  
                                from FLATTEN(Atom.mapping,mappings.merchant.id) where type ='deal') b on a._id = b.dealid
                                INNER JOIN Atom.merchant c on b.merchantid = c.merchantid
                                LEFT JOIN (SELECT integer(dealId) as dealId,count(orderlineID) as voucher_count,
                                integer(merchantId) as merchantID from Atom.order_line where ispaid='t' 
                                AND dealId is not null
                                GROUP BY 1,3
                                ORDER BY 2 desc) d
                                on d.dealId=a._id
                                AND d.merchantId=c.merchantId
                                where a.isActive IS TRUE 
                                AND state ='live'
                                AND units.dval.dToDt > (now()/1000-24*60*60*1000)
                                AND categoryId <>'GTW'
                                GROUP BY 1,2,3,4,5
                                ORDER BY 2 desc
                                )
                        ) sq_tbl1
                    INNER JOIN Atom.deal sq_tbl2 on sq_tbl1.dealId= sq_tbl2._id
                    GROUP BY 1,2,3,4,5,6
                    ORDER BY 2 DESC
                    ) sq_tbl1334
            GROUP BY 1,2 
            HAVING COUNT(*)>1
            ) sq_tbl101
     ON sq_tbl101.activeDealId=sq_tbl100.activeDealId
    AND sq_tbl101.city=sq_tbl100.active_deal_city
)
) sq
GROUP BY 1, 2, 3, 4, 5, 6, 7"
##echo -e "Query: \n $v_query_sold_out table";

tableName=sold_out
v_destination_tbl="$v_dataset_name.${tableName}";
echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1 --destination_table=$v_destination_tbl \"$v_sold_out\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_sold_out" &
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

bq extract nb_reports.sold_out gs://nb_reports/sold_out_deals.csv
gsutil cp gs://nb_reports/sold_out_deals.csv /home/ubuntu/nb_reports
aws s3 mv /home/ubuntu/nb_reports/sold_out_deals.csv  s3://nb-base/redirects/

echo "Sold out deals data file uploaded in s3 :$v_table_status`date`" | mail -s "$v_table_status" rahul.sachan@nearbuy.com 


##mutt -s "Atom Refresh: All Extracts status:  $v_all_extracts_status`date` "  -- sairanganath.v@nearbuy.com rahul.sachan@nearbuy.com rashmi.mishra@nearbuy.com < /dev/null
##mutt  -v "BI Table refresh: CM table status: $v_table_status`date`"  --rashmi.mishra@nearbuy.com < /dev/null

exit 0


