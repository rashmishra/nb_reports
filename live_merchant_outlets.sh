#!/bin/bash


# Script Name: live_merchant_outlets.sh
# Purpose: To generate the Live (Open)/ Closed status of an outlet of a merchant.

# SELECT CURRENT_DATE() AS reporting_date
#       , merchantId AS Merchant_ID
#       , MSEC_TO_TIMESTAMP(createdAt + 19800000) AS Merchant_creation
#       , MSEC_TO_TIMESTAMP(lastUpdateAt + 19800000) AS Merchant_last_update
#       , IF( isDuplicate OR COALESCE(isDeListed, fALSE) OR COALESCE(isDeleted, FALSE)
#                OR COALESCE(isTemporaryClosed, FALSE) OR COALESCE(isPermanentlyClosed, FALSE)
#              , 'Closed Outlet', 'Live Outlet' ) AS Outlet_live_status
#       , IF( isDuplicate OR COALESCE(isDeListed, fALSE) OR COALESCE(isDeleted, FALSE)
#                OR COALESCE(isTemporaryClosed, FALSE) OR COALESCE(isPermanentlyClosed, FALSE)
#                , FALSE, TRUE) AS Outlet_live
# --       , isDeListed AS is_Merchant_Delisted
# --       , isTemporaryClosed AS is_Merchant_Temporarily_Closed
# --       , isPermanentlyClosed AS is_Merchant_Permanently_Closed
#       , isActive AS is_Merchant_Active
# --       , isDeleted AS is_Merchant_Deleted
#       , status AS Merchant_Status
# --       , coverage
#       , isChain
#       , redemptionAddress.cityTown AS Redemption_Town
#       , businessAddress.cityTown AS Business_Town
# --       , isDuplicate AS is_Merchant_Duplicate
# FROM Atom.merchant


# All Outlets' Open/ Closed status
v_todays_live_outlets="SELECT DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY')) AS reporting_date
      , merchantId AS Merchant_ID
      , MSEC_TO_TIMESTAMP(createdAt + 19800000) AS Merchant_creation
      , MSEC_TO_TIMESTAMP(lastUpdateAt + 19800000) AS Merchant_last_update
      , IF( isDuplicate OR COALESCE(isDeListed, fALSE) OR COALESCE(isDeleted, FALSE)
               OR COALESCE(isTemporaryClosed, FALSE) OR COALESCE(isPermanentlyClosed, FALSE)
             , 'Closed Outlet', 'Live Outlet' ) AS Outlet_live_status
      , IF( isDuplicate OR COALESCE(isDeListed, fALSE) OR COALESCE(isDeleted, FALSE)
               OR COALESCE(isTemporaryClosed, FALSE) OR COALESCE(isPermanentlyClosed, FALSE)
               , FALSE, TRUE) AS Outlet_live
      , isActive AS is_Merchant_Active
      , status AS Merchant_Status
      , isChain
      , redemptionAddress.cityTown AS Redemption_Town
      , businessAddress.cityTown AS Business_Town
FROM Atom.merchant
WHERE isPublished = TRUE";



bq query --replace --allow_large_results -n 1 --destination_table "temp.nb_reports_merchant_outlets_live_today" "$v_todays_live_outlets" &
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date

v_prior_live_outlets="SELECT * FROM nb_reports.merchant_outlets_live_status WHERE DATE(reporting_date) <> DATE(CURRENT_DATE()) ";

bq query --replace --allow_large_results -n 1 --destination_table "temp.nb_reports_merchant_outlets_live_prior" "$v_prior_live_outlets" &
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date


bq query --append  --allow_large_results -n 1 --destination_table "temp.nb_reports_merchant_outlets_live_prior" "SELECT * FROM temp.nb_reports_merchant_outlets_live_today" &
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date

bq cp -f  temp.nb_reports_merchant_outlets_live_prior nb_reports.merchant_outlets_live_status &
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date

bq rm  -f temp.nb_reports_merchant_outlets_live_prior
bq rm  -f temp.nb_reports_merchant_outlets_live_today



# Live Deals information
v_todays_live_deals="SELECT DATE(DATE_ADD(CURRENT_DATE(), -1, 'DAY')) as date,
 _id as Deal_ID, 
      merchant._id AS Merchant_ID, 
    CAST(MAX(CAST(offers.isActive AS INTEGER)) AS BOOLEAN) as Is_Deal_Active
 FROM FLATTEN(FLATTEN(FLATTEN(FLATTEN(FLATTEN(FLATTEN([big-query-1233:Atom.nile] , merchant.skus), offers)
                  , offers.offerValidity.redeemDates)
                  , offers.skus._id)
                  , mappedCategories.showOnHomePage)
                  , offers.calender.remainingQuantity)
                  where startDate < (now()/1000-24*60*60*1000 + 19800000) and  endDate > (now()/1000-24*60*60*1000 + 19800000)
                    AND offers.isActive = TRUE and isActive = TRUE
                    AND (offers.calender.remainingQuantity IS NULL OR  offers.calender.remainingQuantity > 0)
GROUP BY Deal_ID, Merchant_ID, date";


bq query --replace --allow_large_results -n 1 --destination_table "temp.todays_live_deals" "$v_todays_live_deals" &
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code for temp.todays_live_deals" ;
else echo "Code failed in one or more table loads" ;
fi

date


# Previous day's outlets with Deal IDs
v_prior_outlet_with_deals="SELECT * FROM nb_reports.outlets_open_with_deals WHERE DATE(reporting_date) <> DATE(CURRENT_DATE())";

bq query --replace  --allow_large_results -n 1 --destination_table "temp.nb_reports_prior_outlets_open_with_deals" "${v_prior_outlet_with_deals}" &
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date


# Today's outlets with Deal IDs
v_todays_outlet_with_deals="SELECT mer.reporting_date AS reporting_date, mer.Merchant_ID AS Merchant_ID, Outlet_live_status, is_Merchant_Active
       , live.Deal_ID as Deal_ID
       , live.Is_Deal_Active AS Is_Deal_Active
       , mer.isChain AS isChain
       , Merchant_Status, Redemption_Town, Business_Town
FROM temp.todays_live_deals live
INNER JOIN [nb_reports.merchant_outlets_live_status] mer
 ON live.Merchant_ID = mer.Merchant_ID
AND live.date = mer.reporting_date
GROUP BY reporting_date, Merchant_ID, Outlet_live_status
         , is_Merchant_Active, Deal_ID, Is_Deal_Active, isChain
         , Merchant_Status, Redemption_Town, Business_Town";

bq query --append  --allow_large_results -n 1 --destination_table "temp.nb_reports_prior_outlets_open_with_deals" "${v_todays_outlet_with_deals}" &
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date


bq cp -f temp.nb_reports_prior_outlets_open_with_deals nb_reports.outlets_open_with_deals;
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date


bq rm -f temp.nb_reports_prior_outlets_open_with_deals;
bq rm -f temp.todays_live_deals;


echo "Completed generating Merchant Outlet's live status at " `date`;




if wait $v_can_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Completed generating Merchant Outlet's live status with status: $v_table_status `date` on `hostname -I`" | mail -s "Live Merchant Outlets: $v_table_status" rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com;


exit 0