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

v_todays_live_outlets="SELECT CURRENT_DATE() AS reporting_date
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



bq query --replace --allow_large_results --destination_table "temp.nb_reports_merchant_outlets_live_today" "$v_todays_live_outlets" &
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date

v_prior_live_outlets="SELECT * FROM nb_reports.merchant_outlets_live_status WHERE DATE(reporting_date) <> DATE(CURRENT_DATE()) ";

bq query --replace --allow_large_results --destination_table "temp.nb_reports_merchant_outlets_live_prior" "$v_prior_live_outlets" &
v_first_pid=$!
v_can_tbl_pids+=" $v_first_pid"
wait $v_first_pid;


if wait $v_can_tbl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date


bq query --append  --allow_large_results --destination_table "temp.nb_reports_merchant_outlets_live_prior" "SELECT * FROM temp.nb_reports_merchant_outlets_live_today" &
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

echo "Completed generating Merchant Outlet's live status at " `date`;




if wait $v_can_tbl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Completed generating Merchant Outlet's live status with status: $v_table_status `date`" | mail -s "Live Merchant Outlets: $v_table_status" rashmi.mishra@nearbuy.com sairanganath.v@nearbuy.com;


exit 0