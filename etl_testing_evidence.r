> #TESTING
> conn <- connect_mssql('vm-windows\\SQLEXPRESS','INVDB','RUSER','Welcome@toSQL2022')
> 
> # demonstrate current state of tables
> sqlQuery(conn,"SELECT * FROM WEB_DAILY.INTRADAY_STATS")
 [1] ID               TICKER           LAST_TRADE       CURRENT_IIV      CHANGE           CHANGE_PCT       CURRENCY_CODE    NAV_AT_MKT_CLOSE CREATED_TSTP     UPDATED_TSTP     LATEST_FLAG     
<0 rows> (or 0-length row.names)
> sqlQuery(conn,"SELECT * FROM WEB_DAILY.YIELD")
 [1] ID                       TICKER                   SEC_30DAY_YIELD          DISTRIBUTION_RATE        DISTRIBUTION_RATE_12MNTH SEC_30DAY_UNSUB_YIELD    CURRENCY_CODE            CREATED_TSTP            
 [9] UPDATED_TSTP             LATEST_FLAG             
<0 rows> (or 0-length row.names)
> sqlQuery(conn,"SELECT * FROM WEB_DAILY.PRIOR_CLOSE")
 [1] ID                    TICKER                CLOSING_PRICE         BID_ASK_MIDPOINT      BID_ASK_PREM_DISC     BID_ASK_PREM_DISC_PCT MEDIAN_BID_ASK_SPREAD CURRENCY_CODE         CREATED_TSTP          UPDATED_TSTP         
[11] LATEST_FLAG          
<0 rows> (or 0-length row.names)
> 
> # call function to save data from web into SQL Server
> sync_web_to_db(conn)
[1] "Total number of tables: 3 Number of tables passed table_name validation 3"
[1] "Total number of tables: 3 Number of tables has NOT passed column_name validation 0"
[1] "EXECUTED: INSERT INTO WEB_DAILY.INTRADAY_STATS (TICKER, LAST_TRADE, CURRENT_IIV, CHANGE, CHANGE_PCT,NAV_AT_MKT_CLOSE, CURRENCY_CODE, LATEST_FLAG, CREATED_TSTP) VALUES ('BKLN', 20.7 , 20.81 , 0 , 0 , 20.81 ,'USD','Y',GETDATE());"
[1] "EXECUTED: INSERT INTO WEB_DAILY.YIELD (TICKER, SEC_30DAY_YIELD, DISTRIBUTION_RATE, DISTRIBUTION_RATE_12MNTH, SEC_30DAY_UNSUB_YIELD , CURRENCY_CODE, LATEST_FLAG, CREATED_TSTP) VALUES ('BKLN', 0.0579 , 0.0499 , 0.0368 , 0.0578 ,'USD','Y',GETDATE());"
[1] "EXECUTED: INSERT INTO WEB_DAILY.PRIOR_CLOSE (TICKER, CLOSING_PRICE, BID_ASK_MIDPOINT, BID_ASK_PREM_DISC, BID_ASK_PREM_DISC_PCT , MEDIAN_BID_ASK_SPREAD , CURRENCY_CODE, LATEST_FLAG, CREATED_TSTP) VALUES ('BKLN', 20.7 , 20.73 , -0.08 , -0.0038 , 5e-04 ,'USD','Y',GETDATE());"
Warning message:
In for (i in seq_along(specs)) { :
  closing unused connection 3 (https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker=BKLN)
> 
> # demonstrate updated state of tables
> sqlQuery(conn,"SELECT * FROM WEB_DAILY.INTRADAY_STATS")
  ID TICKER LAST_TRADE CURRENT_IIV CHANGE CHANGE_PCT CURRENCY_CODE NAV_AT_MKT_CLOSE        CREATED_TSTP UPDATED_TSTP LATEST_FLAG
1  1   BKLN       20.7       20.81      0          0           USD            20.81 2022-09-22 13:45:35         <NA>           Y
> sqlQuery(conn,"SELECT * FROM WEB_DAILY.YIELD")
  ID TICKER SEC_30DAY_YIELD DISTRIBUTION_RATE DISTRIBUTION_RATE_12MNTH SEC_30DAY_UNSUB_YIELD CURRENCY_CODE        CREATED_TSTP UPDATED_TSTP LATEST_FLAG
1  1   BKLN           0.058              0.05                    0.037                 0.058           USD 2022-09-22 13:45:46         <NA>           Y
> sqlQuery(conn,"SELECT * FROM WEB_DAILY.PRIOR_CLOSE")
  ID TICKER CLOSING_PRICE BID_ASK_MIDPOINT BID_ASK_PREM_DISC BID_ASK_PREM_DISC_PCT MEDIAN_BID_ASK_SPREAD CURRENCY_CODE        CREATED_TSTP UPDATED_TSTP LATEST_FLAG
1  1   BKLN          20.7            20.73             -0.08                -0.004                 0.001           USD 2022-09-22 13:45:59         <NA>           Y
> 
> # change tables so they are different from web
> sqlQuery(conn,"UPDATE WEB_DAILY.INTRADAY_STATS SET CURRENT_IIV = 0 WHERE LATEST_FLAG = 'Y'")
character(0)
> sqlQuery(conn,"UPDATE WEB_DAILY.YIELD SET DISTRIBUTION_RATE = 0 WHERE LATEST_FLAG = 'Y'")
character(0)
> sqlQuery(conn,"UPDATE WEB_DAILY.PRIOR_CLOSE SET PRIOR_CLOSE = 0 WHERE LATEST_FLAG = 'Y'")
[1] "42S22 207 [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Invalid column name 'PRIOR_CLOSE'."               
[2] "[RODBC] ERROR: Could not SQLExecDirect 'UPDATE WEB_DAILY.PRIOR_CLOSE SET PRIOR_CLOSE = 0 WHERE LATEST_FLAG = 'Y''"
> 
> # call function to save data from web into SQL Server
> sync_web_to_db(conn)
[1] "Total number of tables: 3 Number of tables passed table_name validation 3"
[1] "Total number of tables: 3 Number of tables has NOT passed column_name validation 0"
[1] "EXECUTED: INSERT INTO WEB_DAILY.INTRADAY_STATS (TICKER, LAST_TRADE, CURRENT_IIV, CHANGE, CHANGE_PCT,NAV_AT_MKT_CLOSE, CURRENCY_CODE, LATEST_FLAG, CREATED_TSTP) VALUES ('BKLN', 20.7 , 20.81 , 0 , 0 , 20.81 ,'USD','Y',GETDATE());"
[1] "EXECUTED: UPDATE WEB_DAILY.INTRADAY_STATS SET LATEST_FLAG = 'N', UPDATED_TSTP = GETDATE() WHERE ID = 1"
[1] "EXECUTED: INSERT INTO WEB_DAILY.YIELD (TICKER, SEC_30DAY_YIELD, DISTRIBUTION_RATE, DISTRIBUTION_RATE_12MNTH, SEC_30DAY_UNSUB_YIELD , CURRENCY_CODE, LATEST_FLAG, CREATED_TSTP) VALUES ('BKLN', 0.0579 , 0.0499 , 0.0368 , 0.0578 ,'USD','Y',GETDATE());"
[1] "EXECUTED: UPDATE WEB_DAILY.YIELD SET LATEST_FLAG = 'N', UPDATED_TSTP = GETDATE() WHERE ID = 1"
[1] "EXECUTED: INSERT INTO WEB_DAILY.PRIOR_CLOSE (TICKER, CLOSING_PRICE, BID_ASK_MIDPOINT, BID_ASK_PREM_DISC, BID_ASK_PREM_DISC_PCT , MEDIAN_BID_ASK_SPREAD , CURRENCY_CODE, LATEST_FLAG, CREATED_TSTP) VALUES ('BKLN', 20.7 , 20.73 , -0.08 , -0.0038 , 5e-04 ,'USD','Y',GETDATE());"
[1] "EXECUTED: UPDATE WEB_DAILY.PRIOR_CLOSE SET LATEST_FLAG = 'N', UPDATED_TSTP = GETDATE() WHERE ID = 1"
Warning message:
In for (i in seq_along(specs)) { : closing unused RODBC handle 2
> 
> # demonstrate updated state of tables
> sqlQuery(conn,"SELECT * FROM WEB_DAILY.INTRADAY_STATS")
  ID TICKER LAST_TRADE CURRENT_IIV CHANGE CHANGE_PCT CURRENCY_CODE NAV_AT_MKT_CLOSE        CREATED_TSTP        UPDATED_TSTP LATEST_FLAG
1  1   BKLN       20.7        0.00      0          0           USD            20.81 2022-09-22 13:45:35 2022-09-22 13:46:53           N
2  2   BKLN       20.7       20.81      0          0           USD            20.81 2022-09-22 13:46:53                <NA>           Y
> sqlQuery(conn,"SELECT * FROM WEB_DAILY.YIELD")
  ID TICKER SEC_30DAY_YIELD DISTRIBUTION_RATE DISTRIBUTION_RATE_12MNTH SEC_30DAY_UNSUB_YIELD CURRENCY_CODE        CREATED_TSTP        UPDATED_TSTP LATEST_FLAG
1  1   BKLN           0.058              0.00                    0.037                 0.058           USD 2022-09-22 13:45:46 2022-09-22 13:47:03           N
2  2   BKLN           0.058              0.05                    0.037                 0.058           USD 2022-09-22 13:47:03                <NA>           Y
> sqlQuery(conn,"SELECT * FROM WEB_DAILY.PRIOR_CLOSE")
  ID TICKER CLOSING_PRICE BID_ASK_MIDPOINT BID_ASK_PREM_DISC BID_ASK_PREM_DISC_PCT MEDIAN_BID_ASK_SPREAD CURRENCY_CODE        CREATED_TSTP        UPDATED_TSTP LATEST_FLAG
1  1   BKLN          20.7            20.73             -0.08                -0.004                 0.001           USD 2022-09-22 13:45:59 2022-09-22 13:47:15           N
2  2   BKLN          20.7            20.73             -0.08                -0.004                 0.001           USD 2022-09-22 13:47:15                <NA>           Y