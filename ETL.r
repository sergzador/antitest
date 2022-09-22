#NOTE:
# requires R version 4+
# requires rvest version 1+


#SCRIPT LOGIC:
    # - data from THREE tables are extracted from web page by using XPATH and rvest package
    # - XPATH is validated against expected table and column (field) names and if table name or at least one of the column names does not pass validation, such table is exclduded from syncing into DB
    # - SQL database keeps records of all changes. Latest data has LATEST_FLAG = 'Y'
    # - SQL database, talbes and user and created separately
    # - comparison between data from WEB and SQL Server is executed in R and in case of detected change SQL INSERT and UPDATE queries are generated and executed

# FUNCTIONS (UDF):
    # - val_from_xpath
    # - num_from_val
    # - connect_mssql
    # - replace_na
    # - sync_web_to_db


library(dplyr)
library(rvest)
library(stringr)
library(RODBC)

url <- "https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker=BKLN"

val_from_xpath <- function(url, xpth) {
    # extracts text from web page element based on web page url and xpath
    value <- url %>%
    read_html() %>%
    html_nodes(xpath=xpth) %>%
    html_text2()
    # cleans text by removing line breaks and extra whitespaces
    clean_text <-tolower(str_squish(gsub("[\n\t\r*]","",value)))[1] #[1]in case returns list
    return(clean_text)
      }

num_from_val <-function(text) {
# converts text into number after removing per cent, dollar sign and asterisk
number  <- as.numeric(gsub("[%$]","",text))

#if value has per cent sign, devide by 100
if (grepl('%',text)) {
  number <- number/100
}
return(number)
}
  
connect_mssql<- function(server,database,username,password) {
  #connect to SQL Server DB
conn <- odbcDriverConnect(paste("Driver={ODBC Driver 17 for SQL Server};Server=",server,";database=",database,";UID=",username,";PWD=",password,sep=''))
 # to run a query : sqlQuery(conn,'select * from information_schema.tables')
return(conn)}

replace_na <- function(inp) {
  # this function is used to make NA comparable to strings and avoiding installing extra library like tidyr
  if (is.na(inp)) {
    inp <-'none'}
    return(inp)
  }




sync_web_to_db <-function(db_server_name, db_name,db_username, db_password) {
#create connection to DB
conn <- connect_mssql(db_server_name, db_name, db_username, db_password)

#create dataframe with xpath for every table name
df_table_xpath <- data.frame(table_name=character(), 
                            xpath=character(),   
                            table_name_validation=character())

#populate table xpath dataframe (easier to maintain this way)
df_table_xpath[nrow(df_table_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[1]/h3', 'bkln intraday stats')
df_table_xpath[nrow(df_table_xpath)+1, ] <- c('yield', '//*[@id="overview-details"]/div[3]/h3/text()', 'yield')
df_table_xpath[nrow(df_table_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[4]/h3', 'prior close')

#excluded fund characteristics because it's very unstable and on some days was totally blank
#df_table_xpath[nrow(df_table_xpath)+1, ] <- c('fund characteristics', '//*[@id="overview-details"]/div[6]/h3', 'fund characteristics')


#list all tables
all_tables_lst <-  df_table_xpath[, 'table_name']

#list of tables that passes validation
valid_tables_lst <- c()

#table name validation with saving to the list of tables passing validation 
for (tb in all_tables_lst) {
    xpath <- filter(df_table_xpath,  table_name == tb)['xpath'][1, 1]
    
    table_name_vailid <-filter(df_table_xpath,  table_name == tb)['table_name_validation'][1, 1]
    
    table_name_web <-val_from_xpath(url,  xpath)
    
    if (table_name_web == table_name_vailid) {
        valid_tables_lst <- append(valid_tables_lst,  tb)
    } 
    else {
        print(paste(tb, "have not passed validation"))
    }
}
#summary of table name validation
print(paste('Total number of tables:',length(all_tables_lst),'Number of tables passed table_name validation',length(valid_tables_lst)))

#create dataframe with xpath for every column name, table_name values MUST match with df_table_xpath
df_column_xpath <- data.frame(table_name=character(),  #from df_table_xpath
                             field_name_xpath=character(),   
                             field_name_validation=character(),  # used for validation and as field name
                             field_value_xpath=character(), 
                             stringsAsFactors=FALSE)

#populate table xpath dataframe (easier to maintain this way)
# in production I'd rather sourced this from Docker volume
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[1]/ul/li[1]/text()', 'last trade', '//*[@id="overview-details"]/div[1]/ul/li[1]/span')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[1]/ul/li[2]/span[1]/text()', 'current iiv', '//*[@id="overview-details"]/div[1]/ul/li[2]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[1]/ul/li[3]/text()', 'change', '//*[@id="overview-details"]/div[1]/ul/li[3]/span/text()')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[1]/ul/li[4]/span[1]/text()', '% change', '//*[@id="overview-details"]/div[1]/ul/li[4]/span[2]/text()')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[2]/ul/li/span[1]', 'nav at market close', '//*[@id="overview-details"]/div[2]/ul/li/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('yield', '//*[@id="overview-details"]/div[3]/ul/li[1]/span[1]/text()', 'sec 30 day yield', '//*[@id="overview-details"]/div[3]/ul/li[1]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('yield', '//*[@id="overview-details"]/div[3]/ul/li[2]/span[1]/text()', 'distribution rate', '//*[@id="overview-details"]/div[3]/ul/li[2]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('yield', '//*[@id="overview-details"]/div[3]/ul/li[3]/span[1]/text()', '12 month distribution rate', '//*[@id="overview-details"]/div[3]/ul/li[3]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('yield', '//*[@id="overview-details"]/div[3]/ul/li[4]/span/text()', '30-day sec unsubsidized yield', '//*[@id="overview-details"]/div[3]/ul/li[4]/div/div[1]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[4]/ul/li[1]/span[1]', 'closing price', '//*[@id="overview-details"]/div[4]/ul/li[1]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[4]/ul/li[2]/span[1]/text()', 'bid/ask midpoint', '//*[@id="overview-details"]/div[4]/ul/li[2]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[4]/ul/li[3]/span[1]/text()', 'bid/ask prem/disc', '//*[@id="overview-details"]/div[4]/ul/li[3]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[4]/ul/li[4]/span[1]', 'bid/ask prem/disc', '//*[@id="overview-details"]/div[4]/ul/li[4]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[5]/ul/li/span[1]/text()', 'median bid/ask spread', '//*[@id="overview-details"]/div[5]/ul/li/span[2]')

#excluded fund characteristics because it's very unstable and on some days was totally blank
#df_column_xpath[nrow(df_column_xpath)+1, ] <- c('fund characteristics', '//*[@id="overview-details"]/div[6]/ul[1]/li[1]/span[1]/text()', 'yield to maturity', '//*[@id="overview-details"]/div[6]/ul[1]/li[1]/span[2]')

#df_column_xpath[nrow(df_column_xpath)+1, ] <- c('fund characteristics', '//*[@id="overview-details"]/div[6]/ul[2]/li[1]/span[1]/text()', '3 month libor', '//*[@id="overview-details"]/div[6]/ul[2]/li[1]/span[2]')
#df_column_xpath[nrow(df_column_xpath)+1, ] <- c('fund characteristics', '//*[@id="overview-details"]/div[6]/ul[2]/li[2]/span[1]/text()', 'weighted avg price', '//*[@id="overview-details"]/div[6]/ul[2]/li[2]/span[2]')


#list of tables that has not passed column name validation
invalid_tables_lst <-c()

#if field name extracted from xpath does not match expected field name, table name is added to list that would be excluded from loading into DB
for (i in seq(nrow(df_column_xpath))) {
    expected_field_name <-  df_column_xpath[i,]['field_name_validation'][1,1]
    xpath <- df_column_xpath[i,]['field_name_xpath'][1,1]
    field_name_web <- val_from_xpath(url, xpath) 
    table_name  <-  df_column_xpath[i,]['table_name'][1,1]

    if (expected_field_name != replace_na(field_name_web)) {
        invalid_tables_lst <- append(invalid_tables_lst, table_name)
        print(paste("Table [",table_name, "] has not passed column name validation because [",expected_field_name,"] is not equal to [", field_name_web,"]"))
    }
}

#summary of column name validation
print(paste('Total number of tables:',length(valid_tables_lst),'Number of tables has NOT passed column_name validation',length(unique(invalid_tables_lst))))

#extracing values from website, field_name_validation is used as column name for tables that passed table name and column name validation
##intraday table
if ('intraday' %in% valid_tables_lst & !('intraday' %in% invalid_tables_lst)) {
last_trade=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'intraday' & field_name_validation == 'last trade' )['field_value_xpath'][1, 1] ))
current_iiv=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'intraday' & field_name_validation == 'current iiv' )['field_value_xpath'][1, 1] ))
change=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'intraday' & field_name_validation == 'change' )['field_value_xpath'][1, 1] ))
change_pct=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'intraday' & field_name_validation == '% change' )['field_value_xpath'][1, 1] ))
nav_at_market_close=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'intraday' & field_name_validation == 'nav at market close' )['field_value_xpath'][1, 1] ))

# UPDATE LAST TRADE TABLE IN SQL DB
df_lasttrade_db = sqlQuery(conn,"select * from [WEB_DAILY].[INTRADAY_STATS] WHERE LATEST_FLAG = 'Y'")

last_trade_id_db <- df_lasttrade_db['ID'][1,1]
last_trade_db <-df_lasttrade_db['LAST_TRADE'][1,1]
current_iiv_db <-df_lasttrade_db['CURRENT_IIV'][1,1]
change_db <-df_lasttrade_db['CHANGE'][1,1]
change_pct_db <-df_lasttrade_db['CHANGE_PCT'][1,1]
nav_at_market_close_db <-df_lasttrade_db['NAV_AT_MKT_CLOSE'][1,1]

insert_query <-  paste("INSERT INTO WEB_DAILY.INTRADAY_STATS (TICKER, LAST_TRADE, CURRENT_IIV, CHANGE, CHANGE_PCT,NAV_AT_MKT_CLOSE, CURRENCY_CODE, LATEST_FLAG, CREATED_TSTP) VALUES ('BKLN',",last_trade,",",current_iiv,",",change,",",change_pct,",",nav_at_market_close,",'USD','Y',GETDATE());")

if(dim(df_lasttrade_db)[1] == 0) {
	sqlQuery(conn, insert_query) # if table is empty insert 1st record
	print(paste("EXECUTED:",insert_query))} else if (

 !(last_trade_db == last_trade 
	& current_iiv_db == current_iiv 
	& change_db == change 
	& change_pct_db == change_pct 
	& nav_at_market_close_db == nav_at_market_close))  # if web data does not match db data...

{
 sqlQuery(conn, insert_query) #... INSERT the record
 print(paste("EXECUTED:",insert_query))
 update_query <- paste("UPDATE WEB_DAILY.INTRADAY_STATS SET LATEST_FLAG = 'N', UPDATED_TSTP = GETDATE() WHERE ID =",last_trade_id_db)
 sqlQuery(conn, update_query)
 print(paste("EXECUTED:",update_query))
}
}

##yield table
if ('yield' %in% valid_tables_lst & !('yield' %in% invalid_tables_lst)) {
sec_30_day_yield=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'yield' & field_name_validation == 'sec 30 day yield' )['field_value_xpath'][1, 1] ))
distribution_rate=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'yield' & field_name_validation == 'distribution rate' )['field_value_xpath'][1, 1] ))
distribution_rate_12_month=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'yield' & field_name_validation == '12 month distribution rate' )['field_value_xpath'][1, 1] ))
sec_unsubsidized_yield_30_day=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'yield' & field_name_validation == '30-day sec unsubsidized yield' )['field_value_xpath'][1, 1] ))
         
# UPDATE YIELD TABLE IN SQL DB
df_yield_db = sqlQuery(conn,"select * from [WEB_DAILY].[YIELD] WHERE LATEST_FLAG = 'Y'")

yield_id_db <- df_yield_db['ID'][1,1]
sec_30_day_yield_db <-df_yield_db['SEC_30DAY_YIELD'][1,1]
distribution_rate_db <-df_yield_db['DISTRIBUTION_RATE'][1,1]
distribution_rate_12_month_db <-df_yield_db['DISTRIBUTION_RATE_12MNTH'][1,1]
sec_unsubsidized_yield_30_day_db <-df_yield_db['SEC_30DAY_UNSUB_YIELD'][1,1]

insert_query <-  paste("INSERT INTO WEB_DAILY.YIELD (TICKER, SEC_30DAY_YIELD, DISTRIBUTION_RATE, DISTRIBUTION_RATE_12MNTH, SEC_30DAY_UNSUB_YIELD , CURRENCY_CODE, LATEST_FLAG, CREATED_TSTP) VALUES ('BKLN',",sec_30_day_yield,",",distribution_rate,",",distribution_rate_12_month,",",sec_unsubsidized_yield_30_day,",'USD','Y',GETDATE());")

if(dim(df_yield_db)[1] == 0) {
	sqlQuery(conn, insert_query) # if table is empty insert 1st record
	print(paste("EXECUTED:",insert_query))} else if (

 !(sec_30_day_yield_db == sec_30_day_yield 
	& distribution_rate_db == distribution_rate 
	& distribution_rate_12_month_db == distribution_rate_12_month 
	& sec_unsubsidized_yield_30_day_db == sec_unsubsidized_yield_30_day))  # if web data does not match db data...
{
 sqlQuery(conn, insert_query) #... INSERT the record
 print(paste("EXECUTED:",insert_query))
 update_query <- paste("UPDATE WEB_DAILY.YIELD SET LATEST_FLAG = 'N', UPDATED_TSTP = GETDATE() WHERE ID =",yield_id_db)
 sqlQuery(conn, update_query)
 print(paste("EXECUTED:",update_query))
}
}


##prior close table
if ('prior close' %in% valid_tables_lst & !('prior close' %in% invalid_tables_lst)) {
closing_price=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'prior close' & field_name_validation == 'closing price' )['field_value_xpath'][1, 1] ))
bid_ask_midpoint=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'prior close' & field_name_validation == 'bid/ask midpoint' )['field_value_xpath'][1, 1] ))
bid_ask_prem_disc=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'prior close' & field_name_validation == 'bid/ask prem/disc' )['field_value_xpath'][1, 1] ))
bid_ask_prem_disc_pct=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'prior close' & field_name_validation == 'bid/ask prem/disc' )['field_value_xpath'][2, 1] )) # same field name in source, hence returns two line df
median_bid_ask_spread=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'prior close' & field_name_validation == 'median bid/ask spread' )['field_value_xpath'][1, 1] ))

# UPDATE PRIOR CLOSE TABLE IN SQL DB
df_prior_close_db = sqlQuery(conn,"select * from [WEB_DAILY].[PRIOR_CLOSE] WHERE LATEST_FLAG = 'Y'")

prior_close_id_db <- df_prior_close_db['ID'][1,1]
closing_price_db <-df_prior_close_db['CLOSING_PRICE'][1,1]
bid_ask_midpoint_db <-df_prior_close_db['BID_ASK_MIDPOINT'][1,1]
bid_ask_prem_disc_db <-df_prior_close_db['BID_ASK_PREM_DISC'][1,1]
bid_ask_prem_disc_pct_db <-df_prior_close_db['BID_ASK_PREM_DISC_PCT'][1,1]
median_bid_ask_spread_db <-df_prior_close_db['MEDIAN_BID_ASK_SPREAD'][1,1]


insert_query <-  paste("INSERT INTO WEB_DAILY.PRIOR_CLOSE (TICKER, CLOSING_PRICE, BID_ASK_MIDPOINT, BID_ASK_PREM_DISC, BID_ASK_PREM_DISC_PCT , MEDIAN_BID_ASK_SPREAD , CURRENCY_CODE, LATEST_FLAG, CREATED_TSTP) VALUES ('BKLN',",closing_price,",",bid_ask_midpoint,",",bid_ask_prem_disc,",",bid_ask_prem_disc_pct,",",median_bid_ask_spread,",'USD','Y',GETDATE());")

if(dim(df_prior_close_db)[1] == 0) {
	sqlQuery(conn, insert_query) # if table is empty insert 1st record
	print(paste("EXECUTED:",insert_query))} else if (

 !(closing_price_db == closing_price 
	& bid_ask_midpoint_db == bid_ask_midpoint 
	& bid_ask_prem_disc_db == bid_ask_prem_disc 
	& bid_ask_prem_disc_db == bid_ask_prem_disc 
	& median_bid_ask_spread_db == median_bid_ask_spread))  # if web data does not match db data...
{
 sqlQuery(conn, insert_query) #... INSERT the record
 print(paste("EXECUTED:",insert_query))
 update_query <- paste("UPDATE WEB_DAILY.PRIOR_CLOSE SET LATEST_FLAG = 'N', UPDATED_TSTP = GETDATE() WHERE ID =",prior_close_id_db)
 sqlQuery(conn, update_query)
 print(paste("EXECUTED:",update_query))
}
}
odbcClose(conn)
}

#call this function to extract data from web page, compare it to SQLServer DB and update the DB if required.
sync_web_to_db('vm-windows\\SQLEXPRESS','INVDB','RUSER','Welcome@toSQL2022')