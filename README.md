# snow test suite

## Env Set Up
* This code requires spark.
* Create a virtual enviornment.  
* pip install -r requirements.txt
* following enviornment variables need to be set either in session or in ~/.bash_profile  
export sfUrl='https://<account>>.snowflakecomputing.com' 
export SFUSER= '<SF USER>'    
export SFPASSWORD= '<PWD>'    
export SFDATABASE= '<DB NAME>'    
export SFSCHEMA= '<schema>'    
export SFWAREHOUSE='<warehouse>'    
export SFROLE='<Role>'    

## Test Data Set up
* Use the queries.xlsx as a template and set up your test data  
* PARAM is a keyword in the excel  
* You can pass as many parameters. If you need more than 3 parameters add columns param4, param5 and call them $PARAM4  
 $PARAM5 in your query  
* This table needs to be created in the same database and schema  
create or replace table python_runs(
    RUN_NAME STRING,
    QUERY_ID STRING,
    QUERY_NAME STRING,
	PYTHON_TIME STRING
);

## Logic

* The program takes 4 parameters:  
    file_name - This is the xlsx file that has your query and parameters  
    run_name - Unique run name to identify your run to find in history later  
    query_id_list = comma separated string of qids that you would like to test  
    total_limit = This is the parameter that will decide how many concurrent runs will be fired.  
    example: if you give query_id_list as 1,2,3,4 and total_limit as 20 each of the query will be created 5 times with random parameters 
    and all of them will be fired at the same time. The total limit should be more than the number of qids
    run_for - This is the total time you want to load the system. If the input is 120 the previous queries will keep 
    running in loop till 120 seconds. This is useful when you want to load the database from multiple systems  

* Snowflake keeps all history in SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY. But they take 5 minutes or more to appear  
Hence we need to use a intermediate table.
After the runs are over we can see all the history by firing this sql  
select 
        a.RUN_NAME,  
        a.QUERY_NAME,  
        b.TOTAL_ELAPSED_TIME,  
        a.PYTHON_TIME,  
        b.EXECUTION_TIME,  
        b.QUERY_ID,  
        b.WAREHOUSE_NAME,  
        b.WAREHOUSE_SIZE,  
        b.WAREHOUSE_TYPE,  
        b.CLUSTER_NUMBER,  
        b.QUERY_TAG,  
        b.EXECUTION_STATUS,  
        b.ERROR_CODE,  
        b.ERROR_MESSAGE,  
        b.START_TIME,  
        b.END_TIME,  
        b.BYTES_SCANNED,  
        b.PERCENTAGE_SCANNED_FROM_CACHE,  
        b.PARTITIONS_SCANNED,  
        b.PARTITIONS_TOTAL,  
        b.COMPILATION_TIME,  
        b.QUEUED_PROVISIONING_TIME,  
        b.QUEUED_REPAIR_TIME,  
        b.QUEUED_OVERLOAD_TIME,  
        b.TRANSACTION_BLOCKED_TIME,  
        b.CREDITS_USED_CLOUD_SERVICES,  
        b.QUERY_LOAD_PERCENT  
        from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY b, python_runs a  
    where a.QUERY_ID = b.QUERY_ID  
    and b.warehouse_name = 'Your_database_name'  
    and b.user_name = 'Your user name'   
    and (a.run_name) like '%Your run names%';  


 
## How to run
from terminal inside the virtual env 
python <your path>/jobs/engine.py  
It will ask for the above parameters  
If you find input to be annoying you can change the main in engine.py file  
and hardcode your file name etc or use sys.argv

