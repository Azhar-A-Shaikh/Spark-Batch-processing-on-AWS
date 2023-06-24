# This main.py File will pull only 1 record from the Forex Exchnage API. 

**Note it will only pull the specified date data from the API**

**IF we dont specifiy the date while submititng. It will pull record of Todays Date.**

## Submit command 

```
# To run for todays date
python3 main.py --config '{"app_id" : "251803cdbb994fe2813635578dacbd0a","s3_out_location":"s3://pysparkapi/api_response/","s3_error_out_location":"s3://pysparkapi/api_response/"}'
```

## To pull record for specific date 

```
# To run for historical date
python3 main.py --run_ts '1999-01-06' --config '{"app_id" : "251803cdbb994fe2813635578dacbd0a","s3_out_location":"s3://pysparkapi/api_response/","s3_error_out_location":"s3://pysparkapi/api_response/"}'
```

**--run_ts is the config required to pull the data of specific date**








