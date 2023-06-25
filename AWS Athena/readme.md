### AWS Crawler and AWS Athena steps invovled 
Steps 1
**Go into AWS Glue Service and Locate Crawler & click on it**

1. Click on Create Crawler
2. Set it up name and description
3. For this setting : 'Is your data already mapped to Glue tables?' Select No
4. Add a data source. Select the S3 bucket in which daily_batch.py result is coming. in our case it would be ( s3://pysparkapi/api_response/response/ ) **Click on Next**
5. **For IAM ROLE SELECT**  ( AWSGlueServiceRole-s3-crawl ) This will allow your crawler to access the s3 bucket. Click on Next -
6. For Target Database Select " default " & Table name prefix type pysparkapi_ and click on Next - Next

### Once setup is done Run your crawler. 

## Come in AWS Athena Service opne Query Editor

Select Data source as :- AwsDataCatalog
Database as :- default 
in Table your should be able to see a table starting with prefix:- pysparkapi ... Done now you can query it 

use the following code to select and query the table 

```
select * from <Table Name>
```












