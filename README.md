
# Data reconciliation between source DB and S3 bucket in AWS using Athena Federated query and Step Function

The goal of the project is to provision AWS infrastructure that would allow to reconcile data between source RDBMS and S3 bucket where that data has been ingested into by DMS from the source DB.
It would allow to provide the confidence that DMS data movement works as intended. It also provides a good example how athena federated query can be utilized with step function and data lake.

```
$ python -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation


## Logic explained
1. First we run the crawler to make sure that all the latest data ingested into S3 by DMS can be discovered by Athena
2. In the map element we increment through all the prefixes in s3 bucket to run reconciliation for every single object.
3. Given that the number of the columns might differ between source table in RDBMS and ingested s3 entity (we create additional column with dms_ingestion_time), we first query the list of columns for every 
data source we are looking to compare.
4. We build a dynamic query based on the set of columns identified and select all the columns from the source table and RDBMS using the federated data source we created in athena 
and compare the result of the query with the result of the query against the Glue Data Catalog that Crawler created.
5. We expect to have empty result which would confirm that every record in DB table has a corresponding record in Data Catalog.

## Note
Provisioning of DMS and the target S3 bucket is outside of the scope of this project
## Reference
About Athena federated query - https://aws.amazon.com/blogs/big-data/query-any-data-source-with-amazon-athenas-new-federated-query/