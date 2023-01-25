
# This is data masking project

The goal of the project is to provision AWS Infra that would allow to read files from S3 bucket and analyze it with AWS DataBrew.
If sensitive has been found in the file, it would be masked using DataBrew masking recipe.

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
One step function is used for the one-off history ingestion while the other one is being triggered by the object created in the S3 bucket and used for ongoing ingestion
The expected S3 path is <data_source_name>/file_name.parquet
File is expected to be in parquet format however it can be re-adjusted and used for json or csv formats.