#!/bin/bash
if [ -z "$1" ]; then
    echo "Missing s3 bucket name."
    echo "Usage: bash build-deploy.sh WORK_BUCKET SUBNET SECURITY_GROUP GLUE_AZ"
    exit 1
fi
if [ -z "$2" ]; then
    echo "Missing glue subnet"
    echo "Usage: bash build-deploy.sh WORK_BUCKET SUBNET SECURITY_GROUP GLUE_AZ"
    exit 1
fi
if [ -z "$3" ]; then
    echo "Missing glue security group"
    echo "Usage: bash build-deploy.sh WORK_BUCKET SUBNET SECURITY_GROUP GLUE_AZ"
    exit 1
fi
if [ -z "$4" ]; then
    echo "Missing glue availability zone"
    echo "Usage: bash build-deploy.sh WORK_BUCKET SUBNET SECURITY_GROUP GLUE_AZ"
    exit 1
fi

cd etl-jobs
python3 setup.py bdist_wheel
rm -rf build etl_modules*
cd ..

work_bucket=${1}
subnet=${2}
security_group=${3}
glue_az=${4}
extractor_s3_uri="s3://$work_bucket/etl-jobs/extractor.py"
transformations_s3_uri="s3://$work_bucket/etl-jobs/transformations.py"
load_redshift_s3_uri="s3://$work_bucket/etl-jobs/load_redshift.py"
etl_modules_s3_uri="s3://$work_bucket/etl-jobs/dist/etl_modules-0.0.0-py3-none-any.whl"

aws s3 cp ./etl-jobs/extractor.py "$extractor_s3_uri"
aws s3 cp ./etl-jobs/transformations.py "$transformations_s3_uri"
aws s3 cp ./etl-jobs/load_redshift.py "$load_redshift_s3_uri"
aws s3 cp ./etl-jobs/dist/etl_modules-0.0.0-py3-none-any.whl "$etl_modules_s3_uri"

sam validate
sam build
sam package --template-file template.yaml --s3-bucket "$work_bucket" --output-template-file packaged.yaml
sam deploy --template-file packaged.yaml --stack-name olist-challenge --s3-bucket "$work_bucket" --capabilities CAPABILITY_IAM --parameter-overrides ExtractorJobLocation="$extractor_s3_uri" WorkBucket="$work_bucket" TransformationsJobLocation="$transformations_s3_uri" EtlModulesLocation="$etl_modules_s3_uri" LoadRedshiftLocation="$load_redshift_s3_uri" GlueSubnet="$subnet" GlueSecurityGroup="$security_group" GlueAZ="$glue_az"