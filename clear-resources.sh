aws s3 rm s3://alh-olist-challenge-processed2 --recursive
aws s3 rm s3://alh-olist-challenge-raw --recursive
aws cloudformation delete-stack --stack-name olist-challenge