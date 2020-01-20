import requests
import sys
import os

def run(dest_bucket):
    datasets = [
        'olist_customers_dataset.csv', 
        'olist_geolocation_dataset.csv', 
        'olist_order_items_dataset.csv', 
        'olist_order_reviews_dataset.csv',
        'olist_orders_dataset.csv',
        'olist_products_dataset.csv',
        'olist_sellers_dataset.csv'
    ]

    print('Downloading files...')

    urls = [f'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/{x}' for x in datasets]

    for i in range(len(urls)):
        filename = datasets[i]
        print(f'Downloading {filename}')
        output_path = os.path.join(sys.path[0], filename)
        response = requests.get(urls[i])
        open(output_path, 'wb').write(response.content)

    print('Uploading files to S3...')

    for ds in datasets:
        print(f'Uploading {ds}')
        path = os.path.join(sys.path[0], ds)
        os.system(f'aws s3 cp {path} s3://{dest_bucket}/{ds.rsplit("_", 1)[0]}/{ds}')

    # s3 trigger
    filename = 'SUCCESS_'
    path = os.path.join(sys.path[0], filename)
    open(path, 'w').write('OK') # aws-cli issue #2403
    os.system(f'aws s3 cp {path} s3://{dest_bucket}/{filename}')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Missing destination bucket.\nUsage: python3 app.py DESTINATION_BUCKET')
        exit()
    run(sys.argv[1])
