import boto3
import csv
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def bytes_to_mb(bytes_size):
    """Convert bytes to megabytes."""
    return bytes_size / (1024 * 1024)

def list_spice_datasets(quicksight_client, account_id):
    datasets_info = []
    try:
        datasets = quicksight_client.list_data_sets(AwsAccountId=account_id)
        spice_datasets = [ds for ds in datasets['DataSetSummaries'] if ds['ImportMode'] == 'SPICE']

        for dataset in spice_datasets:
            dataset_id = dataset['DataSetId']
            try:
                dataset_info = quicksight_client.describe_data_set(
                    AwsAccountId=account_id,
                    DataSetId=dataset_id
                )
                capacity_bytes = dataset_info['DataSet']['ConsumedSpiceCapacityInBytes']
                capacity_mb = bytes_to_mb(capacity_bytes)
                dataset_name = dataset_info['DataSet']['Name']
                datasets_info.append([dataset_name, dataset_id, f"{capacity_mb:.2f}"])
            except ClientError as e:
                if e.response['Error']['Code'] == 'InvalidParameterValueException':
                    logger.warning(f"Dataset ID: {dataset_id} could not be described through the API. Name: {dataset['Name']}")
                    datasets_info.append([dataset['Name'], dataset_id, "failed to get"])
                else:
                    raise

    except ClientError as e:
        logger.error(f"An error occurred: {e}")
        return None

    return datasets_info

def save_to_csv(datasets_info, filename='quicksight_datasets_info.csv'):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['DatasetName', 'DatasetID', 'Capacity(MB)'])
        writer.writerows(datasets_info)

def main():
    account_id = 'account_id'
    region_name = 'region_name'

    quicksight_client = boto3.client('quicksight', region_name=region_name)
    datasets_info = list_spice_datasets(quicksight_client, account_id)
    if datasets_info is not None:
        save_to_csv(datasets_info)

if __name__ == '__main__':
    main()

