# Pipeline options
PIPELINE_CONFIG = {
    "project": "vf-uk-raid-pre-prod",
    "region": "europe-west2",
    "temp_location":  "gs://vf-uk-raid-pre-prod-working/dataflow/temp/",
    "runner": "DataflowRunner",
    "service_account_email": "567858485002-compute@developer.gserviceaccount.com",
    "network": "vf-uk-raid-pre-prod-vpc-1",
    "subnetwork": "regions/europe-west2/subnetworks/vf-uk-raid-pre-prod-app-subnet"
}

my_project = "vf-uk-raid-pre-prod"
my_dataset = "VPDB_DAT"


SOURCE_NAME = "PGW"

PIPELINE_LOGGER_NAME = "pipeline_" + SOURCE_NAME


bq_kms_key_name = (
    "projects/vf-uk-raid-prd-kms/"
    "locations/europe-west2/"
    "keyRings/vf-uk-raid-pre-prod-kms-ring/"
    "cryptoKeys/vf-uk-raid-pre-prod-kms-key"
)

