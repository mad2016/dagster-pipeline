from dagster_aws.s3.io_manager import s3_pickle_io_manager
from dagster_aws.s3.resources import s3_resource

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

from . import assets

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)


defs = Definitions(
    assets=load_assets_from_package_module(assets),
    # The AWS resources use boto under the hood, so if you are accessing your private
    # buckets, you will need to provide the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    # environment variables or follow one of the other boto authentication methods.
    # Read about using environment variables and secrets in Dagster:
    #   https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets
    resources={
        # With this I/O manager in place, your job runs will store data passed between assets
        # on S3 in the location s3://<bucket>/dagster/storage/<asset key>.
        "io_manager": s3_pickle_io_manager.configured(
            {
                "s3_bucket": {"env": "S3_DAGSTER_BUCKET"},
                # "s3_access_key": {"env": "ACCESS_KEY"},
                # "s3_secret_key": {"env": "SECRET_KEY"},
            }
        ),
        "s3": s3_resource,
    },
    schedules=[daily_refresh_schedule],
)
