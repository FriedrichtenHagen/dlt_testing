from datetime import timedelta
from airflow.decorators import dag

import dlt
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup


# modify the default task arguments - all the tasks created for dlt pipeline will inherit it
# - set e-mail notifications
# - we set retries to 0 and recommend to use `PipelineTasksGroup` retry policies with tenacity library, you can also retry just extract and load steps
# - execution_timeout is set to 20 hours, tasks running longer that that will be terminated

default_task_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'test@test.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'execution_timeout': timedelta(hours=1),
}

# modify the default DAG arguments
# - the schedule below sets the pipeline to `@daily` be run each day after midnight, you can use crontab expression instead
# - start_date - a date from which to generate backfill runs
# - catchup is False which means that the daily runs from `start_date` will not be run, set to True to enable backfill
# - max_active_runs - how many dag runs to perform in parallel. you should always start with 1


@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args
)
def load_facebook_action_insights():
    # set `use_data_folder` to True to store temporary data on the `data` bucket. Use only when it does not fit on the local storage
    tasks = PipelineTasksGroup("facebook_action_insights", use_data_folder=False, wipe_local_data=True)

    from facebook_ads import (
        facebook_ads_source,
        facebook_insights_source,
        DEFAULT_ADCREATIVE_FIELDS,
        ADV_INSIGHTS_FIELDS,
        AdCreative,
        enrich_ad_objects,
    )
    pipeline = dlt.pipeline(
        pipeline_name='facebook_insights_fb_pixel_purchase',
        destination='duckdb',
        dataset_name='facebook_insights_fb_pixel_purchases',
        progress="log"
    )

    number_of_days = 10

    fb_ads_insights_source = facebook_insights_source(
        initial_load_past_days=number_of_days,
        time_increment_days=1,
        attribution_window_days_lag=7,
        fields=ADV_INSIGHTS_FIELDS,
        action_breakdowns=("action_type",),
        action_attribution_windows=('7d_click', '1d_view'),
        batch_size=50,
        # dev_mode=False,
        filtering=[
            {"field": "action_type",
             "operator":"IN",
             "value":["offsite_conversion.fb_pixel_purchase"]}
        ]
    )
    # this may not be necessary since the write disposition seems to be merge by default (and not replace)
    # fb_ads_insights_source.root_key = True

    # info = pipeline.run(fb_ads_insights_source)
    # print(info)



    # # modify the pipeline parameters 
    # pipeline = dlt.pipeline(pipeline_name='pipeline_name',
    #                  dataset_name='dataset_name',
    #                  destination='duckdb',
    #                  full_refresh=False # must be false if we decompose
    #                  )

    # create the source, the "serialize" decompose option will converts dlt resources into Airflow tasks. use "none" to disable it
    tasks.add_run(pipeline, fb_ads_insights_source, decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)


load_facebook_action_insights()