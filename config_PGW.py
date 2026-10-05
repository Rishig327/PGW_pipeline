import os, posixpath
from config_common import my_project,my_dataset

# Input schema

HEADER = True


SOURCE_NAME = "PGW"

PIPELINE_LOGGER_NAME = "pipeline_" + SOURCE_NAME

############# codec ##############

codec = True  
codec_file_path = "gs://vf-uk-raid-pre-prod-working/dataflow/PGW/input/"
codec_file_pattern = "pgw*.gz"


codec_matcher = (posixpath.join(codec_file_path.rstrip("/"), codec_file_pattern)
                 if codec_file_path.startswith("gs://")
                 else os.path.join(codec_file_path, codec_file_pattern))

Log_Level = 'DEBUG' #DEBUG,WARNING,INFO,ERROR,CRITICAL



INPUT_FILE_PATH = "gs://vf-uk-raid-pre-prod-working/dataflow/PGW/input/"

INPUT_FILE_PATTERN = "pgw*"



input_matcher = (posixpath.join(INPUT_FILE_PATH.rstrip("/"), INPUT_FILE_PATTERN)
                 if INPUT_FILE_PATH.startswith("gs://")
                 else os.path.join(INPUT_FILE_PATH, INPUT_FILE_PATTERN))

OUTPUT_FILE_PATH =  "gs://vf-uk-raid-pre-prod-working/dataflow/PGW/output/"

PROCESSED_ARCHIVE_PATH = "gs://vf-uk-raid-pre-prod-working/dataflow/PGW/done"

INPUT_FIELDS = ["recordType","varName","varValue","recordProperty"]

OUTPUT_FIELDS =  ["STARTTIME","ENDTIME","MES_EVENT_DURATION","SERVED_MSISDN","APN","CALL_TYPE","EVENT_TYPE","SUBSCRIBER_TYPE","MES_CHARGED_AMOUNT","CHARGING_CHARACTERISTICS","RATING_GROUP","RATTYPE","MES_UPLINK_VOLUME","MES_DOWNLINK_VOLUME","SGSN_ADDRESS","GGSN_ADDRESS","SERVED_IMSI","FILTER_CODE","SESSION_ID","SERVICE_TYPE","FILENAME","ACCESS_POINT_NAME_NI","PROC_DATE","PROC_HOUR","NE_ID","MES_SEQ_NUM","MES_CDR_COUNT","MES_EVENT_COUNT","TRAFF_CAT_900","NETWORK_IN","NETWORK_OUT"]



agg_table ='VP_T_EVENTS_READINESS'


AGG_DIMENSIONS = ["EVT_DATE","DIM_END_DATE","DIM_EVENT_TYPE_ID","DIM_CALL_TYPE_ID","DIM_SUBSCRIBER_TYPE_ID","DIM_SERVICE_TYPE_ID","DIM_NETWORK_IN_ID","DIM_NETWORK_OUT_ID","DIM_FILTER_CODE_ID","DIM_PORTED_ID","DIM_APN","DIM_OWNER","DIM_SUBSLOC","DIM_ENTITY"]


# Measures for aggregation
AGG_MEASURES = {
    "MES_EVENT_COUNT": {"field": "MES_EVENT_COUNT", "agg": "sum"},
    "MES_EVENT_DURATION": {"field": "MES_EVENT_DURATION", "agg": "sum"},
    "MES_DATA_VOLUME": {"field": "MES_DATA_VOLUME", "agg": "sum"},
    "MES_DOWNLINK_VOLUME": {"field": "MES_DOWNLINK_VOLUME", "agg": "sum"},
    "MES_UPLINK_VOLUME": {"field": "MES_UPLINK_VOLUME", "agg": "sum"},
    "MES_CHARGED_AMOUNT":{"field": "MES_CHARGED_AMOUNT", "agg": "sum"},
    "MES_CDR_COUNT": {"field": "MES_CDR_COUNT", "agg": "sum"},
    "FIELD1":{"field": "FIELD1", "agg": "sum"},
    "FIELD2":{"field": "FIELD2", "agg": "sum"},
    "FIELD3":{"field": "FIELD3", "agg": "sum"},
    "FIELD4":{"field": "FIELD4", "agg": "sum"},
    "FIELD5":{"field": "FIELD5", "agg": "sum"},
    "FIELD6":{"field": "FIELD6", "agg": "sum"},
    "FIELD7":{"field": "FIELD7", "agg": "sum"},
    "FIELD8":{"field": "FIELD8", "agg": "sum"}
    #"min_duration": {"field": "duration", "agg": "min"},
    #"max_duration": {"field": "duration", "agg": "max"},
    #"avg_duration": {"field": "duration", "agg": "avg"}
}



agg_stg_schema = """
DIM_STARTDATE:STRING,
DIM_EVENT_TYPE_ID:STRING,
DIM_CALL_TYPE_ID:STRING,
DIM_NETWORK_OUT_ID:STRING,
DIM_SUBSCRIBER_TYPE_ID:STRING,
DIM_SERVICE_TYPE_ID:STRING,
Record_Count:INT64
"""

def build_merge_sql(
    my_project,
    my_dataset,
    agg_table,
    agg_dimensions,
    agg_measures,
    source_name
):
    """
    Build BigQuery MERGE SQL dynamically.

    AGG_MEASURES example:

    {
        "EVENT_COUNT": {
            "field": "MES_EVENT_COUNT",
            "agg": "sum"
        },
        "MIN_DURATION": {
            "field": "MES_EVENT_DURATION",
            "agg": "min"
        },
        "MAX_DURATION": {
            "field": "MES_EVENT_DURATION",
            "agg": "max"
        }
    }

    Output column names are taken from the dictionary keys.
    """

    agg_func_map = {
        "sum": "SUM",
        "min": "MIN",
        "max": "MAX",
        "count": "COUNT"
    }

    # ------------------------------------------------------------------
    # SELECT
    # ------------------------------------------------------------------
    select_dims = ",\n    ".join(agg_dimensions)

    select_measures = []

    for out_col, cfg in agg_measures.items():
        field = cfg["field"]
        agg_type = cfg["agg"].lower()

        if agg_type == "avg":
            raise ValueError(
                f"AVG aggregation not supported for MERGE "
                f"(column={out_col}). "
                f"Store SUM and COUNT separately."
            )

        if agg_type not in agg_func_map:
            raise ValueError(
                f"Unsupported aggregation '{agg_type}' "
                f"for column '{out_col}'"
            )

        select_measures.append(
            f"{agg_func_map[agg_type]}({field}) AS {out_col}"
        )

    select_measures_sql = ",\n    ".join(select_measures)

    # ------------------------------------------------------------------
    # GROUP BY
    # ------------------------------------------------------------------
    group_by_sql = ",\n    ".join(agg_dimensions)

    # ------------------------------------------------------------------
    # ON
    # ------------------------------------------------------------------
    on_sql = " AND\n  ".join(
        f"T.{col} = S.{col}"
        for col in agg_dimensions
    )

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------
    update_clauses = []

    for out_col, cfg in agg_measures.items():

        agg_type = cfg["agg"].lower()

        if agg_type in ("sum", "count"):

            update_clauses.append(
                f"T.{out_col} = IFNULL(T.{out_col},0) + IFNULL(S.{out_col},0)"
            )

        elif agg_type == "min":

            update_clauses.append(
                f"T.{out_col} = LEAST(IFNULL(T.{out_col}, S.{out_col}), S.{out_col})"
            )

        elif agg_type == "max":

            update_clauses.append(
                f"T.{out_col} = GREATEST(IFNULL(T.{out_col}, S.{out_col}), S.{out_col})"
            )

    update_sql = ",\n  ".join(update_clauses)

    # ------------------------------------------------------------------
    # INSERT
    # ------------------------------------------------------------------
    insert_cols = agg_dimensions + list(agg_measures.keys())

    insert_columns_sql = ",\n  ".join(insert_cols)

    insert_values_sql = ",\n  ".join(
        f"S.{col}"
        for col in insert_cols
    )

    # ------------------------------------------------------------------
    # Final SQL
    # ------------------------------------------------------------------
    merge_sql = f"""
MERGE `{my_project}.{my_dataset}.{agg_table}` T
USING (
  SELECT
    {select_dims},
    {select_measures_sql}
  FROM `{my_project}.{my_dataset}.TMP_{agg_table}_{source_name}_STG`
  GROUP BY
    {group_by_sql}
) S
ON
  {on_sql}

WHEN MATCHED THEN
UPDATE SET
  {update_sql}

WHEN NOT MATCHED THEN
INSERT (
  {insert_columns_sql}
)
VALUES (
  {insert_values_sql}
)
"""

    return merge_sql



merge_agg_sql = build_merge_sql(
    my_project=my_project,
    my_dataset=my_dataset,
    agg_table="VP_T_EVENTS_READINESS",
    agg_dimensions=AGG_DIMENSIONS,
    agg_measures=AGG_MEASURES,
    source_name=SOURCE_NAME
)

