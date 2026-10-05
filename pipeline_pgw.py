# pipeline_PGW.py
# write multiple file in multiple file and parquet logic added

import os
import uuid
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio
from apache_beam.io.fileio import WriteToFiles, TextSink
from apache_beam.io.filesystems import FileSystems
from config_PGW import merge_agg_sql, agg_table, agg_stg_schema
from google.cloud import bigquery
from datetime import datetime, timezone
from aggregate_funcation import build_key,build_key_with_filename, TelecomAggregateFn, format_agg_output, format_agg_output_for_bq_fn

import pyarrow as pa
import logging

# ---------------- CONFIG IMPORTS for codec ----------------
from config_PGW import codec,codec_matcher
from config_PGW import AGG_DIMENSIONS, AGG_MEASURES

# ---------------- CONFIG IMPORTS ----------------
from config_PGW import  Log_Level, PIPELINE_LOGGER_NAME,SOURCE_NAME 
from config_PGW import  INPUT_FIELDS, OUTPUT_FIELDS,INPUT_FILE_PATH, input_matcher, OUTPUT_FILE_PATH,PROCESSED_ARCHIVE_PATH

from config_common import my_project,my_dataset,bq_kms_key_name, PIPELINE_CONFIG


from Extract_Compressed_Files import ExtractCompressedFilesToPath


from beam_ber_asn_decoder import   AsnDecodeDoFn   
from transformation_PGW import transform_record   


# =========================================================
# LOGGING SETUP (Safe for Beam)
# =========================================================
def setup_logging(level_str: str):
    level = getattr(logging, (level_str or "INFO").upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s [%(filename)s:%(lineno)d]: %(message)s"
    )

    # Keep Beam internal logging quieter (prevents noisy internal debug traces)
    logging.getLogger("apache_beam").setLevel(logging.INFO)

    logger = logging.getLogger(PIPELINE_LOGGER_NAME)
    logger.setLevel(level)
    logger.info("Logging initialized. level=%s", logging.getLevelName(level))
    return logger


logger = logging.getLogger(PIPELINE_LOGGER_NAME)






# =========================================================
# LOGGING HELPERS FOR PIPELINE (DO NOT LOG EVERY ELEMENT!)
# =========================================================
def log_file_metadata(m):

    ####FileMetadata(C:\Users\...\IDD1_STLVGB0D1_TMC720430.ZIP, 192380, 1773393857.552869)

    logger.info("Matched input file: %s", m.path)
    #print(m , m.path)
    return m


class LogFirstN(beam.DoFn):
    """
    Logs first N elements of a PCollection for debugging.
    Avoid large N in production.
    """
    def __init__(self, stage_name, n=2, level="DEBUG"):
        self.stage_name = stage_name
        self.n = n
        self.level = level.upper()
        self._count = 0
        
    
    def setup(self):
        setup_logging(Log_Level)
        self.logger = logging.getLogger(PIPELINE_LOGGER_NAME)


    def process(self, element):
        if self._count < self.n:
            if self.level == "INFO":
                logger.info("[%s] sample=%s", self.stage_name, element)
            else:
                logger.debug("[%s] sample=%s", self.stage_name, element)
            self._count += 1
        yield element




def log_per_file(stage_name):
    
    def _log(kv):
        filename, cnt = kv
        logger.info("[%s] filename=%s count=%s", stage_name, filename, cnt)
        return kv
    return _log




def normalize_filename(path: str) -> str:
    # Works for local paths + GCS paths
    # e.g. gs://bucket/dir/a.zip -> a.zip
    # e.g. C:\dir\a.zip -> a.zip
    return os.path.basename(path) if path else ""



def epoch_to_local_time(epoch_seconds):
    """
    Convert epoch (seconds) to local system time
    where the code is running.
    """
    if not epoch_seconds:
        return ""
    try:
        return datetime.fromtimestamp(float(epoch_seconds)).isoformat(sep=" ", timespec="seconds")
    except Exception:
        return ""

def build_inner_record(kv):
    inner_name, grp = kv

    # grp["main"] can have 1 element (expected)
    main_rec = grp["main"][0] if grp["main"] else {"main_filename": None, "inner_name": inner_name}

    decoded = grp["decoded"][0] if grp["decoded"] else 0
    transformed = grp["transformed"][0] if grp["transformed"] else 0

    return {
        "main_filename": main_rec["main_filename"],
        "inner_name": inner_name,
        "decoded_count": decoded,
        "transformed_count": transformed,
    }


def parse_filename_seqnum(file_name: str):
    """
    TAP format: CDT + HOME(4) + ROAMING(5) + SEQ(5)
    Example: CDTUNORROMMF20974
    """
    file_name = file_name.strip()

    seq_group_value = file_name[7:12]   # For TAP its Roaming Partner and for Switch its Network element or others it may be Empty   
    seq_num = file_name[12:17]           # positions 13–17
    proc_date = datetime.strptime(file_name[0:8], "%Y%m%d").date()         #position date if not available then current date file_name = "20260331_MSC_IN_001.dat"

    if proc_date =='':
        proc_date = datetime.now(timezone.utc).date()



    return {
        "proc_date": proc_date,
        "seq_group_value": seq_group_value,
        "seq_num": seq_num
    }



def make_final_meta(run_id, source_name):
    def _final_meta(kv):
        inner_filename, grp = kv

        # In your current join, meta is under "main"
        meta = (grp.get("main") or [{}])
        meta0 = meta[0] if meta else {}

        decoded_cnt = (grp.get("decoded") or [0])
        transformed_cnt = (grp.get("transformed") or [0])

        
        # Ingestion timestamp (UTC, RFC3339 – BigQuery friendly)
        load_ts = datetime.now(timezone.utc)

        filename_seqnum = parse_filename_seqnum(inner_filename)

        out = {
            "inner_name": inner_filename,
            "size": meta0.get("size"),
            "timestamp": meta0.get("timestamp"),
            "decoded_count": decoded_cnt[0] if decoded_cnt else 0,
            "transformed_count": transformed_cnt[0] if transformed_cnt else 0,
            "run_id": run_id,
            "source_name": source_name,
            "load_ts": load_ts,
            **filename_seqnum
        }
        yield out

    return _final_meta


def format_output(row):
    values = []
    for field in OUTPUT_FIELDS:
        value = row.get(field, "")
        if value is None:
            value = ""
        values.append(str(value))
    return ",".join(values)

def format_output_all(row):
    values = []
    for field in row:
        value = row.get(field, "")
        if value is None:
            value = ""
        values.append(str(value))
    return ",".join(values)


class DetailTextSink(TextSink):
    def __init__(self, output_all=False):
        super().__init__()
        self.output_all = output_all



    def open(self, fh):
        self._fh = fh

        if not self.output_all:
            header = ",".join(OUTPUT_FIELDS) + "\n"
            self._fh.write(header.encode("utf-8"))


    def write(self, row):
        if self.output_all:
            formatted = format_output_all(row)
        else:
            formatted = format_output(row)

        super().write(formatted)

from google.cloud import bigquery

def ensure_stg_table_exists(
    my_project: str,
    my_dataset: str,
    agg_table: str,
    my_job_config: str,
    source_name: str = SOURCE_NAME
    
):
        
    client = bigquery.Client(project=my_project)

    create_stg_sql = f"""
    CREATE TABLE IF NOT EXISTS
    `{my_project}.{my_dataset}.TMP_{agg_table}_{source_name}_STG`
    LIKE `{my_project}.{my_dataset}.{agg_table}`
    OPTIONS (
    kms_key_name='{bq_kms_key_name}'
    )
    """

    client.query(create_stg_sql).result()

def merge_agg_stg_into_main_table(
    my_project: str,
    my_dataset: str,
    agg_table: str,
    merge_agg_sql: str,
    my_job_config: str,
    source_name: str = SOURCE_NAME
     
):   
    client = bigquery.Client(project=my_project)
    job = client.query(merge_agg_sql, job_config=my_job_config)
    job.result()

    
    truncate_sql = f"TRUNCATE TABLE `{my_project}.{my_dataset}.TMP_{agg_table}_{source_name}_STG`"
    
    # Then truncate staging
    client.query(truncate_sql, job_config=my_job_config).result()

    





# =========================================================
# MAIN PIPELINE
# =========================================================
def run():
    RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info("RUN_ID=%s", RUN_ID)
    logger.info("Runner=%s", PIPELINE_CONFIG.get("runner"))
    logger.info("TempLocation=%s", PIPELINE_CONFIG.get("temp_location"))
    logger.info("InputPattern=%s", input_matcher)
    logger.info("DetailOutputPath=%s", OUTPUT_FILE_PATH)
    logger.info("ProcessedArchivePath=%s (delete source required)", PROCESSED_ARCHIVE_PATH)

    options = PipelineOptions(
    
        flags=[
            f"--default_sdk_harness_log_level={Log_Level.upper()}"
        ],
        runner=PIPELINE_CONFIG["runner"],
        project=PIPELINE_CONFIG["project"],
        region=PIPELINE_CONFIG["region"],
        temp_location=PIPELINE_CONFIG["temp_location"],
        service_account_email= PIPELINE_CONFIG["service_account_email"],
        network= PIPELINE_CONFIG["network"],
        subnetwork= PIPELINE_CONFIG["subnetwork"],
        setup_file="./setup.py",
        save_main_session=True
    )

    job_config = bigquery.QueryJobConfig(destination_encryption_configuration=bigquery.EncryptionConfiguration(kms_key_name=bq_kms_key_name))


    ensure_stg_table_exists(
        my_project=my_project,
        my_dataset=my_dataset,
        agg_table=agg_table,
        my_job_config=job_config,
        source_name=SOURCE_NAME
        )


    with beam.Pipeline(options=options) as p:

        # ================= INPUT FILES -> EXTRACT if zip or tar  =================
        if codec:
            extracted_matched = (
                p
                | "Match_extract_files" >> fileio.MatchFiles(
                    codec_matcher,
                    empty_match_treatment=fileio.EmptyMatchTreatment.ALLOW
                )
                | "Read_extract_matches" >> fileio.ReadMatches()
                | "log_file_extarct" >> beam.ParDo(LogFirstN("log1", n=2))
            )

       
            extracted_archives = (
                extracted_matched
                | "FilterZipTar" >> beam.Filter(lambda rf: rf.metadata.path.lower().endswith((".zip",".tar",".tar.gz",".tgz",".gz",".xz",".bz2")))
                | "ExtractToCodecPath" >> beam.ParDo(
                    ExtractCompressedFilesToPath(
                        input_file_path=INPUT_FILE_PATH,
                        processed_prefix=PROCESSED_ARCHIVE_PATH,   # where archive goes
                        delete_source=True
                ))
                | "BuildExtractedPattern" >> beam.Map(lambda kv: kv[1])
                | "DistinctPatterns" >> beam.Distinct()
            )
        

            # MatchAll consumes a PCollection of patterns and returns FileMetadata
            matched_input = (
                extracted_archives
                | "MatchExtractedFiles" >> fileio.MatchAll(
                    empty_match_treatment=fileio.EmptyMatchTreatment.ALLOW
                )
                | "LogMatchedExtracted" >> beam.Map(log_file_metadata)
                
            )

        else:
           
        # ================= INPUT FILES Matcher =================
            matched_input = (
                p
                | "Match_Input_files" >> fileio.MatchFiles(
                    input_matcher,
                    empty_match_treatment=fileio.EmptyMatchTreatment.ALLOW
                )
                | "LogMatchedFiles" >> beam.Map(log_file_metadata)
            )  ####FileMetadata(C:\Users\...\IDD1_STLVGB0D1_TMC720430.ZIP, 192380, 1773393857.552869)


        matched_read = (
            matched_input
            | "Read_Input_matches" >> fileio.ReadMatches()
            | "log_matched_files" >> beam.ParDo(LogFirstN("log5", n=2))
        )      


        # ================= DECODE (WITH DONE TAG) =================
        decoded = (
            matched_read  
                | beam.ParDo(AsnDecodeDoFn(
                    required_fields=INPUT_FIELDS,
                    archive_prefix=PROCESSED_ARCHIVE_PATH,   # your archive folder
                    move_after_decode=True,
                    format_xml_path = "gs://vf-uk-raid-pre-prod-working/dataflow/formats/format_pgw.xml",
            
                    ne_name="PGW",
                    max_records=None,
                    sample_debug_rows=1
                    ))
                    )
        
        # ========== pcoll_decoded is for logging ==============

        _ = (
            decoded
            | "LogDecoded" >> beam.ParDo(LogFirstN("decoded", n=13))
        )

        # ================= TRANSFORM (MULTI LOOKUP JOIN) =================
        pcoll_transform = (
            decoded
             | "FilterTrafficEventTypeAndBilling" >> beam.Filter(
                    lambda row: True #row.get("Traffic_event_type") in [5] # Filter condition
                     
                )           
            | "Transform" >> beam.ParDo(
                transform_record()
            )
            | "LogSample_Transformed" >> beam.ParDo(LogFirstN("transformed", n=2))
        )

        _ = (
            pcoll_transform
            | "Write_Decoded_Output" >> WriteToFiles(
                path=OUTPUT_FILE_PATH,
                destination=lambda row: row["filename"],
                sink=lambda dest: DetailTextSink(output_all=False),
                file_naming=lambda window, pane, shard_index, total_shards, compression, destination: f"{destination}"
                )
            )
        # ================= AGG OUTPUT IN BQ =================

        pcoll_agg_bq = (
            pcoll_transform
            | "BuildKey_bq" >> beam.Map(build_key(AGG_DIMENSIONS))
            | "Aggregate_bq" >> beam.CombinePerKey(TelecomAggregateFn(AGG_MEASURES))
            | "FormatOutput_bq" >> beam.Map(format_agg_output_for_bq_fn(AGG_DIMENSIONS))
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=f"{my_project}:{my_dataset}.TMP_{agg_table}_{SOURCE_NAME}_STG",
                #schema=agg_stg_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters={'destinationEncryptionConfiguration': {'kmsKeyName': bq_kms_key_name}}
            )
        )     


    # ------------------------------------------------------
    # 2) Run MERGE in one line immediately after pipeline finishes
    # ------------------------------------------------------
    merge_agg_stg_into_main_table(
        my_project=my_project,
        my_dataset=my_dataset,
        agg_table=agg_table,
        merge_agg_sql=merge_agg_sql,
        my_job_config=job_config,
        source_name=SOURCE_NAME
        )


if __name__ == "__main__":
    setup_logging(Log_Level)
    logger.info("module name=%s", __name__)
    logger.info("Starting pipeline execution...")
    run()
    logger.info("Pipeline execution finished.")
