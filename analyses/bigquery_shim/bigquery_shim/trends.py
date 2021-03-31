from .snake_case import convert_snake_case_dict
from google.cloud import bigquery

ArchKey = "environment/build/architecture"
FxVersionKey = "environment/build/version"
Wow64Key = "environment/system/isWow64"
CpuKey = "environment/system/cpu"
GfxAdaptersKey = "environment/system/gfx/adapters"
GfxFeaturesKey = "environment/system/gfx/features"
OSNameKey = "environment/system/os/name"
OSVersionKey = "environment/system/os/version"
OSServicePackMajorKey = "environment/system/os/servicePackMajor"

FORMAT_DS = "%Y-%m-%d"


def fetch_results(
    spark,
    start_date,
    end_date,
    project_id="mozdata",
    dataset_id="tmp",
    table_id="graphics_telemetry_trends_tmp",
):
    query = """
  SELECT client_id,
  creation_date,
  environment.build.architecture,
  environment.build.version as build_version,
  environment.system.is_wow64,
  environment.system.cpu,
  environment.system.gfx.adapters,
  environment.system.gfx.features,
  environment.system.os.name,
  environment.system.os.version as os_version,
  environment.system.os.service_pack_major
  FROM `moz-fx-data-shared-prod.telemetry_stable.main_v4` WHERE
  sample_id = 42 AND
  date(submission_timestamp) >= '{}' AND date(submission_timestamp) <= '{}' AND
  CAST(SPLIT(application.version, '.')[OFFSET(0)] AS INT64) > 53 AND
  MOD(CAST(RAND()*10 AS INT64), 10) <3
  """.format(
        start_date.strftime(FORMAT_DS), end_date.strftime(FORMAT_DS)
    )

    bq = bigquery.Client(project=project_id)
    # We need to explicitly specify destination table since the query result is smaller than 10MB, otherwise we could omit next 4 lines
    table_ref = bq.dataset(dataset_id, project=project_id).table(table_id)
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.write_disposition = "WRITE_TRUNCATE"

    query_job = bq.query(query, job_config=job_config)
    # Wait for query execution
    result = query_job.result()
    return (
        spark.read.format("bigquery")
        .option("project", project_id)
        .option("dataset", query_job.destination.dataset_id)
        .option("table", query_job.destination.table_id)
        .load()
        .rdd.map(to_dataset)
    )


def to_dataset(ping):
    """Hacky conversion to dataset API-style RDD"""
    o = {}
    o["clientId"] = ping.client_id
    o["creationDate"] = ping.creation_date
    o[ArchKey] = ping.architecture
    o[FxVersionKey] = ping.build_version
    o[Wow64Key] = ping.is_wow64
    o[CpuKey] = ping.cpu.asDict() if ping.cpu else None
    o[GfxAdaptersKey] = [x.asDict() for x in ping.adapters]
    o[GfxFeaturesKey] = ping.features.asDict(recursive=True) if ping.features else None
    o[OSNameKey] = ping.name
    # fixme os_version
    o[OSVersionKey] = ping.os_version
    o[OSServicePackMajorKey] = ping.service_pack_major
    return convert_snake_case_dict(o)
