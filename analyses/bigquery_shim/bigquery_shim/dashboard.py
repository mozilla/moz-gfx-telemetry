"""For use with graphics-telemetry-dashboard-db.ipynb"""

import collections
import json

from google.cloud import bigquery, storage

from .snake_case import SnakeCaseDict, convert_snake_case_dict

FORMAT_DS = "%Y-%m-%d"


def fetch_results(
    spark,
    start_date,
    end_date,
    channel=None,
    min_firefox_version="53",
    project_id="moz-fx-data-shared-prod",
    dataset_id="analysis",
    table_id="graphics_telemetry_dashboard_tmp",
):
    channel_filter = ""
    if channel is not None:
        channel_filter = "normalized_channel = '{}' AND".format(channel)

    query = """
    WITH sample AS
    (select client_id,
        creation_date,
        additional_properties,
        environment.build.version                                                 as environment__build__version,
        environment.build.build_id                                                as environment__build__build_id,
        environment.system.memory_mb                                              as environment__system__memory_mb,
        environment.system.is_wow64                                               as environment__system__is_wow64,
        environment.system.cpu                                                    as environment__system__cpu,
        environment.system.os.name                                                as environment__system__os__name,
        environment.system.os.version                                             as environment__system__os__version,
        environment.system.os.service_pack_major                                  as environment__system__os__service_pack_major,
        environment.system.gfx.adapters                                           as environment__system__gfx__adapters,
        payload.info.revision                                                     as payload__info__revision,
        environment.system.gfx                                                    as environment__system__gfx,
        environment.system.gfx.monitors                                           as environment__system__gfx__monitors,
        environment.build.architecture                                            as environment__build__architecture,
        environment.system.gfx.features                                           as environment__system__gfx__features,
        payload.histograms.DEVICE_RESET_REASON                                    as payload__histograms__DEVICE_RESET_REASON,
        payload.histograms.GRAPHICS_SANITY_TEST                                   as payload__histograms__GRAPHICS_SANITY_TEST,
        payload.histograms.GRAPHICS_SANITY_TEST_REASON                            as payload__histograms__GRAPHICS_SANITY_TEST_REASON,
        payload.histograms.GRAPHICS_DRIVER_STARTUP_TEST                           as payload__histograms__GRAPHICS_DRIVER_STARTUP_TEST,
        payload.histograms.CANVAS_WEBGL_SUCCESS                                   as payload__histograms__CANVAS_WEBGL_SUCCESS,
        payload.histograms.CANVAS_WEBGL2_SUCCESS                                  as payload__histograms__CANVAS_WEBGL2_SUCCESS,
        payload.histograms.PLUGIN_DRAWING_MODEL                                   as payload__histograms__PLUGIN_DRAWING_MODEL,
        payload.histograms.MEDIA_DECODER_BACKEND_USED                             as payload__histograms__MEDIA_DECODER_BACKEND_USED,
        payload.processes.content.histograms.DEVICE_RESET_REASON                  as payload__processes__content__histograms__DEVICE_RESET_REASON,
        payload.processes.content.histograms.GRAPHICS_SANITY_TEST                 as payload__processes__content__histograms__GRAPHICS_SANITY_TEST,
        payload.processes.content.histograms.GRAPHICS_SANITY_TEST_REASON          as payload__processes__content__histograms__GRAPHICS_SANITY_TEST_REASON,
        payload.processes.content.histograms.GRAPHICS_DRIVER_STARTUP_TEST         as payload__processes__content__histograms__GRAPHICS_DRIVER_STARTUP_TEST,
        payload.processes.content.histograms.CANVAS_WEBGL_SUCCESS                 as payload__processes__content__histograms__CANVAS_WEBGL_SUCCESS,
        payload.processes.content.histograms.CANVAS_WEBGL2_SUCCESS                as payload__processes__content__histograms__CANVAS_WEBGL2_SUCCESS,
        payload.processes.content.histograms.PLUGIN_DRAWING_MODEL                 as payload__processes__content__histograms__PLUGIN_DRAWING_MODEL,
        payload.processes.content.histograms.MEDIA_DECODER_BACKEND_USED           as payload__processes__content__histograms__MEDIA_DECODER_BACKEND_USED,
        payload.keyed_histograms.D3D11_COMPOSITING_FAILURE_ID                     as payload__keyed_histograms__D3D11_COMPOSITING_FAILURE_ID,
        payload.keyed_histograms.OPENGL_COMPOSITING_FAILURE_ID                    as payload__keyed_histograms__OPENGL_COMPOSITING_FAILURE_ID,
        payload.keyed_histograms.CANVAS_WEBGL_ACCL_FAILURE_ID                     as payload__keyed_histograms__CANVAS_WEBGL_ACCL_FAILURE_ID,
        payload.keyed_histograms.CANVAS_WEBGL_FAILURE_ID                          as payload__keyed_histograms__CANVAS_WEBGL_FAILURE_ID,
        payload.processes.content.keyed_histograms.D3D11_COMPOSITING_FAILURE_ID   as payload__processes__content__keyed_histograms__D3D11_COMPOSITING_FAILURE_ID,
        payload.processes.content.keyed_histograms.OPENGL_COMPOSITING_FAILURE_ID  as payload__processes__content__keyed_histograms__OPENGL_COMPOSITING_FAILURE_ID,
        payload.processes.content.keyed_histograms.CANVAS_WEBGL_ACCL_FAILURE_ID   as payload__processes__content__keyed_histograms__CANVAS_WEBGL_ACCL_FAILURE_ID,
        payload.processes.content.keyed_histograms.CANVAS_WEBGL_FAILURE_ID        as payload__processes__content__keyed_histograms__CANVAS_WEBGL_FAILURE_ID
        from `moz-fx-data-shared-prod.telemetry_stable.main_v4` where
        date(submission_timestamp) >= '{start_date}' AND date(submission_timestamp) <= '{end_date}' AND
        normalized_app_name = 'Firefox' AND
        {channel_filter}
        CAST(SPLIT(application.version, '.')[OFFSET(0)] AS INT64) > {min_firefox_version} AND
        -- NOTE: fixed fraction corresponding to 0.0003
        sample_id = 42 AND
        MOD(CAST(RAND()*10 AS INT64), 10) < 3),

    distinct_client_ids AS (SELECT distinct(client_id) FROM sample),

    -- Retain only the first seen documents for each client ID ,
    base AS (SELECT * FROM sample JOIN distinct_client_ids USING (client_id)),

    numbered_duplicates AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id) AS _n FROM base)
    --
    -- Retain only one document for each ID.
    SELECT
      * EXCEPT(_n)
    FROM
      numbered_duplicates
    WHERE
      _n = 1
    """.format(
        start_date=start_date.strftime(FORMAT_DS),
        end_date=end_date.strftime(FORMAT_DS),
        channel_filter=channel_filter,
        min_firefox_version=min_firefox_version,
    )

    bq = bigquery.Client()
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
        .rdd
    )


def revert(s):
    replacements = {
        "client_id": "clientId",
        "creation_date": "creationDate",
        "keyed_histograms": "keyedHistograms",
        "build_id": "buildId",
        "user_prefs": "userPrefs",
        "service_pack_major": "servicePackMajor",
        "is_wow64": "isWow64",
        "memory_mb": "memoryMB",
    }
    if s in replacements:
        return replacements[s]
    else:
        return s


def convert_bigquery_results(f):
    "Convert dataframe-row rdd record into a format suitable for get_pings_properties, which does histogram conversion"
    d = f.asDict(True)
    # generally speaking, we could use additional_properties as the basis and correctly implement recursive merging
    # instead, we explicitly add the properties we know we care about at the end of this routine
    additional_properties = json.loads(d.pop("additional_properties") or "{}")
    newdict = {}
    for k, v in d.items():
        pieces = list(map(revert, k.split("__")))
        # print(pieces)
        if len(pieces) == 1:
            newdict[pieces[0]] = v
        elif len(pieces) == 3:
            first, second, third = pieces
            if second == "histograms" and isinstance(v, str):
                v = json.loads(v)
            if second == "keyedHistograms":
                if len(v) == 0:
                    continue
                else:
                    z = {}
                    for i in v:
                        z[i["key"]] = json.loads(i["value"])
                    v = z
            if first not in newdict:
                newdict[first] = {}
            if second not in newdict[first]:
                newdict[first][second] = {}
            newdict[first][second][third] = v
        elif len(pieces) == 4:
            first, second, third, fourth = pieces
            if first not in newdict:
                newdict[first] = {}
            if second not in newdict[first]:
                newdict[first][second] = {}
            if third not in newdict[first][second]:
                newdict[first][second][third] = {}
            newdict[first][second][third][fourth] = v
        elif len(pieces) == 5:
            first, second, third, fourth, fifth = pieces
            if fourth == "histograms" and isinstance(v, str):
                v = json.loads(v)
            if fourth == "keyedHistograms":
                if len(v) == 0:
                    continue
                else:
                    z = {}
                    for i in v:
                        z[i["key"]] = json.loads(i["value"])
                    v = z
            if first not in newdict:
                newdict[first] = {}
            if second not in newdict[first]:
                newdict[first][second] = {}
            if third not in newdict[first][second]:
                newdict[first][second][third] = {}
            if fourth not in newdict[first][second][third]:
                newdict[first][second][third][fourth] = {}
            newdict[first][second][third][fourth][fifth] = v
        elif len(pieces) > 5:
            raise (Exception("too many pieces"))

    # example additional_properties
    # {"environment":{"system":{"gfx":{"adapters":[{"driverVendor":null}],"ContentBackend":"Skia"}},"addons":{"activeGMPlugins":{"dummy-gmp":{"applyBackgroundUpdates":1}}}},"payload":{"processes":{"extension":{"histograms":{"FXA_CONFIGURED":{"bucket_count":3,"histogram_type":3,"sum":0,"range":[1,2],"values":{"0":1,"1":0}}}}},"simpleMeasurements":{"selectProfile":15637,"XPI_startup_end":27497,"XPI_finalUIStartup":29308,"start":696,"AMI_startup_begin":19076,"startupCrashDetectionBegin":18382,"AMI_startup_end":27766,"afterProfileLocked":15742,"startupInterrupted":0,"maximalNumberOfConcurrentThreads":72,"createTopLevelWindow":29539,"XPI_bootstrap_addons_end":27497,"XPI_bootstrap_addons_begin":27382,"sessionRestoreInitialized":29400,"delayedStartupStarted":38798,"sessionRestoreInit":29358,"debuggerAttached":0,"startupCrashDetectionEnd":75127,"delayedStartupFinished":40246,"XPI_startup_begin":19670}}}'
    if (
        "environment" in additional_properties
        and "system" in additional_properties["environment"]
        and "gfx" in additional_properties["environment"]["system"]
    ):
        gfx = additional_properties["environment"]["system"]["gfx"]
        if "ContentBackend" in gfx:
            newdict["environment"]["system"]["gfx"]["ContentBackend"] = gfx[
                "ContentBackend"
            ]
        if "adapters" in gfx:
            for i, adapter in enumerate(gfx["adapters"]):
                newdict["environment"]["system"]["gfx"]["adapters"][i].update(adapter)

    return newdict
