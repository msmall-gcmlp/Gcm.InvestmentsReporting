from gcm.Dao.DaoRunner import DaoRunner, DaoSource, DaoRunnerConfigArgs
from _legacy.core.ReportStructure.report_structure import (
    base_output_location,
)
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeDao,
    AzureDataLakeFile,
)


def _get_location(params):
    source_zone = params.get("zone", base_output_location.replace("/", ""))
    source_location = params.get("location", "")
    top_level_loc = f"{source_zone}"
    if source_location is not None and source_location != "":
        top_level_loc += f"/{source_location}"
    return (source_zone, source_location, top_level_loc)


def get_file_streams(params):
    source_env = params.get("source_env", "dev")
    source_sub = params.get("source_sub", "nonprd")

    config_params = {
        DaoRunnerConfigArgs.dao_global_envs.name: {
            DaoSource.ReportingStorage.name: {
                "Environment": source_env,
                "Subscription": source_sub,
            }
        }
    }
    match_on = params.get("match_on", "")

    [zone, location, top_level_loc] = _get_location(params)

    def list_files(dao, params):
        params = dao.create_get_data_params(zone, "")
        filesystem_client = dao.data_engine.get_client(params)
        paths = [path for path in filesystem_client.list_blobs(location)]
        list = [x.name for x in paths]
        return list

    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params=config_params,
    )
    files = runner.execute(
        params=None,
        source=DaoSource.ReportingStorage,
        operation=list_files,
    )
    final = [f for f in files if match_on in f]
    # now for each file, get file stream associated with each:
    final_dict = {}

    for f in final:
        name: str = f"{f}"
        if "/" in f:
            name = name.split("/").pop()
        source_params = AzureDataLakeDao.create_get_data_params(
            top_level_loc,
            name,
            retry=False,
        )
        file: AzureDataLakeFile = runner.execute(
            params=source_params,
            source=DaoSource.ReportingStorage,
            operation=lambda dao, params: dao.get_data(params),
        )
        temp = {
            "data": file.content,
            "meta": file.file_properties.metadata,
        }
        final_dict[name] = temp
    return final_dict


def save_file_streams(params, stream_dict):
    target_env = params.get("target_env", "dev")
    target_sub = params.get("target_sub", "nonprd")
    [zone, location, top_level_loc] = _get_location(params)
    config_params = {
        DaoRunnerConfigArgs.dao_global_envs.name: {
            DaoSource.ReportingStorage.name: {
                "Environment": target_env,
                "Subscription": target_sub,
            }
        }
    }
    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params=config_params,
    )
    for name in stream_dict:
        target_params = AzureDataLakeDao.create_get_data_params(
            top_level_loc,
            name,
            retry=False,
            metadata=stream_dict[name]["meta"],
        )
        runner.execute(
            params=target_params,
            source=DaoSource.ReportingStorage,
            operation=lambda dao, params: dao.post_data(
                params, stream_dict[name]["data"]
            ),
        )
    return True
