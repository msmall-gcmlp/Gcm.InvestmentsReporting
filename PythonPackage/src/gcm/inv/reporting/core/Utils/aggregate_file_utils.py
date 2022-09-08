from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.Dao.daos.azure_datalake.azure_datalake_file import (
    AzureDataLakeFile,
)
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.DaoRunner import DaoRunner
from typing import List
import itertools

# standard utils:


def copy_metadata(
    runner: DaoRunner,
    target_file_location: str,
    target_file_name: str,
    target_dao_type: DaoSource,
    source_file_location: str = None,
    source_file_name: str = None,
    source_dao_type: DaoSource = None,
    metadata: dict = None,
):
    if metadata is None:
        source_params = AzureDataLakeDao.create_get_data_params(
            source_file_location,
            source_file_name,
            retry=False,
        )

        excel_file: AzureDataLakeFile = runner.execute(
            params=source_params,
            source=source_dao_type,
            operation=lambda dao, params: dao.get_data(params),
        )

        metadata = excel_file.file_properties.metadata
    target_params = AzureDataLakeDao.create_get_data_params(
        target_file_location,
        target_file_name,
        retry=False,
        metadata=metadata,
    )

    target_file: AzureDataLakeFile = runner.execute(
        params=target_params,
        source=target_dao_type,
        operation=lambda dao, params: dao.get_data(params),
    )

    runner.execute(
        params=target_params,
        source=target_dao_type,
        operation=lambda dao, params: dao.post_data(
            params, target_file.content
        ),
    )


def merge_metadata_to_target_file(
    metadata_list: List[dict],
    target_file_location: str,
    target_file_name: str,
    target_dao_type: DaoSource,
) -> None:

    ordered_values = [
        v
        for d in itertools.chain(metadata_list)
        for _, v in sorted(d.items())
    ]
    result = dict(zip(metadata_list.count(1), ordered_values))
    copy_metadata(
        target_file_location=target_file_location,
        target_file_name=target_file_name,
        target_dao_type=target_dao_type,
        metadata=result,
    )
