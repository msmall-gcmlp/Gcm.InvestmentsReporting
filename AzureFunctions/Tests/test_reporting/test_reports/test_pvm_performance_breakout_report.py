from copy import deepcopy
import numpy as np
from Reporting.Reports.entity_reports.utils.pvm_performance_utils.pvm_performance_helper import (
    PvmPerformanceHelper,
    pd,
)
from gcm.inv.scenario import Scenario
from gcm.inv.utils.date.AggregateInterval import AggregateInterval
import datetime as dt
from gcm.inv.entityhierarchy.EntityDomain.entity_domain.entity_domain_types import (
    EntityDomainTypes,
    get_domain,
    EntityDomain,
)
from Reporting.Reports.entity_reports.xentity_reports.pvm_portfolio_performance_report import (
    PvmPerformanceBreakoutReport,
)
from Reporting.Reports.report_names import ReportNames
from Reporting.core.report_structure import (
    ReportMeta,
    Frequency,
    FrequencyType,
    ReportType,
    ReportConsumer,
    Calendar,
)
from utils.print_utils import print
import pytest
from gcm.Dao.DaoRunner import DaoSource, DaoRunnerConfigArgs
from gcm.inv.scenario import Scenario, DaoRunner

import io
from typing import Dict, Type, Union, List
from azure.storage.filedatalake import FileSystemClient, DataLakeFileClient
from azure.storage.blob import ContainerClient, BlobClient, BlobProperties
from gcm.data.storage import DataLakeZone, StorageQueryParams, DataLakeClient, StorageBlobClient
from gcm.data import DataAccess, DataSource
import json
import re



#ad hoc
@pytest.fixture
def sut() -> DataAccess:
    return DataAccess()

class TestReportingRename(object):



    @pytest.mark.parametrize(
        ("data_access_type", "data_source", "account_name", "file_system", "path", "expected_client_type", "expected_file_client_type"),
        [
            (DataLakeClient, DataSource.DataLake, "gcmdatalakenonprd", DataLakeZone.raw, "test/rqstest/test_created_blob.csv",
             FileSystemClient, DataLakeFileClient),
            (StorageBlobClient, DataSource.Blob, "storage54ywawydt3bxs", "unfiled", "test_created_blob.csv",
             ContainerClient, BlobClient)
        ]
    )
    def test__storage_types(
        self,
        sut: DataAccess,
        data_access_type: Type,
        data_source: DataSource,
        account_name: str,
        file_system: Union[str, DataLakeZone],
        path: str,
        expected_client_type: Union[FileSystemClient, ContainerClient],
        expected_file_client_type: Union[DataLakeFileClient, BlobClient]
    ):
        """Acceptance test targeting non-production Azure Storage and DataLake
        """
        # Arrange, Act
        dao = sut.get(data_source, account_name)

        client = dao.get_client(file_system)
        file_client = dao.get_file_client(client, path)

        # Assert
        assert dao is not None
        assert isinstance(dao, data_access_type)
        assert isinstance(client, expected_client_type)
        assert isinstance(file_client, expected_file_client_type)

    @pytest.mark.parametrize(
        ("data_source", "account_name", "file_system", "path", "expected_number"),
        [
            (DataSource.DataLake, "gcmdatalakenonprd", DataLakeZone.raw, "test/rqstest/metadata", 5),
            (DataSource.Blob, "storage54ywawydt3bxs", "unfiled", "", 5)
        ]
    )
    def test__storage_get_paths__return_paths(
        self,
        sut: DataAccess,
        data_source: DataSource,
        account_name: str,
        file_system: Union[str, DataLakeZone],
        path: str,
        expected_number: int
    ):
        """Acceptance test targeting non-production Azure Storage and DataLake
        """
        # Arrange, Act
        dao = sut.get(data_source, account_name)
        files = dao.get_paths(StorageQueryParams(
            file_system=file_system,
            path=path))

        # Assert
        assert len(files) == expected_number

    @pytest.mark.parametrize(
        ("data_source", "account_name", "file_system", "path", "content_to_upload", "metadata_to_upload"),
        [
            (DataSource.DataLake, "gcmdatalakenonprd", DataLakeZone.raw, "test/rqstest/test_created_blob.csv",
             b'name,surname,age\r\njohn,doe,48\r\nmary,jane,22\r\n', {'key1': 'value1', 'key2': 'value2'}),
            (DataSource.Blob, "storage54ywawydt3bxs", "unfiled", "test_created_blob.csv",
             b'name,surname,age\r\njohn,doe,48\r\nmary,jane,22\r\n', {'key1': 'value1', 'key2': 'value2'})
        ]
    )
    def test__storage_crud(
        self,
        sut: DataAccess,
        data_source: DataSource,
        account_name: str,
        file_system: Union[str, DataLakeZone],
        path: str,
        content_to_upload: bytes,
        metadata_to_upload: Dict[str, str]
    ):
        """Acceptance test targeting non-production Azure Storage and DataLake
        """
        # Arrange, Act
        dao = sut.get(data_source, account_name)

        dao.create_blob(StorageQueryParams(
            file_system=file_system,
            path=path,
            metadata=metadata_to_upload),
            data=content_to_upload)

        original_file = dao.get_blob(StorageQueryParams(
            file_system=file_system,
            path=path))

        modified_data = (original_file.content.decode('utf-8')
                         + "abc,def,33\r\n").encode('utf-8')

        dao.upload_blob(StorageQueryParams(
            file_system=file_system,
            path=path,
            metadata=None),
            data=modified_data)

        dao.set_metadata(StorageQueryParams(
            file_system=file_system,
            path=path,
            metadata=metadata_to_upload))

        modified_file = dao.get_blob(StorageQueryParams(
            file_system=file_system,
            path=path))

        dao.delete_blob(StorageQueryParams(
            file_system=file_system,
            path=path))

        # Assert
        assert original_file.content == content_to_upload
        assert modified_file.content == modified_data
        assert modified_file.file_properties.metadata == metadata_to_upload

    @pytest.mark.parametrize(
        ("data_source", "account_name", "file_system", "path", "content_to_upload"),
        [
            (DataSource.DataLake, "gcmdatalakenonprd", DataLakeZone.raw, "test/rqstest/test_created_blob.csv",
             b'name,surname,age\r\njohn,doe,48\r\nmary,jane,22\r\n'),
            (DataSource.Blob, "storage54ywawydt3bxs", "unfiled", "test_created_blob.csv",
             b'name,surname,age\r\njohn,doe,48\r\nmary,jane,22\r\n')
        ]
    )
    def test__storage_stream(
        self,
        sut: DataAccess,
        data_source: DataSource,
        account_name: str,
        file_system: Union[str, DataLakeZone],
        path: str,
        content_to_upload: bytes
    ):
        """Acceptance test targeting non-production Azure Storage and DataLake
        """
        # Arrange, Act
        dao = sut.get(data_source, account_name)

        dao.create_blob(StorageQueryParams(
            file_system=file_system,
            path=path),
            data=content_to_upload)

        storage_downloader = dao.get_downloader(StorageQueryParams(
            file_system=file_system,
            path=path))

        dao.delete_blob(StorageQueryParams(file_system=file_system, path=path))

        # Assert
        with io.BytesIO() as stream:
            storage_downloader.readinto(stream)
            stream.seek(0)
            file_content = stream.read()
            assert file_content == content_to_upload

    @staticmethod
    def get_sub_blob_names(file_system_client: ContainerClient, params: StorageQueryParams) -> List[str]:
        blob_names = [x for x in file_system_client.list_blob_names(name_starts_with=params.path)]
        return blob_names

    # Slower
    @staticmethod
    def get_sub_blob_details(file_system_client: ContainerClient, params: StorageQueryParams) -> List[
        BlobProperties]:
        blob_details = [x for x in file_system_client.list_blobs(name_starts_with=params.path)]
        return blob_details

    @staticmethod
    def get_source_entity_id_from_idw_id(
            entity_id: int,
            domain_source_mapping_table: str='PortfolioSourceMapping',
            source_name: str='PVM.MED'):

        def idw_dao_operation(dao, params):
            raw = f" select top 1 ExternalId from entitymaster.{domain_source_mapping_table} e" \
                  f" left join entitymaster.SourceDimn s" \
                  f" on e.SourceId=s.SourceId" \
                  f" where " \
                  f" SourceName = '{source_name}' " \
                  f" and" \
                  f" EntityId= {entity_id}"
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        dao: DaoRunner = Scenario.get_attribute("dao")

        source_entity_id: pd.DataFrame = dao.execute(
            params={},
            source=DaoSource.InvestmentsDwh,
            operation=idw_dao_operation,
        )
        source_entity_id = int(float(source_entity_id.squeeze()))

        return source_entity_id

    def test_reporting_rename(self,
                              sut: DataAccess):
        with Scenario(
                as_of_date=dt.date.today(),
                aggregate_interval=AggregateInterval.ITD,
                save=True,
                dao_config={
                    DaoRunnerConfigArgs.dao_global_envs.name: {
                        DaoSource.DataLake.name: {
                            "Environment": "prd",
                            "Subscription": "prd",
                        },
                        # DaoSource.InvestmentsDwh.name: {
                        #     "Environment": "prd",
                        #     "Subscription": "prd",
                        # },
                        DaoSource.DataLake_Blob.name: {
                            "Environment": "prd",
                            "Subscription": "prd",
                        },
                        DaoSource.ReportingStorage.name: {
                            "Environment": "prd",
                            "Subscription": "prd",
                            },
                        }
                    }
        ).context():



                uat_reporting_hub = 'j4r35rf2polr4'
                hub_address = f"storage{uat_reporting_hub}"

                # prod_reporting_hub = 'c5cw4wheqixsu'
                # hub_address = f"storage{prod_reporting_hub}"
                dao = sut.get(DataSource.Blob, hub_address)
                folder_params = StorageQueryParams("performance", "Performance/2022_12_31/", None)
                file_system_client = dao.get_client(folder_params.file_system)

                def is_to_be_operated_on(_b: BlobProperties) -> bool:
                    check_name = 'PvmPerformanceBreakoutReport'
                    if check_name in _b.name:
                        return True
                    return False

                def operate_on_blob(_b: BlobProperties):
                    blob_client = file_system_client.get_blob_client(_b)
                    blob = blob_client.download_blob()
                    metadata = blob.properties.metadata

                    if metadata['gcm_report_name'] == 'PE Portfolio Performance Breakout':
                        metadata['gcm_report_name'] = 'PE Portfolio Performance'
                        if metadata['gcm_entity_source'] == "IDW":
                            idw_entity_id = int(re.sub('[^0-9]', '', metadata['gcm_entity_ids']))
                            pvm_entity_id = TestReportingRename.get_source_entity_id_from_idw_id(
                                entity_id=idw_entity_id,
                                domain_source_mapping_table='PortfolioSourceMapping',
                                source_name='PVM.MED'
                            )

                            metadata['gcm_entity_source'] = 'pvm-med'
                            metadata['gcm_entity_ids'] = str([pvm_entity_id])

                            # can't print things to debug; AttributeError: 'str' object has no attribute 'save_params'
                            # print(f"{metadata['gcm_report_name']} - updating {metadata['gcm_entity_name']} - {metadata['gcm_as_of_date']}")
                            blob_client.set_blob_metadata(
                                metadata
                            )



                sub_blobs = TestReportingRename.get_sub_blob_details(file_system_client, folder_params)

                for b in sub_blobs:
                    if is_to_be_operated_on(b):
                        operate_on_blob(b)




class TestPerformanceBreakDown(object):
    @staticmethod
    def get_entity(domain, name):
        entity_domain_table: EntityDomain = get_domain(domain)
        [r, s] = entity_domain_table.get_by_entity_names(
            [name],
        )
        entity_info: pd.DataFrame = EntityDomain.merge_ref_and_sources(
            r, s
        )
        entity_info.SourceName = np.where(entity_info.SourceName == 'PVM.MED', 'pvm-med', entity_info.SourceName)
        return entity_info

    @staticmethod
    def get_pe_only_portfolios(active_only=True):
        def pvm_dao_operation(dao, params):
            raw = """
            select distinct
                [Portfolio Master Id] PortfolioMasterId, 
                [Operational Series Ticker] OperationalSeriesTicker,
                [Portfolio Reporting Name] PortfolioReportingName, 
                [Operational Series Investment Type] OperationalSeriesInvestmentType,
                [Portfolio Ticker] PortfolioTicker, 
                [Portfolio Currency] PortfolioCurrency, 
                [Deal Predominant Asset Class] DealPredominantAssetClass
            from analytics.MasterEntityDataInvestmentTrack
            where [Portfolio Master Id] not in 
                (
                    select distinct [Portfolio Master Id]
                    from analytics.MasterEntityDataInvestmentTrack
                    where [Deal Predominant Asset Class] != 'Private Equity'
                )
            """
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        def idw_dao_operation(dao, params):
            raw = f" select distinct OwnerName as OperationalSeriesTicker" \
                  f" from iLevel.vExtendedCollapsedCashflows" \
                  f" where TransactionDate = '{as_of_date}'" \
                  f" and TransactionType = 'Net Asset Value'" \
                  f" and BaseAmount > 0"
            df = pd.read_sql(
                raw,
                dao.data_engine.session.bind,
            )
            return df

        dao: DaoRunner = Scenario.get_attribute("dao")
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")

        portfolios: pd.DataFrame = dao.execute(
            params={},
            source=DaoSource.PvmDwh,
            operation=pvm_dao_operation,
        )
        if active_only:
            active_os: pd.DataFrame = dao.execute(
                params={},
                source=DaoSource.InvestmentsDwh,
                operation=idw_dao_operation,
            )
            portfolios = portfolios[portfolios.OperationalSeriesTicker.isin(active_os.OperationalSeriesTicker)]

        # TODO: set up warning and or assertion error
        return portfolios

    def test_basic_helper_object(self):
        as_of_date = dt.date(2022, 12, 31)
        with Scenario(
            as_of_date=as_of_date,
            aggregate_interval=AggregateInterval.ITD,
            save=False,
        ).context():
            port_name = "The Consolidated Edison Pension Plan Master Trust - GCM PE Account"
            domain = EntityDomainTypes.Portfolio
            info = TestPerformanceBreakDown.get_entity(
                domain=domain, name=port_name
            )
            this_helper = PvmPerformanceHelper(domain, info)
            final_data = this_helper.generate_components_for_this_entity(
                as_of_date
            )
            assert final_data is not None

    def test_new_report_run(self):
        as_of_date = dt.date(2023, 3, 31)
        # as_of_date = dt.date(2022, 12, 31)
        error_df = pd.DataFrame()

        with Scenario(
                as_of_date=as_of_date,
                aggregate_interval=AggregateInterval.ITD,
                save=True,
                dao_config={
                    DaoRunnerConfigArgs.dao_global_envs.name: {
                        # DaoSource.DataLake.name: {
                        #     "Environment": "prd",
                        #     "Subscription": "prd",
                        # },
                        # DaoSource.PubDwh.name: {
                        #     "Environment": "prd",
                        #     "Subscription": "prd",
                        # },
                        # DaoSource.InvestmentsDwh.name: {
                        #     "Environment": "prd",
                        #     "Subscription": "prd",
                        # },
                        # DaoSource.DataLake_Blob.name: {
                        #     "Environment": "prd",
                        #     "Subscription": "prd",
                        # },
                        # DaoSource.ReportingStorage.name: {
                        #     "Environment": "prd",
                        #     "Subscription": "prd",
                        # },
                    }
                }
        ).context():
            # portfolios_to_run = TestPerformanceBreakDown.get_pe_only_portfolios(active_only=True)
            # port_list = list(reversed(list(set(portfolios_to_run.PortfolioReportingName.to_list()))))

            # set report name and dimension config here
            report_name = ReportNames.PE_Portfolio_Performance_x_Investment_Manager

            port_list = ['The Consolidated Edison Pension Plan Master Trust - GCM PE Account']
            for port_name in port_list:
                domain = EntityDomainTypes.Portfolio
                info = TestPerformanceBreakDown.get_entity(
                    domain=domain, name=port_name
                )
                try:
                    this_report = PvmPerformanceBreakoutReport(
                        report_name_enum=report_name,
                        report_meta=ReportMeta(
                            type=ReportType.Performance,
                            interval=Scenario.get_attribute(
                                "aggregate_interval"
                            ),
                            consumer=ReportConsumer(
                                horizontal=[ReportConsumer.Horizontal.IC],
                                vertical=ReportConsumer.Vertical.PE,
                            ),
                            frequency=Frequency(
                                FrequencyType.Quarterly, Calendar.AllDays
                            ),
                            entity_domain=domain,
                            entity_info=info,
                        ),
                    )

                    output = print(
                        report_structure=this_report, print_pdf=True
                    )

                except Exception as e:
                    error_msg = getattr(e, "message", repr(e))
                    # print(error_msg)
                    error_df = pd.concat(
                        [
                            pd.DataFrame(
                                {
                                    "Portfolio": [port_name],
                                    "Date": [as_of_date],
                                    "ErrorMessage": [error_msg],
                                }
                            ),
                            error_df,
                        ]
                    )
                # error_df.to_csv('C:/Tmp/error df port reports.csv')
                # assert output is not None