import pytest

from gcm.Dao.DaoRunner import DaoRunner, DaoSource
from gcm.inv.reporting.reports.Pvm.TrackRecord.performance_concentration import (
    PerformanceConcentrationReport,
)
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import (
    AzureDataLakeDao,
    AzureDataLakeFile,
)
import datetime as dt


class TestPerformanceQualityReport:
    @pytest.fixture
    def runner(self):
        runner = DaoRunner()
        return runner

    def test_report_construction(self, runner):
        params_all = AzureDataLakeDao.create_get_data_params(
            "raw/test/rqstest/rqstest/",
            "NBPartners_All_2021-12-31.xlsx",
        )
        file_all: AzureDataLakeFile = runner.execute(
            source=DaoSource.DataLake,
            params=params_all,
            operation=lambda dao, p: dao.get_data(p),
        )
        params_realized = AzureDataLakeDao.create_get_data_params(
            "raw/test/rqstest/rqstest/",
            "NBPartners_Realized_2021-12-31.xlsx",
        )
        file_realized: AzureDataLakeFile = runner.execute(
            source=DaoSource.DataLake,
            params=params_realized,
            operation=lambda dao, p: dao.get_data(p),
        )
        data = {"all": file_all, "realized": file_realized}
        PerformanceConcentrationReport(
            runner, asofdate=dt.datetime(2021, 12, 31),
            managername="NorthBridge Partners",
            vertical='Real Estate',
            underwriting='Fund IV'
        ).execute(data=data)
