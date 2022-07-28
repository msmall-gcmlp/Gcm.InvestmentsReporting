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

    @pytest.mark.skip()
    def test_report_construction(self, runner):
        params_all = AzureDataLakeDao.create_get_data_params(
            "raw/test/rqstest/rqstest/Re Uploads/",
            "Ethos_09-30-2021_All.xlsx",
        )
        file_all: AzureDataLakeFile = runner.execute(
            source=DaoSource.DataLake,
            params=params_all,
            operation=lambda dao, p: dao.get_data(p),
        )
        params_realized = AzureDataLakeDao.create_get_data_params(
            "raw/test/rqstest/rqstest/Re Uploads/",
            "Ethos_09-30-2021_Realized.xlsx",
        )
        file_realized: AzureDataLakeFile = runner.execute(
            source=DaoSource.DataLake,
            params=params_realized,
            operation=lambda dao, p: dao.get_data(p),
        )
        data = {"all": file_all, "realized": file_realized}
        PerformanceConcentrationReport(
            runner,
            asofdate=dt.datetime(2021, 9, 30),
            managername="Ethos",
            vertical="Real Estate",
            underwriting="Fund IV",
        ).execute(data=data)
