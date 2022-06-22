from unittest import mock
from gcm.Scenario.scenario import Scenario
import pytest
import datetime as dt
import pandas as pd
import ast

from gcm.inv.reporting.reports.performance_quality_peer_summary_report import PerformanceQualityPeerSummaryReport
from gcm.inv.reporting.reports.performance_quality_report import PerformanceQualityReport
from gcm.inv.reporting.reports.performance_quality_report_data import PerformanceQualityReportData
from gcm.Dao.DaoRunner import DaoRunner, DaoSource, DaoRunnerConfigArgs


class TestPerformanceQualityReport:
    @pytest.fixture
    def runner(self):
        config_params = {
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.PubDwh.name: {
                    "Environment": "uat",
                    "Subscription": "nonprd",
                },
            }
        }

        runner = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params=config_params,
        )
        return runner

    @pytest.fixture
    def perf_quality_report(self, runner):
        # TODO consider refactoring as_of_date to Scenario
        return PerformanceQualityReport(runner=runner, as_of_date=dt.date(2022, 3, 31), fund_name='Skye')

    @pytest.fixture
    def perf_quality_peer(self, runner):
        # TODO consider refactoring as_of_date to Scenario
        return PerformanceQualityPeerSummaryReport(runner=runner, as_of_date=dt.date(2022, 3, 31),
                                                   peer_group='GCM TMT')

    #@pytest.mark.skip(reason='slow')
    def test_performance_quality_report_data(self, runner):
        with Scenario(runner=runner, as_of_date=dt.date(2022, 3, 31)).context():
            perf_quality = PerformanceQualityReportData(start_date=dt.date(2012, 3, 1),
                                                        end_date=dt.date(2022, 3, 31),
                                                        investment_group_ids=[19224, 23319, 74984]
                                                        )

            report_inputs = perf_quality.get_performance_quality_report_inputs()

        ###
        # gcm_peer_constituent_returns = pd.read_json(report_inputs['gcm_peer_constituent_returns'], orient='index')
        # gcm_peer_columns = [ast.literal_eval(x) for x in gcm_peer_constituent_returns.columns]
        # gcm_peer_columns = pd.MultiIndex.from_tuples(gcm_peer_columns, names=['PeerGroupName', 'SourceInvestmentId'])
        # gcm_peer_constituent_returns.columns = gcm_peer_columns
        # gcm_peer_constituent_returns = gcm_peer_constituent_returns[['GCM TMT', 'GCM Multi-PM']]
        # report_inputs['gcm_peer_constituent_returns'] = gcm_peer_constituent_returns.to_json(orient='index')
        #
        # eurekahedge_constituent_returns = pd.read_json(report_inputs['eurekahedge_constituent_returns'], orient='index')
        # eh_columns = [ast.literal_eval(x) for x in eurekahedge_constituent_returns.columns]
        # eh_columns = pd.MultiIndex.from_tuples(eh_columns, names=['EurekahedgeBenchmark', 'SourceInvestmentId'])
        # eurekahedge_constituent_returns.columns = eh_columns
        # eurekahedge_constituent_returns = eurekahedge_constituent_returns[['EHI50 Multi-Strategy',
        #                                                                    'EHI50 Long/Short Equity']]
        # report_inputs['eurekahedge_constituent_returns'] = eurekahedge_constituent_returns.to_json(orient='index')
        #
        # import json
        # with open('gcm/inv/reporting/test_data/performance_quality_report_inputs.json', 'w') as fp:
        #     json.dump(report_inputs, fp)
        ###

        fund_dimn = pd.read_json(report_inputs['fund_dimn'], orient='index')
        fund_returns = pd.read_json(report_inputs['fund_returns'], orient='index')
        eurekahedge_returns = pd.read_json(report_inputs['eurekahedge_returns'], orient='index')
        abs_bmrk_returns = pd.read_json(report_inputs['abs_bmrk_returns'], orient='index')
        gcm_peer_returns = pd.read_json(report_inputs['gcm_peer_returns'], orient='index')

        gcm_peer_constituent_returns = pd.read_json(report_inputs['gcm_peer_constituent_returns'], orient='index')
        gcm_peer_columns = [ast.literal_eval(x) for x in gcm_peer_constituent_returns.columns]
        gcm_peer_columns = pd.MultiIndex.from_tuples(gcm_peer_columns, names=['PeerGroupName', 'SourceInvestmentId'])
        gcm_peer_constituent_returns.columns = gcm_peer_columns

        eurekahedge_constituent_returns = pd.read_json(report_inputs['eurekahedge_constituent_returns'], orient='index')
        eh_columns = [ast.literal_eval(x) for x in eurekahedge_constituent_returns.columns]
        eh_columns = pd.MultiIndex.from_tuples(eh_columns, names=['EurekahedgeBenchmark', 'SourceInvestmentId'])
        eurekahedge_constituent_returns.columns = eh_columns

        exposure_latest = pd.read_json(report_inputs['exposure_latest'], orient='index')
        exposure_3y = pd.read_json(report_inputs['exposure_3y'], orient='index')
        exposure_5y = pd.read_json(report_inputs['exposure_5y'], orient='index')
        exposure_10y = pd.read_json(report_inputs['exposure_10y'], orient='index')
        rba = pd.read_json(report_inputs['rba'], orient='index')
        rba_risk_decomp = pd.read_json(report_inputs['rba_risk_decomp'], orient='index')
        rba_adj_r_squared = pd.read_json(report_inputs['rba_adj_r_squared'], orient='index')

        pba_publics = pd.read_json(report_inputs['pba_publics'], orient='index')
        pba_privates = pd.read_json(report_inputs['pba_privates'], orient='index')

        assert fund_dimn.shape[0] > 0
        assert fund_returns.shape[0] > 0
        assert eurekahedge_returns.shape[0] > 0
        assert abs_bmrk_returns.shape[0] > 0
        assert gcm_peer_returns.shape[0] > 0
        assert gcm_peer_constituent_returns.shape[0] > 0
        assert eurekahedge_constituent_returns.shape[0] > 0
        assert exposure_latest.shape[0] == 3
        assert exposure_3y.shape[0] == 3
        assert exposure_5y.shape[0] == 3
        assert exposure_10y.shape[0] == 3
        assert rba.shape[0] > 0
        assert rba_risk_decomp.shape[0] > 0
        assert rba_adj_r_squared.shape[0] > 0
        assert pba_publics.shape[0] > 0
        assert pba_privates.shape[0] > 0

    @mock.patch("gcm.inv.reporting.reports.performance_quality_report.PerformanceQualityReport.download_performance_quality_report_inputs", autospec=True)
    def test_performance_quality_report_skye(self, mock_download, performance_quality_report_inputs,
                                             perf_quality_report):
        mock_download.return_value = performance_quality_report_inputs
        benchmark_summary = perf_quality_report.build_benchmark_summary()
        assert all(benchmark_summary.index == ['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y', '10Y'])
        expected_columns = ['Fund',
                            'AbsoluteReturnBenchmark', 'AbsoluteReturnBenchmarkExcess',
                            'GcmPeer', 'GcmPeerExcess',
                            'EHI50', 'EHI50Excess',
                            'EHI200', 'EHI200Excess',
                            'Peer1Ptile', 'Peer2Ptile',
                            'EH50Ptile', 'EHI200Ptile']
        assert all(benchmark_summary.columns == expected_columns)

    @mock.patch("gcm.inv.reporting.reports.performance_quality_report.PerformanceQualityReport.download_performance_quality_report_inputs", autospec=True)
    def test_performance_quality_report_future_ahead(self, mock_download, performance_quality_report_inputs, perf_quality_report):
        mock_download.return_value = performance_quality_report_inputs
        benchmark_summary = perf_quality_report.build_benchmark_summary()
        assert all(benchmark_summary.index == ['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y', '10Y'])
        expected_columns = ['Fund',
                            'AbsoluteReturnBenchmark', 'AbsoluteReturnBenchmarkExcess',
                            'GcmPeer', 'GcmPeerExcess',
                            'EHI50', 'EHI50Excess',
                            'EHI200', 'EHI200Excess',
                            'Peer1Ptile', 'Peer2Ptile',
                            'EH50Ptile', 'EHI200Ptile']
        assert all(benchmark_summary.columns == expected_columns)

    @mock.patch("gcm.inv.reporting.reports.performance_quality_report.PerformanceQualityReport.download_performance_quality_report_inputs", autospec=True)
    def test_exposure_skye(self, mock_download, performance_quality_report_inputs, perf_quality_report):
        mock_download.return_value = performance_quality_report_inputs
        benchmark_summary = perf_quality_report.build_exposure_summary()
        latest_exposure_heading = perf_quality_report.get_latest_exposure_heading()
        assert all(benchmark_summary.index == ['Latest', '3Y', '5Y', '10Y'])
        assert all(benchmark_summary.columns == ['LongNotional', 'ShortNotional', 'GrossNotional', 'NetNotional'])
        assert len(latest_exposure_heading) == 1

    @mock.patch("gcm.inv.reporting.reports.performance_quality_report.PerformanceQualityReport.download_performance_quality_report_inputs", autospec=True)
    def test_perf_stability_skye(self, mock_download, performance_quality_report_inputs, perf_quality_report):
        mock_download.return_value = performance_quality_report_inputs
        stability_summary = perf_quality_report.build_performance_stability_fund_summary()
        assert stability_summary.shape[0] > 0
        assert all(stability_summary.index == ['TTM', '3Y', '5Y', '5YMedian'])
        assert all(stability_summary.columns == ['Vol', 'Beta', 'Sharpe', 'BattingAvg', 'WinLoss',
                                                 'Return_min', 'Return_25%', 'Return_75%', 'Return_max',
                                                 'Sharpe_min', 'Sharpe_25%', 'Sharpe_75%', 'Sharpe_max'])

    @pytest.mark.skip('replaced by read in peer summary')
    @mock.patch("gcm.inv.reporting.reports.performance_quality_report.PerformanceQualityReport.download_performance_quality_report_inputs", autospec=True)
    def test_perf_stability_peer_skye(self, mock_download, performance_quality_report_inputs, perf_quality_report):
        mock_download.return_value = performance_quality_report_inputs
        stability_summary = perf_quality_report.build_performance_stability_peer_summary()
        assert stability_summary.shape[0] > 0
        assert all(stability_summary.index == ['TTM', '3Y', '5Y', '5YMedian'])
        assert all(stability_summary.columns == ['AvgVol', 'AvgBeta', 'AvgSharpe', 'AvgBattingAvg', 'AvgWinLoss',
                                                 'AvgReturn_min', 'AvgReturn_25%', 'AvgReturn_75%', 'AvgReturn_max',
                                                 'AvgSharpe_min', 'AvgSharpe_25%', 'AvgSharpe_75%', 'AvgSharpe_max'])

    @mock.patch("gcm.inv.reporting.reports.performance_quality_report.PerformanceQualityReport.download_performance_quality_report_inputs", autospec=True)
    def test_rba_skye(self, mock_download, performance_quality_report_inputs, perf_quality_report):
        mock_download.return_value = performance_quality_report_inputs
        rba = perf_quality_report.build_rba_summary()
        assert rba.shape[0] > 0
        assert all(rba.index == ['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y'])
        assert all(rba.columns == ['Total', 'Market Beta', 'Region', 'Industries', 'Styles',
                                   'Hedge Fund Technicals', 'Selection Risk', 'Unexplained',
                                   'Beta', 'X-Asset', 'L/S', 'Residual', 'AdjR2'])

    @mock.patch("gcm.inv.reporting.reports.performance_quality_report.PerformanceQualityReport.download_performance_quality_report_inputs", autospec=True)
    def test_pba_skye(self, mock_download, performance_quality_report_inputs, perf_quality_report):
        mock_download.return_value = performance_quality_report_inputs
        pba = perf_quality_report.build_pba_summary()
        assert pba.shape[0] > 0
        assert all(pba.index == ['MTD - Publics', 'MTD - Privates', 'QTD - Publics',
                                 'QTD - Privates', 'YTD - Publics', 'YTD - Privates'])
        assert all(pba.columns == ['Total', 'Beta', 'Regional', 'Industry', 'MacroRV',
                                   'LS_Equity', 'LS_Credit', 'Residual', 'Fees', 'Unallocated'])

    @mock.patch("gcm.inv.reporting.reports.performance_quality_report.PerformanceQualityReport.download_performance_quality_report_inputs", autospec=True)
    def test_shortfall_skye(self, mock_download, performance_quality_report_inputs, perf_quality_report):
        mock_download.return_value = performance_quality_report_inputs
        shortfall = perf_quality_report.build_shortfall_summary()
        assert all(shortfall.columns == ['Trigger', 'Drawdown', 'Pass/Fail'])

    @mock.patch("gcm.inv.reporting.reports.performance_quality_report.PerformanceQualityReport.download_performance_quality_report_inputs", autospec=True)
    def test_risk_model_expectations_skye(self, mock_download, performance_quality_report_inputs, perf_quality_report):
        mock_download.return_value = performance_quality_report_inputs
        summary = perf_quality_report.build_risk_model_expectations_summary()
        assert all(summary.columns == ['Expectations'])
        assert all(summary.index == ['ExpectedReturn', 'ExpectedVolatility'])

    @mock.patch("gcm.inv.reporting.reports.performance_quality_peer_summary_report.PerformanceQualityPeerSummaryReport.download_performance_quality_report_inputs", autospec=True)
    def test_peer_summary(self, mock_download, performance_quality_report_inputs, perf_quality_peer):
        mock_download.return_value = performance_quality_report_inputs
        summary = perf_quality_peer.build_performance_stability_peer_summary()
        assert summary.shape[0] > 0
        assert all(summary.index == ['TTM', '3Y', '5Y', '5YMedian'])
        assert all(summary.columns == ['AvgVol', 'AvgBeta', 'AvgSharpe', 'AvgBattingAvg', 'AvgWinLoss',
                                       'AvgReturn_min', 'AvgReturn_25%', 'AvgReturn_75%', 'AvgReturn_max',
                                       'AvgSharpe_min', 'AvgSharpe_25%', 'AvgSharpe_75%', 'AvgSharpe_max'])

    @mock.patch("gcm.inv.reporting.reports.performance_quality_report.PerformanceQualityReport.download_performance_quality_report_inputs", autospec=True)
    def test_pub_investment_group_id(self, mock_download, performance_quality_report_inputs, perf_quality_report):
        mock_download.return_value = performance_quality_report_inputs
        id = perf_quality_report._pub_investment_group_id
        assert id == 618

    @pytest.mark.skip("debugging purposes only")
    def test_debug(self):
        from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao, AzureDataLakeFile
        runner = DaoRunner(
            container_lambda=lambda b, i: b.config.from_dict(i),
            config_params={
                DaoRunnerConfigArgs.dao_global_envs.name: {
                    DaoSource.ReportingStorage.name: {
                        "Environment": "uat",
                        "Subscription": "nonprd",
                    }
                }
            })
        file_path = 'Performance Quality_Whale Rock_PFUND_Risk_2022-05-31.xlsx'
        params = AzureDataLakeDao.create_get_data_params('performance/Risk', file_path)
        file: AzureDataLakeFile = runner.execute(
            params=params,
            source=DaoSource.ReportingStorage,
            operation=lambda dao, params: dao.get_data(params),
        )
        file.content
