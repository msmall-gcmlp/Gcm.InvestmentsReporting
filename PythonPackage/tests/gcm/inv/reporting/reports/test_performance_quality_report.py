from unittest import mock

import pytest
import datetime as dt
import pandas as pd
import ast
import json

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
        params = dict()
        params['fund_name'] = 'Skye'
        params['vertical'] = 'ARS'
        params['entity'] = 'PFUND'
        return PerformanceQualityReport(runner=runner, as_of_date=dt.date(2021, 12, 31), params=params)

    def test_performance_quality_report_data(self, runner):
        params = {'vertical': 'ARS', 'entity': 'PFUND',
                  'status': 'EMM', 'investment_ids': '[34411, 41096, 139998]'}

        perf_quality = PerformanceQualityReportData(
            runner=runner,
            start_date=dt.date(2020, 1, 1),
            end_date=dt.date(2022, 3, 31),
            as_of_date=dt.date(2022, 3, 31),
            params=params
        )

        report_inputs = perf_quality.get_performance_quality_report_inputs()

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
        # with open('gcm/inv/reporting/test_data/performance_quality_report_inputs.json', 'w') as fp:
        #     json.dump(report_inputs, fp)

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

    @pytest.mark.skip('very slow')
    def test_performance_quality_report_data_no_inv_filter(self, runner):
        params = {'status': 'EMM', 'vertical': 'ARS', 'entity': 'PFUND',
                  'run': 'PerformanceQualityReportData'}
        perf_quality = PerformanceQualityReportData(
            runner=runner,
            start_date=dt.date(2020, 10, 1),
            end_date=dt.date(2021, 12, 31),
            as_of_date=dt.date(2021, 12, 31),
            params=params
        )
        report_inputs = perf_quality.get_performance_quality_report_inputs()

        # with open('test_data/performance_quality_report_inputs.json', 'w') as fp:
        #     json.dump(report_inputs, fp)

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

        assert fund_dimn.shape[0] > 0
        assert fund_returns.shape[0] > 0
        assert eurekahedge_returns.shape[0] > 0
        assert abs_bmrk_returns.shape[0] > 0
        assert gcm_peer_returns.shape[0] > 0
        assert gcm_peer_constituent_returns.shape[0] > 0
        assert eurekahedge_constituent_returns.shape[0] > 0

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

    @pytest.mark.skip(reason='inputs no longer passed in through params')
    def test_performance_quality_report_missing_returns(self, runner, performance_quality_report_inputs):
        returns = pd.read_json(performance_quality_report_inputs['fund_returns'], orient='index').drop(columns={'Skye'})
        performance_quality_report_inputs['fund_returns'] = returns.to_json(orient='index')
        performance_quality_report_inputs['fund_name'] = 'Skye'
        performance_quality_report_inputs['vertical'] = 'ARS'
        performance_quality_report_inputs['entity'] = 'PFUND'
        perf_quality_report = PerformanceQualityReport(runner=runner, as_of_date=dt.date(2021, 12, 31),
                                                       params=performance_quality_report_inputs)
        report = perf_quality_report.generate_performance_quality_report()
        assert report == 'Invalid inputs'

    @pytest.mark.skip(reason='for debugging only')
    def test_performance_quality_report_data_all(self, runner):
        params = {'status': 'EMM', 'vertical': 'ARS', 'entity': 'PFUND',
                  'run': 'PerformanceQualityReportData'}
        perf_quality = PerformanceQualityReportData(
            runner=runner,
            start_date=dt.date(2020, 10, 1),
            end_date=dt.date(2021, 12, 31),
            as_of_date=dt.date(2021, 12, 31),
            params=params
        )
        report_inputs = perf_quality.execute()

        with open('gcm/inv/reporting/test_data/performance_quality_report_inputs_all.json', 'w') as fp:
            json.dump(report_inputs, fp)

    @pytest.mark.skip(reason='for debugging only')
    def test_performance_quality_report_all(self, runner, performance_quality_report_inputs_all):
        report_inputs = performance_quality_report_inputs_all
        report_inputs['vertical'] = 'ARS'
        report_inputs['entity'] = 'PFUND'
        funds = pd.read_json(report_inputs['fund_dimn'], orient='index')['InvestmentGroupName'].tolist()
        for fund in funds:
            print(fund)
            report_inputs['fund_name'] = fund
            perf_quality_report = PerformanceQualityReport(runner=runner, as_of_date=dt.date(2021, 12, 31),
                                                           params=report_inputs)
            perf_quality_report.generate_performance_quality_report()

    @pytest.mark.skip(reason='debugging')
    def test_debug(self, runner):
        params = {'vertical': 'ARS', 'entity': 'PFUND',
                  'status': 'EMM', 'investment_ids': '[62737, 71394]'}

        perf_quality = PerformanceQualityReportData(
            runner=runner,
            start_date=dt.date(2012, 1, 1),
            end_date=dt.date(2022, 3, 31),
            as_of_date=dt.date(2022, 3, 31),
            params=params
        )

        funds = perf_quality.execute()

    @pytest.mark.skip(reason='for debugging only')
    def test_debug2(self, runner, performance_quality_report_inputs_all):
        params = dict()
        params['vertical'] = 'ARS'
        params['entity'] = 'PFUND'
        funds = ['GCM Macro SubFund', 'Hawksbridge']

        for fund in funds:
            print(fund)
            params['fund_name'] = fund
            perf_quality_report = PerformanceQualityReport(runner=runner, as_of_date=dt.date(2022, 3, 31), params=params)
            perf_quality_report.execute()
