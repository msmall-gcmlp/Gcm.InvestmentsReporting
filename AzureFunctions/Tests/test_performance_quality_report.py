import pytest
import datetime as dt
import pandas as pd
import json

from ..Reports.performance_quality_report import PerformanceQualityReport
from ..Reports.performance_quality_report_data import PerformanceQualityReportData
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

    def test_performance_quality_data(self, runner):
        params = {'group': 'EMM', 'vertical': 'ARS', 'entity': 'PFUND', 'filter': 'EMM'}
        perf_quality = PerformanceQualityReportData(
            runner=runner,
            start_date=dt.date(2020, 10, 1),
            end_date=dt.date(2021, 12, 31),
            as_of_date=dt.date(2021, 12, 31),
            params=params
        )

        report_inputs = perf_quality.execute()

        # with open('test_data/performance_quality_data.json', 'w') as fp:
        #     json.dump(report_inputs, fp)

        fund_dimn = pd.read_json(report_inputs['fund_dimn'], orient='index')
        fund_returns = pd.read_json(report_inputs['fund_returns'], orient='index')
        eurekahedge_returns = pd.read_json(report_inputs['eurekahedge_returns'], orient='index')
        abs_bmrk_returns = pd.read_json(report_inputs['abs_bmrk_returns'], orient='index')
        gcm_peer_returns = pd.read_json(report_inputs['gcm_peer_returns'], orient='index')

        assert fund_dimn.shape[0] > 0
        assert fund_returns.shape[0] > 0
        assert eurekahedge_returns.shape[0] > 0
        assert abs_bmrk_returns.shape[0] > 0
        assert gcm_peer_returns.shape[0] > 0

    def test_performance_quality_report_skye(self, runner):
        f = open('test_data/performance_quality_data.json')
        report_inputs = json.load(f)
        report_inputs['fund_name'] = 'Skye'
        report_inputs['vertical'] = 'ARS'
        report_inputs['entity'] = 'PFUND'
        perf_quality_report = PerformanceQualityReport(runner=runner, as_of_date=dt.date(2021, 12, 31),
                                                       params=report_inputs)
        benchmark_summary = perf_quality_report.build_benchmark_summary()
        assert all(benchmark_summary.index == ['MTD', 'QTD', 'YTD'])
        expected_columns = ['Fund',
                            'AbsoluteReturnBenchmark', 'AbsoluteReturnBenchmarkExcess',
                            'GcmPeer', 'GcmPeerExcess',
                            'EHI50', 'EHI50Excess',
                            'EHI200', 'EHI200Excess']
        assert all(benchmark_summary.columns == expected_columns)

    def test_performance_quality_report_citadel(self, runner):
        f = open('test_data/performance_quality_data.json')
        report_inputs = json.load(f)
        report_inputs['fund_name'] = 'Citadel'
        report_inputs['vertical'] = 'ARS'
        report_inputs['entity'] = 'PFUND'
        perf_quality_report = PerformanceQualityReport(runner=runner, as_of_date=dt.date(2021, 12, 31),
                                                       params=report_inputs)
        benchmark_summary = perf_quality_report.build_benchmark_summary()
        assert all(benchmark_summary.index == ['MTD', 'QTD', 'YTD'])
        expected_columns = ['Fund',
                            'AbsoluteReturnBenchmark', 'AbsoluteReturnBenchmarkExcess',
                            'GcmPeer', 'GcmPeerExcess',
                            'EHI50', 'EHI50Excess',
                            'EHI200', 'EHI200Excess']
        assert all(benchmark_summary.columns == expected_columns)

    def test_performance_quality_report_future_ahead(self, runner):
        f = open('test_data/performance_quality_data.json')
        report_inputs = json.load(f)
        report_inputs['fund_name'] = 'Skye'
        report_inputs['vertical'] = 'ARS'
        report_inputs['entity'] = 'PFUND'
        perf_quality_report = PerformanceQualityReport(runner=runner, as_of_date=dt.date(2099, 12, 31),
                                                       params=report_inputs)
        benchmark_summary = perf_quality_report.build_benchmark_summary()
        assert all(benchmark_summary.index == ['MTD', 'QTD', 'YTD'])
        expected_columns = ['Fund',
                            'AbsoluteReturnBenchmark', 'AbsoluteReturnBenchmarkExcess',
                            'GcmPeer', 'GcmPeerExcess',
                            'EHI50', 'EHI50Excess',
                            'EHI200', 'EHI200Excess']
        assert all(benchmark_summary.columns == expected_columns)
