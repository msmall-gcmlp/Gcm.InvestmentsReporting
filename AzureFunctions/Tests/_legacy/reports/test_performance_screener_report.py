import pytest
import datetime as dt
from gcm.inv.scenario import Scenario
from _legacy.Reports.reports.peer_rankings.performance_screener_report import PerformanceScreenerReport
from gcm.Dao.DaoRunner import DaoRunner


class TestPeerRankingReport:
    @pytest.fixture
    def runner(self):
        return DaoRunner()

    def test_peer_ranking_report(self, runner):
        with Scenario(runner=runner, as_of_date=dt.date(2022, 3, 31)).context():
            peer_ranking_report = PerformanceScreenerReport(peer_group=['GCM Multi-PM'])
            constituent_returns = peer_ranking_report._updated_constituent_returns
            constituents = peer_ranking_report._constituents
            standalone_metrics, _ = peer_ranking_report.build_standalone_metrics_summary()
            relative_metrics = peer_ranking_report.build_absolute_return_benchmark_summary()
            rba_excess = peer_ranking_report.build_rba_excess_return_summary()
            peer_ranking_report = PerformanceScreenerReport(peer_group=['GCM Multi-PM'])
            rba_risk = peer_ranking_report.build_rba_risk_decomposition_summary()

        assert constituent_returns.shape[0] == 36
        assert all(constituents.columns == ['InvestmentGroupId',
                                            'InvestmentGroupName',
                                            'InvestmentName',
                                            'InvestmentStatus'])
        assert all(standalone_metrics.columns[0:5] == ['Return', 'Vol', 'Sharpe', 'MaxPttDdown', 'BetaToSpx'])
        assert standalone_metrics.shape[0] > 0
        assert all(relative_metrics.columns == ['Excess', 'ExcessDecile'])
        assert relative_metrics.shape[0] > 0
        assert all(rba_excess.columns == ['RbaAlpha', 'RbaAlphaDecile'])
        assert rba_excess.shape[0] > 0
        assert all(rba_risk.columns == ['SYSTEMATIC', 'X_ASSET_CLASS', 'PUBLIC_LS', 'NON_FACTOR', 'PctIdioDecile'])
        assert rba_risk.shape[0] > 0
