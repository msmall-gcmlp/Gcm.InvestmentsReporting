import datetime as dt
from _legacy.core.ReportStructure.report_structure import ReportingEntityTypes, ReportType, ReportVertical
from _legacy.core.Runners.investmentsreporting import InvestmentsReportRunner
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource
from gcm.inv.scenario import Scenario
from gcm.inv.models.ars_peer_analysis.calibrate_peer_level_assumptions import CalibratePeerLevelAssumptions


class ArsPeerAssumptionDiagnosticReport(ReportingRunnerBase):
    def __init__(self):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")
        self._peer_model = CalibratePeerLevelAssumptions(runner=self._runner, as_of_date=self._as_of_date)
        self._peer_bmrk_mapping = self._peer_model.peer_bmrk_mapping

    def generate_excel_inputs(self, historical_peer_summary, peer_corr_mat, arb_assumptions, peer_exposure, col_order):
        mapping = self._peer_bmrk_mapping.set_index("ReportingPeerGroup").loc[col_order]

        arb_assumptions = mapping[['Ticker']].merge(arb_assumptions, left_on='Ticker', right_index=True, how='left')
        arb_assumptions = arb_assumptions.apply(lambda x: x.replace(' Index', '', regex=True))
        arb_assumptions = arb_assumptions.T

        bmrk_assignments = mapping[['Ticker', 'EhiBenchmarkShortName', 'BenchmarkSubstrategyShortName']]
        bmrk_assignments = bmrk_assignments.apply(lambda x: x.replace(' Index', '', regex=True))
        benchmark_assignments = bmrk_assignments.T

        excess_spreads = historical_peer_summary[['Peer90', 'Peer75', 'Peer25', 'Peer10']]
        peer_excess_spreads = excess_spreads[excess_spreads.index.get_level_values(1) == 'Peer'].loc[col_order]
        peer_excess_spreads = peer_excess_spreads.T
        ehi_excess_spreads = excess_spreads[excess_spreads.index.get_level_values(1) == 'ehi'].loc[col_order]
        ehi_excess_spreads = ehi_excess_spreads.T

        peer_historical_corr_mat = peer_corr_mat.loc[col_order, col_order]

        def _format(data, field, row_order=['Peer', 'EhiTop', 'ehi', 'gcm']):
            summary = data[field].reset_index()
            summary = summary.pivot(index='level_1', columns='level_0')

            summary.columns = summary.columns.droplevel(0)
            summary = summary.loc[row_order, col_order]
            return summary.loc[row_order, col_order]

        excel_data = {
            "arb_assumptions": arb_assumptions,
            "historical_total_vol": _format(historical_peer_summary, field='ItdTotalVol', row_order=['Peer', 'ehi']),
            "historical_beta_arb": _format(historical_peer_summary, field='ItdBetaVsBmrk', row_order=['Peer', 'ehi']),
            "avg_gross_exposure": peer_exposure.get('leverage'),
            "avg_net_exposure": peer_exposure.get('net_notional'),
            "min_return_date": _format(historical_peer_summary, field='MinDate'),
            "historical_excess_return": _format(historical_peer_summary, field='Alpha'),
            "historical_residual_vol": _format(historical_peer_summary, field='ResidVol'),
            "historical_excess_vol": _format(historical_peer_summary, field='Vol'),
            "historical_excess_arb_vol_ratio": _format(historical_peer_summary, field='VolOverBmrkVol'),
            "historical_excess_corr_vs_arb": _format(historical_peer_summary, field='Corr'),
            "peer_excess_ptile_spreads": peer_excess_spreads,
            "ehi_excess_ptile_spreads": ehi_excess_spreads,
            "historical_5y_avg_corr_to_other_bmrks": _format(historical_peer_summary, field='IntraPeerBmrkCorr5Y'),
            "historical_itd_avg_corr_to_other_bmrks": _format(historical_peer_summary, field='IntraPeerBmrkCorrItd'),
            "historical_excess_corr_vs_all_arbs": _format(historical_peer_summary, field='AvgArbCorr'),
            "historical_excess_corr_arb_pairs": _format(historical_peer_summary, field='ArbCorrPairs'),
            "benchmark_assignments": benchmark_assignments,
            "peer_historical_corr_mat": peer_historical_corr_mat,
            "historical_excess_corr_vs_all_peers": _format(historical_peer_summary, field='AvgInterPeerCorr'),
            "historical_excess_min_corr_peer_pairs": _format(historical_peer_summary, field='MinInterPeerCorrPairs'),
            "historical_excess_max_corr_peer_pairs": _format(historical_peer_summary, field='MaxInterPeerCorrPairs')
        }
        return excel_data

    def generate_excel_report(self, exp_rf):
        col_order = [
            'Generalist L/S Eqty',
            'Multi-PM',
            'Macro',
            'Div Multi-Strat',
            # 'Quant',
            # 'Relative Value',
            # 'Cross Cap',
            # 'Fdmtl Credit',
            # 'Structured Credit',
            # 'L/S Credit',
            # 'Europe Credit',
            # 'Consumer',
            # 'Energy',
            # 'Financials',
            # 'Healthcare',
            # 'TMT',
            # 'Asia Equity',
            # 'China',
            # 'Europe Eqty'
        ]

        peer_constituent_summaries, historical_peer_summary, peer_corr_mat, arb_assumptions, peer_exposure = \
            self._peer_model.generate_peer_level_assumptions(col_order=col_order, exp_rf=exp_rf)

        col_order = self._peer_bmrk_mapping.set_index('PeerGroupShortName').loc[col_order]['ReportingPeerGroup']
        col_order = col_order.squeeze().tolist()
        excel_data = self.generate_excel_inputs(historical_peer_summary=historical_peer_summary,
                                                peer_corr_mat=peer_corr_mat,
                                                arb_assumptions=arb_assumptions,
                                                peer_exposure=peer_exposure,
                                                col_order=col_order)

        with Scenario(as_of_date=self._as_of_date).context():
            InvestmentsReportRunner().execute(
                data=excel_data,
                template="ARS_Fund_Distribution_Model_Diagnostics_Template.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.cross_entity,
                entity_name='ARS',
                entity_display_name='ARS',
                entity_ids='',
                report_name="ARS Fund Distribution Model Diagnostics",
                report_type=ReportType.Performance,
                report_vertical=ReportVertical.ARS,
                report_frequency="Monthly",
                output_dir="cleansed/investmentsreporting/printedexcels/",
                report_output_source=DaoSource.DataLake,
            )

    def run(self, exp_rf, **kwargs):
        self.generate_excel_report(exp_rf=exp_rf)
        return 'Complete'


if __name__ == "__main__":
    dao_runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "dev",
                    "Subscription": "nonprd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        },
    )

    with Scenario(dao=dao_runner, as_of_date=dt.date(2022, 12, 31)).context():
        ArsPeerAssumptionDiagnosticReport().execute(exp_rf=0.03)
