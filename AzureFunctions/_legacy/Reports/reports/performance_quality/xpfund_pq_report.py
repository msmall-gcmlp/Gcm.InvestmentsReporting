import datetime as dt
import json
import pandas as pd
from gcm.Dao.DaoRunner import DaoRunnerConfigArgs, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.scenario import Scenario
from gcm.inv.dataprovider.investment_group import InvestmentGroup


def _download_inputs(runner, dl_location, file_path) -> dict:
    try:
        read_params = AzureDataLakeDao.create_get_data_params(
            dl_location,
            file_path,
            retry=False,
        )
        file = runner.execute(
            params=read_params,
            source=DaoSource.DataLake,
            operation=lambda dao, params: dao.get_data(read_params),
        )
        inputs = json.loads(file.content)
    except:
        inputs = None
    return inputs


def _parse_json(fund_data, item):
    return pd.read_json(fund_data[item], orient="index")


def _peer_file_path(peer_name, as_of_date):
    return peer_name.replace("/", "") + as_of_date.strftime("%Y-%m-%d") + ".json"


def _filter_summary(json_data, named_range, fund_name):
    summary = _parse_json(json_data, named_range)
    summary['FundName'] = fund_name
    return summary


def _fund_file_path(fund_name, as_of_date):
    return fund_name.replace("/", "") + "_fund_" + as_of_date.strftime("%Y-%m-%d") + ".json"


def _filter_fund_set(inv_group_ids=None):
    if inv_group_ids is None:
        fund_dimn = InvestmentGroup(investment_group_ids=None).get_dimensions(
            exclude_gcm_portfolios=True,
            include_filters=dict(status=["EMM"]),
            exclude_filters=dict(strategy=["Other", "Aggregated Prior Period Adjustment",
                                           "Uninvested"]),
        )
    else:
        fund_dimn = InvestmentGroup(investment_group_ids=inv_group_ids).get_dimensions()
    return fund_dimn


def _subset_fund_dimn(fund_dimn):
    fund_dimn_columns = [
        "InvestmentGroupId",
        "PubInvestmentGroupId",
        "InvestmentGroupName",
        "AbsoluteBenchmarkId",
        "AbsoluteBenchmarkName",
        "EurekahedgeBenchmark",
        "InceptionDate",
        "InvestmentStatus",
        "ReportingPeerGroup",
        "StrategyPeerGroup",
        "Strategy",
        "SubStrategy",
        "FleScl",
    ]

    fund_dimn = fund_dimn.reindex(columns=fund_dimn_columns, fill_value=None)
    return fund_dimn


def _get_fund_dimn(as_of_date, inv_group_ids=None):
    funds = _subset_fund_dimn(fund_dimn=_filter_fund_set(inv_group_ids=inv_group_ids))
    ig = InvestmentGroup(investment_group_ids=funds['InvestmentGroupId'].tolist())
    allocs = ig.get_firmwide_allocation(start_date=as_of_date, end_date=as_of_date)

    funds = funds.merge(allocs[['InvestmentGroupName', 'EndingBalance']], on='InvestmentGroupName', how='left')
    funds = funds[['InvestmentGroupName', 'EurekahedgeBenchmark', 'ReportingPeerGroup', 'EndingBalance']]
    funds.rename(columns={'EndingBalance': 'FirmwideAllocation'}, inplace=True)
    funds['FirmwideAllocation'] = funds['FirmwideAllocation'] / 1_000_000
    funds = funds.set_index('InvestmentGroupName')
    funds = funds.replace('GCM ', '', regex=True).replace('EHI100 ', '', regex=True)
    return funds


def _pivot_and_reindex(data, level_1_cols, level_2_cols):
    col_order = pd.MultiIndex.from_product([level_1_cols, level_2_cols], names=['Field', 'Period'])
    data = data.reset_index().pivot(index='FundName', columns='index')
    data = data.reindex(columns=col_order)
    return data


def _format_benchmark_excess_summary(full_stats):
    data = full_stats['benchmark_summary']
    # Excludes: AbsoluteReturnBenchmark, GcmPeer, EHI50, EHI200
    cols = ['Fund',
            'AbsoluteReturnBenchmarkExcess',
            'GcmPeerExcess',
            'EHI50Excess',
            'EHI200Excess']
    periods = ['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y', '10Y', 'ITD']
    formatted_summary = _pivot_and_reindex(data=data,
                                           level_1_cols=cols,
                                           level_2_cols=periods)
    return formatted_summary


def _format_benchmark_ptile_summary(full_stats):
    data = full_stats['benchmark_summary']
    # Excludes: AbsoluteReturnBenchmark, GcmPeer, EHI50, EHI200
    cols = ['Peer1Ptile',
            'Peer2Ptile',
            'EH50Ptile',
            'EHI200Ptile']
    periods = ['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y', '10Y']
    formatted_summary = _pivot_and_reindex(data=data,
                                           level_1_cols=cols,
                                           level_2_cols=periods)
    return formatted_summary


def _format_perf_stability_summary(full_stats):
    data = full_stats['performance_stability_fund_summary']
    # Excluded cols: Return_min, Return_25%, Return_75%, Return_max, Sharpe_min,
    # Sharpe_25%, Sharpe_75%, Sharpe_max
    cols = ['Sharpe',
            'Beta',
            'Vol',
            'BattingAvg',
            'WinLoss'
            ]
    periods = ['TTM', '3Y', '5Y']
    formatted_summary = _pivot_and_reindex(data=data,
                                           level_1_cols=cols,
                                           level_2_cols=periods)
    return formatted_summary


def _format_rba_attribution(full_stats):
    data = full_stats['rba_summary']
    # Excluded cols: SYSTEMATIC, REGION, INDUSTRY, X_ASSET_CLASS_EXCLUDED, LS_EQUITY, LS_CREDIT, MACRO
    cols = ['NON_FACTOR_SECURITY_SELECTION',
            'NON_FACTOR_OUTLIER_EFFECTS']
    periods = ['MTD', 'QTD', 'YTD', 'TTM', '3Y', '5Y']
    formatted_summary = _pivot_and_reindex(data=data,
                                           level_1_cols=cols,
                                           level_2_cols=periods)
    return formatted_summary


def _format_rba_risk_decomp(full_stats):
    data = full_stats['rba_summary']
    cols = ['SYSTEMATIC_RISK',
            'X_ASSET_RISK',
            'PUBLIC_LS_RISK',
            'NON_FACTOR_RISK']
    periods = ['TTM', '3Y', '5Y']
    formatted_summary = _pivot_and_reindex(data=data,
                                           level_1_cols=cols,
                                           level_2_cols=periods)
    return formatted_summary


def _format_pba_summary(full_stats):
    data = pd.concat([full_stats['pba_mtd'], full_stats['pba_qtd'], full_stats['pba_ytd']])
    # Excluded cols: Beta, Regional, Industry, Repay, LS_Equity, LS_Credit, MacroRV, Fees, Unallocated
    cols = ['Residual']
    periods = ['MTD - Publics',
               'QTD - Publics',
               'YTD - Publics',
               'MTD - Privates',
               'QTD - Privates',
               'YTD - Privates']

    formatted_summary = _pivot_and_reindex(data=data,
                                           level_1_cols=cols,
                                           level_2_cols=periods)
    return formatted_summary


def _format_shortfall_summary(full_stats):
    data = full_stats['shortfall_summary']
    cols = ['Trigger', 'Drawdown', 'Pass/Fail']
    data = data.set_index('FundName')
    formatted_summary = data[cols]
    return formatted_summary


def _format_risk_model_expectations(full_stats):
    data = full_stats['risk_model_expectations']
    cols = ['ExpectedReturn', 'ExpectedVolatility']
    data = data.reset_index().pivot(index='FundName', columns='index')
    data.columns = data.columns.droplevel(0)
    formatted_summary = data[cols]
    return formatted_summary


def _format_exposure_summary(full_stats):
    summary = full_stats['exposure_summary']
    periods = ['Latest', '3Y', '5Y', '10Y']

    summaries = pd.DataFrame()
    for period in periods:
        period_summary = summary.loc[period].set_index('FundName')
        summaries = pd.concat([summaries, period_summary], axis=1)

    summaries.columns = pd.MultiIndex.from_product([periods, period_summary.columns],
                                                   names=['Field', 'Period'])
    formatted_summary = summaries * 0.01

    return formatted_summary


def _generate_final_summary(emm_dimn, pq_stats, peer_rankings, peer_rankings_lag):
    benchmark_excess = _format_benchmark_excess_summary(full_stats=pq_stats)
    benchmark_ptile = _format_benchmark_ptile_summary(full_stats=pq_stats)
    perf_stability = _format_perf_stability_summary(full_stats=pq_stats)
    rba_idios = _format_rba_attribution(full_stats=pq_stats)
    rba_risk = _format_rba_risk_decomp(full_stats=pq_stats)
    pba = _format_pba_summary(full_stats=pq_stats)
    shortfall = _format_shortfall_summary(full_stats=pq_stats)
    expectations = _format_risk_model_expectations(full_stats=pq_stats)
    exposure = _format_exposure_summary(full_stats=pq_stats)

    final_summary = pd.concat([benchmark_excess,
                               benchmark_ptile,
                               perf_stability,
                               rba_idios,
                               rba_risk,
                               pba,
                               shortfall,
                               expectations,
                               exposure], axis=1)

    final_summary = emm_dimn.merge(final_summary, left_index=True, right_index=True, how='left')
    final_summary = final_summary.merge(peer_rankings, left_index=True, right_index=True, how='left')
    final_summary = final_summary.merge(peer_rankings_lag, left_index=True, right_index=True, how='left')
    final_summary = final_summary.reset_index()
    return final_summary


def _get_peer_rankings(runner, as_of_date, emm_dimn):
    peer_screen_location = "raw/investmentsreporting/summarydata/ars_performance_screener"
    peers = sorted([x for x in emm_dimn['ReportingPeerGroup'].unique().tolist() if str(x) != 'nan' and x is not None])
    peer_rankings = pd.DataFrame(columns=['InvestmentGroupNameRaw', 'Peer', 'Decile', 'Confidence', 'Persistence'])
    for peer in peers:
        print(peer)
        peer_ranks = _download_inputs(runner=runner,
                                      dl_location=peer_screen_location,
                                      file_path=_peer_file_path('GCM ' + peer, as_of_date))
        if peer_ranks is not None:
            peer_ranks = _parse_json(peer_ranks, "summary_table")
            peer_ranks = peer_ranks[['InvestmentGroupNameRaw', 'Decile', 'Confidence', 'Persistence']]
            peer_ranks['Peer'] = peer
            peer_rankings = pd.concat([peer_rankings, peer_ranks])

    emm = emm_dimn.reset_index()
    rankings = emm.merge(peer_rankings, left_on='InvestmentGroupName', right_on='InvestmentGroupNameRaw', how='left')
    rankings = rankings[rankings['Peer'] == rankings['ReportingPeerGroup']]
    rankings = rankings[['InvestmentGroupName', 'Decile', 'Confidence', 'Persistence']]
    rankings = rankings.set_index('InvestmentGroupName')

    return rankings


def _get_performance_quality_metrics(runner, emm_dimn, as_of_date):
    stats = {}
    pq_location = "raw/investmentsreporting/summarydata/performancequality"
    for fund_name in emm_dimn.index:
        fund_data = _download_inputs(runner=runner,
                                     dl_location=pq_location,
                                     file_path=_fund_file_path(fund_name, as_of_date))
        if fund_data is not None:
            for named_range in ['benchmark_summary', 'performance_stability_fund_summary',
                                'rba_summary', 'pba_mtd', 'pba_qtd', 'pba_ytd', 'shortfall_summary',
                                'risk_model_expectations', 'exposure_summary']:
                summary = _filter_summary(json_data=fund_data, named_range=named_range, fund_name=fund_name)
                if named_range in stats.keys():
                    stats[named_range] = pd.concat([stats[named_range], summary])
                else:
                    stats[named_range] = summary
    return stats


def generate_xpfund_pq_report_data(runner: DaoRunner, date: dt.date, inv_group_ids=None):
    with Scenario(dao=runner, as_of_date=date).context():
        fund_dimn = _get_fund_dimn(as_of_date=date, inv_group_ids=inv_group_ids)

        date_q_minus_1 = pd.to_datetime(date - pd.tseries.offsets.QuarterEnd(1)).date()
        peer_rankings = _get_peer_rankings(runner=runner, as_of_date=date_q_minus_1, emm_dimn=fund_dimn)
        date_q_minus_2 = pd.to_datetime(date - pd.tseries.offsets.QuarterEnd(2)).date()
        peer_rankings_lag = _get_peer_rankings(runner=runner, as_of_date=date_q_minus_2, emm_dimn=fund_dimn)
        pq_stats = _get_performance_quality_metrics(runner=runner, emm_dimn=fund_dimn, as_of_date=date)
        final_summary = _generate_final_summary(emm_dimn=fund_dimn,
                                                pq_stats=pq_stats,
                                                peer_rankings=peer_rankings,
                                                peer_rankings_lag=peer_rankings_lag)
    return final_summary


if __name__ == "__main__":
    dao_runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.DataLake.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                },
            }
        },
    )
    date = dt.date(2023, 4, 30)
    # inv_group_ids = [19717, 20292, 20319, 31378, 89745, 43058, 51810, 86478, 87478, 89809] <- ESG Prd Ids
    inv_group_ids = None
    report_data = generate_xpfund_pq_report_data(runner=dao_runner,
                                                 date=date,
                                                 inv_group_ids=inv_group_ids)
