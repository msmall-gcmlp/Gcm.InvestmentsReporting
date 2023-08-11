import datetime as dt
import json
import pandas as pd
from gcm.Dao.DaoRunner import DaoRunnerConfigArgs, DaoRunner
from gcm.Dao.DaoSources import DaoSource
from gcm.Dao.daos.azure_datalake.azure_datalake_dao import AzureDataLakeDao
from gcm.inv.scenario import Scenario


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


def _filter_summary(json_data, named_range, portfolio, scenario_name):
    summary = _parse_json(json_data, named_range)
    summary['Portfolio'] = portfolio
    summary['Scenario'] = scenario_name
    return summary


def _portfolio_file_path(acronym, scenario_name, as_of_date):
    return acronym.replace("/", "") + "_" + scenario_name + "_" + as_of_date.strftime("%Y-%m-%d") + ".json"


def _pivot_and_reindex(data, level_1_cols, level_2_cols):
    col_order = pd.MultiIndex.from_product([level_1_cols, level_2_cols], names=['Field', 'Period'])
    data = data.reset_index().pivot(index=['Portfolio', 'Scenario'], columns='index')
    data = data.reindex(columns=col_order)
    return data


def _format_liquidity_summary(full_stats):
    data = full_stats['liquidity']
    # Excludes: AbsoluteReturnBenchmark, GcmPeer, EHI50, EHI200
    cols = list(set(data.columns) - set(['Portfolio', 'Scenario']))
    periods = data.index.unique()
    formatted_summary = _pivot_and_reindex(data=data,
                                           level_1_cols=cols,
                                           level_2_cols=periods)
    return formatted_summary


def _get_metrics(runner, acronyms, scenario_name, as_of_date):
    stats = {}
    pq_location = "raw/investmentsreporting/summarydata/portfolioconstruction"
    for acronym in acronyms:
        data = _download_inputs(runner=runner,
                                dl_location=pq_location,
                                file_path=_portfolio_file_path(acronym, scenario_name, as_of_date))
        if data is not None:
            for named_range in ['liquidity', 'LiquidityConstraint_Informal']:
                summary = _filter_summary(json_data=data, named_range=named_range, portfolio=acronym,
                                          scenario_name=scenario_name)
                if named_range in stats.keys():
                    stats[named_range] = pd.concat([stats[named_range], summary])
                else:
                    stats[named_range] = summary
    return stats


def generate_xportfolio_pc_report_data(runner: DaoRunner, as_of_date: dt.date, acronyms, scenario_name):
    with Scenario(dao=runner, as_of_date=date).context():
        metrics = _get_metrics(runner=runner, acronyms=acronyms, scenario_name=scenario_name, as_of_date=as_of_date)
        liquidity = metrics['liquidity']
        liquidity['CurrentMinusOptimal'] = liquidity['Current'] - liquidity['LongTermOptimal']
        liquidity = liquidity.reset_index()
        liquidity = liquidity.merge(metrics['LiquidityConstraint_Informal'], left_on=['Portfolio', 'Scenario'],
                                    right_on=['Portfolio', 'Scenario'], how='left')
        liquidity.columns = ['Freq'] + liquidity.columns[1:-1].tolist() + ['Constraint']
        liquidity = liquidity.sort_values('CurrentMinusOptimal', ascending=True)

        # for reference if ever creating large dump like XPFUND PQ
        formatted_summary = _format_liquidity_summary(metrics)
        formatted_summary = formatted_summary[['LongTermOptimal', 'Current', 'CurrentMinusOptimal']]
        formatted_summary['MinDiff'] = formatted_summary['CurrentMinusOptimal'].min(axis=1)
        formatted_summary = formatted_summary.sort_values('MinDiff', ascending=True)
        constraints = metrics['LiquidityConstraint_Informal'].set_index(['Portfolio', 'Scenario'])
        formatted_summary = pd.concat([formatted_summary, constraints], axis=1)

    return formatted_summary


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
    date = dt.date(2023, 7, 1)
    acronyms = ['AAH', 'AIF1', 'ALPHAOPP', 'ANCHOR4', 'ANCHOR4C', 'AOF', 'ASUTY', 'BALCERA', 'BH2', 'BUCKEYE',
                'BUTTER', 'BUTTERB', 'CARMEL', 'CASCADE', 'CHARLES2', 'CHARTEROAK', 'CICFOREST', 'CLOVER2', 'CMFL',
                'CORKTOWN', 'CPA', 'CPAT', 'CTOM', 'ELAR', 'FALCON', 'FARIA', 'FATHUNTER', 'FOB', 'FTPAT', 'GAIC',
                'GARS-A', 'GBMF', 'GCM SELECT', 'GIP', 'GJFF', 'GJFF-B', 'GMMUT', 'GMSF', 'GMSUT3YD', 'GMSUTVY',
                'GMSUTVYC', 'GOATMF', 'GOLDEN', 'GOMCFLP', 'GOMSF', 'GRMSMF', 'GROVE', 'HFGPS', 'HFGPSOFF',
                'IF', 'IFC', 'IFCC', 'IFCH', 'IFH', 'JDPTCONS', 'JJP', 'JJP-B', 'JPASA', 'LOTUSC', 'LUPINEB', 'MACRO',
                'MAY14', 'MFIA', 'MMUT', 'MPAC', 'OCFV', 'PSUTY', 'RAVEN1', 'RAVEN6', 'ROGUE', 'SCARFD', 'SF',
                'SINGULAR', 'SMART', 'SOLAR', 'SPECTRUM', 'STAR', 'TEKTON', 'TITANIUM',
                'WILMORE', 'WINDANDSEA']
    report_data = generate_xportfolio_pc_report_data(runner=dao_runner,
                                                     as_of_date=date,
                                                     acronyms=acronyms,
                                                     scenario_name="Default")
    report_data.to_csv('liquidity_terms_vetting.csv')
