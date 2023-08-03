import datetime as dt
import numpy as np
import pandas as pd
from gcm.inv.models.portfolio_construction.portfolio_metrics.portfolio_metrics import collect_portfolio_metrics, \
    _process_optimization_inputs
from _legacy.Reports.reports.portfolio_construction.portfolio_construction_report_data import _RawData


def _format_weights(allocations, strategies, capacity_status, conditional_returns, stress_names):
    def _create_strategy_block(weights, strategy, stress_names):
        wts_g = weights[weights['Strategy'] == strategy]
        wts_g.drop(columns={'Strategy'}, inplace=True)

        summable_fields = ['Fund', 'Current', 'ShortTermOptimal', 'LongTermOptimal',
                           'Current - %Risk', 'ShortTermOptimal - %Risk',
                           'LongTermOptimal - %Risk'] + stress_names

        allocations = wts_g[summable_fields].sum().to_frame().T

        condl_current = wts_g['Current'] @ wts_g[['Down_Current', 'Base_Current', 'Up_Current']]

        if sum(wts_g['Current']) > 0:
            condl_current = condl_current / sum(wts_g['Current'])

        condl_current = condl_current.to_frame().T.reset_index(drop=True)
        condl_current = condl_current.fillna(0)
        condl_lt = wts_g['LongTermOptimal'] @ wts_g[['Down_LT', 'Base_LT', 'Up_LT']] / sum(wts_g['LongTermOptimal'])
        condl_lt = condl_lt.fillna(0)
        condl_lt = condl_lt.to_frame().T.reset_index(drop=True)
        strategy_g = pd.concat([allocations, condl_current, condl_lt], axis=1)
        strategy_g['Fund'] = strategy
        spacer_row = strategy_g.copy()
        spacer_row[0:] = None
        return pd.concat([strategy_g, wts_g, spacer_row], axis=0)

    wts = allocations * 100
    wts = wts.reset_index()
    wts_formatted = pd.DataFrame(columns=wts.columns.tolist())

    wts = wts.merge(strategies)

    groups = sorted(wts['Strategy'].unique())

    wts = wts.merge(conditional_returns * 100, left_on='Fund', right_index=True)

    for g in groups:
        wts_formatted = pd.concat([wts_formatted, _create_strategy_block(weights=wts,
                                                                         strategy=g,
                                                                         stress_names=stress_names)], axis=0)

    wts.drop(columns={'Strategy'}, inplace=True)

    totals = wts.sum()

    unallocated_current_rtn = wts['Current'] @ wts[['Down_Current', 'Base_Current', 'Up_Current']]

    if sum(wts['Current']) > 0:
        unallocated_current_rtn = unallocated_current_rtn / sum(wts['Current'])

    totals.loc[unallocated_current_rtn.index] = unallocated_current_rtn
    unallocated_lt_rtn = wts['LongTermOptimal'] @ wts[['Down_LT', 'Base_LT', 'Up_LT']] / sum(wts['LongTermOptimal'])
    totals.loc[unallocated_lt_rtn.index] = unallocated_lt_rtn

    unallocated_row = totals.copy()
    unallocated_row['Fund'] = 'Cash & Other'
    weight_columns = ['Current', 'ShortTermOptimal', 'LongTermOptimal']
    unallocated_row[weight_columns] = (100 - totals[weight_columns])
    unallocated_row[list(set(unallocated_row.index) - set(['Fund'] + weight_columns))] = 0
    unallocated_row = unallocated_row.to_frame().T

    total_row = totals.copy()
    total_row['Fund'] = 'Total'
    total_row[weight_columns] = 100
    total_row = total_row.to_frame().T

    wts_formatted = pd.concat([wts_formatted, unallocated_row, total_row], axis=0)

    wts_formatted = wts_formatted.merge(capacity_status, how='left', on='Fund')
    is_cc = wts_formatted['Capacity'] < 1
    wts_formatted.loc[is_cc, 'Fund'] = wts_formatted.loc[is_cc, 'Fund'] + ' *'
    is_closed = wts_formatted['Capacity'] == 0
    wts_formatted.loc[is_closed, 'Fund'] = wts_formatted.loc[is_closed, 'Fund'] + '*'
    wts_formatted.drop(columns={"Capacity"}, inplace=True)

    wts_formatted.iloc[:, 1:] = wts_formatted.iloc[:, 1:].astype(float).round(0)
    wts_formatted['Delta_LT'] = wts_formatted['LongTermOptimal'] - wts_formatted['Current']
    wts_formatted['Delta_ST'] = wts_formatted['ShortTermOptimal'] - wts_formatted['Current']
    wts_formatted['Delta_LT - %Risk'] = wts_formatted['LongTermOptimal - %Risk'] - wts_formatted[
        'Current - %Risk']
    wts_formatted['Delta_ST - %Risk'] = wts_formatted['ShortTermOptimal - %Risk'] - wts_formatted[
        'Current - %Risk']
    wt_column_order = ['Current',
                       'ShortTermOptimal',
                       'Delta_ST',
                       'LongTermOptimal',
                       'Delta_LT']
    wt_column_order = ['Fund'] + wt_column_order + [x + ' - %Risk' for x in wt_column_order]
    column_order = wt_column_order + conditional_returns.columns.tolist() + stress_names
    wts_formatted = wts_formatted[column_order]

    return wts_formatted


def _format_allocations_summary(weights, metrics, stress_contribs):
    weights = pd.concat([weights, stress_contribs], axis=1)
    weights = weights.sort_index()
    strategies = metrics['fund_strategies']
    strategies['Strategy'] = strategies['Current'].combine_first(strategies['LongTermOptimal'])
    strategies['Strategy'] = strategies.Strategy.combine_first(strategies.ShortTermOptimal)
    strategies = strategies.reset_index().rename(columns={'index': 'Fund'})[['Fund', 'Strategy']]

    capacity = metrics['fund_capacities']
    capacity['Capacity'] = capacity['Current'].combine_first(capacity['LongTermOptimal'])
    capacity['Capacity'] = capacity['Capacity'].combine_first(capacity['ShortTermOptimal'])
    capacity = capacity.reset_index().rename(columns={'index': 'Fund'})[['Fund', 'Capacity']]

    metrics['risk_allocations'].columns = metrics['risk_allocations'].columns + ' - %Risk'
    combined_weights = weights.merge(metrics['risk_allocations'], how='outer', left_index=True, right_index=True)
    combined_weights = combined_weights.fillna(0)

    condl_current = metrics['conditional_current_returns']
    condl_lt = metrics['conditional_long_term_returns']
    condl_current.columns = condl_current.columns + '_Current'
    condl_lt.columns = condl_lt.columns + '_LT'
    conditional_returns = pd.concat([condl_current, condl_lt], axis=1)

    stress_names = stress_contribs.columns.tolist()
    formatted_weights = _format_weights(allocations=combined_weights,
                                        strategies=strategies,
                                        capacity_status=capacity,
                                        conditional_returns=conditional_returns,
                                        stress_names=stress_names)

    dollar_weights = formatted_weights[['Fund',
                                        'Current',
                                        'ShortTermOptimal',
                                        'Delta_ST',
                                        'LongTermOptimal',
                                        'Delta_LT',
                                        ]]

    risk_columns = ['Fund', 'Current - %Risk', 'ShortTermOptimal - %Risk', 'LongTermOptimal - %Risk',
                    'Current_LongRunSelloff', 'ShortTermOptimal_LongRunSelloff', 'LongTermOptimal_LongRunSelloff',
                    'Current_CreditLiquidityTechnicals', 'ShortTermOptimal_CreditLiquidityTechnicals', 'LongTermOptimal_CreditLiquidityTechnicals',
                    'Current_GrowthSelloff', 'ShortTermOptimal_GrowthSelloff', 'LongTermOptimal_GrowthSelloff',
                    'Current_HFDelever', 'ShortTermOptimal_HFDelever', 'LongTermOptimal_HFDelever',
                    'Current_GrossExp', 'ShortTermOptimal_GrossExp', 'LongTermOptimal_GrossExp']

    risk_weights = formatted_weights[risk_columns]
    is_empty = risk_weights.iloc[:, 1:].apply(lambda x: x.abs().max() == 0)
    is_empty = [False] + is_empty.values.tolist()
    risk_weights.loc[:, is_empty] = np.nan

    conditional_returns_summary = formatted_weights[['Fund',
                                                     'Current',
                                                     'ShortTermOptimal',
                                                     'LongTermOptimal',
                                                     'Down_Current',
                                                     'Base_Current',
                                                     'Up_Current',
                                                     'Down_LT',
                                                     'Base_LT',
                                                     'Up_LT'
                                                     ]]

    is_empty = conditional_returns_summary.iloc[:, 1:].apply(lambda x: x.abs().max() == 0)
    is_empty = [False] + is_empty.values.tolist()
    conditional_returns_summary.loc[:, is_empty] = np.nan

    weights = {"dollar_allocation_summary": dollar_weights,
               "risk_allocation_summary": risk_weights,
               "conditional_return_summary": conditional_returns_summary}

    return weights


def _combine_metrics_across_weights(weights, optim_inputs, rf):
    portfolio_metrics = {k: collect_portfolio_metrics(
        weights=weights[k],
        optim_inputs=optim_inputs,
        rf=rf) for k in weights.columns
    }

    metrics = {key: None for key in [f.name for f in portfolio_metrics['Current'].metrics.__attrs_attrs__]}
    metric_names = list(metrics.keys())
    metric_names = set(metric_names) - set(['conditional_manager_returns'])
    for m in metric_names:
        metrics[m] = pd.concat([getattr(portfolio_metrics['Current'].metrics, m),
                                getattr(portfolio_metrics['ShortTermOptimal'].metrics, m),
                                getattr(portfolio_metrics['LongTermOptimal'].metrics, m)], axis=1)
        metrics[m].columns = weights.columns

    strategy_order = pd.DataFrame(index=['Credit', 'Long/Short Equity', 'Macro', 'Multi-Strategy', 'Quantitative',
                                         'Relative Value'])
    strategy_weights = strategy_order.merge(metrics['strategy_weights'],
                                            left_index=True, right_index=True,
                                            how='left')
    cash_and_other = pd.DataFrame(1 - strategy_weights.sum()).T
    cash_and_other.index = ['Cash and Other']
    metrics['strategy_weights'] = pd.concat([strategy_weights, cash_and_other], axis=0)

    metrics["distribution_of_returns"] = metrics.pop('outcomes_distribution')
    metrics["exp_risk_adj_performance"] = metrics.pop('risk_adj_performance')
    metrics["strategy_allocation"] = metrics.pop('strategy_weights')

    metrics['conditional_current_returns'] = getattr(portfolio_metrics['Current'].metrics,
                                                     'conditional_manager_returns')

    metrics['conditional_long_term_returns'] = getattr(portfolio_metrics['LongTermOptimal'].metrics,
                                                       'conditional_manager_returns')

    return metrics


def _format_header_info(acronym, scenario_name, as_of_date):
    header_info = pd.DataFrame(
        {
            "header_info": [
                acronym,
                scenario_name,
                as_of_date,
            ]
        }
    )
    header_info = {"header_info": header_info}
    return header_info


def _summarize_liquidity_cons(obs_cons):
    liq_class = obs_cons.liquidityConstraints
    liq_freq = list(obs_cons.liquidityConstraints.__annotations__.keys())
    liquidity = {x: liq_class.__getattribute__(x) for x in liq_freq if liq_class.__getattribute__(x) is not None}
    liquidity.pop('illiquid', None)
    liquidity.pop('other', None)
    liq_display_names = {'daily': 'D',
                         'monthly': 'M',
                         'quarterly': 'Q',
                         'semiannual': 'SA',
                         'annual': 'A',
                         'eighteenMonths': '18M',
                         'twoYears': '2Y',
                         'threeYears': '3Y',
                         'greaterThanThreeYears': '4Y'}
    liquidity = {liq_display_names[k]: liquidity[k] for k in liquidity}
    liquidity = ' | '.join("{:.0f}".format(100 * y) + '% ' + str(x) for x, y in liquidity.items())
    return liquidity


def _summarize_stress_limits(obs_cons):
    scenarios = obs_cons.stress_scenarios
    n_scenarios = range(len(scenarios))
    beta = [scenarios[x].limit for x in n_scenarios if scenarios[x].name == 'LongRunSelloff']
    max_mkt_exposure = beta[0] / -0.15

    credit_lim = [scenarios[x].limit for x in n_scenarios if scenarios[x].name == 'CreditLiquidityTechnicals']
    credit_lim = credit_lim[0]
    return max_mkt_exposure, credit_lim


def _summarize_position_limits(obs_cons):
    stress_limits = pd.DataFrame(
        {
            "stress_limits": [
                0.03,
                obs_cons.maxLeverage if obs_cons.maxLeverage is not None else "Unconstrained",
                obs_cons.minMaterialNonZero
            ]
        },
        index=['SingleName', 'MaxGrossExposure', 'MinMaterialAllocation']
    )
    return stress_limits


def _format_config_attributes(weights, optim_inputs, rf):
    metrics = collect_portfolio_metrics(weights=weights['Current'],
                                        optim_inputs=optim_inputs,
                                        rf=rf)
    obs_cons = metrics.obs_cons
    target_excess = 10_000 * (obs_cons.target_return - rf)
    target_excess = "{:.0f}".format(target_excess) + 'bps over risk-free'
    liquidity = _summarize_liquidity_cons(obs_cons)
    max_mkt_exposure, credit_lim = _summarize_stress_limits(obs_cons)

    obs_cons_summary = dict({
        "TargetReturn": pd.DataFrame({obs_cons.target_return}),
        "TargetExcessReturn": pd.DataFrame({target_excess}),
        "MaxMktExposure": pd.DataFrame({max_mkt_exposure}),
        "LiquidityConstraint_Informal": pd.DataFrame({liquidity}),
        "MaxFundCount": pd.DataFrame({obs_cons.maxFundCount}),
        "MinAcceptableAllocation": pd.DataFrame({obs_cons.minMaterialNonZero}),
        "CreditLiquidityTechnicalsLimit": pd.DataFrame({credit_lim}),
        "ReallocateScarceCapacity": pd.DataFrame({"No"}),
        "GrowthSelloffLimit": pd.DataFrame({-0.03})
    })

    return obs_cons_summary


def _format_non_config_attributes(reference_attributes):
    reference_attributes = dict(zip(reference_attributes['Field'], reference_attributes['Value']))

    # format fees for display
    fee_items = ['MgmtFee', 'IncentiveFee', 'HurdleRate']
    fees = {key: str(round(100 * reference_attributes[key], 2)) + '%' for key in fee_items}
    reference_attributes['Fees'] = fees['MgmtFee'] + ' | ' + fees['IncentiveFee'] + ' | ' + fees['HurdleRate']

    # format beta for display
    beta = reference_attributes.get('MaxBeta')
    if beta is None:
        reference_attributes["FormalBetaConstraint"] = "--"
    else:
        beta = "{:.2f}".format(reference_attributes['MaxBeta'])
        bmrk = reference_attributes.get('BetaBenchmark', 'Market')
        reference_attributes["FormalBetaConstraint"] = beta + ' vs ' + bmrk

    formal_attributes = ['ClientName',
                         'StrategyFocus',
                         'FirstPrincipal',
                         'FirstAnalyst',
                         'Fees',
                         'MaxLineOfCreditAmount',
                         'ReturnObjective',
                         'VolatilityConstraint',
                         'FormalBetaConstraint',
                         'LiquidityConstraint',
                         'MaxSLFAllocation_Formal',
                         'MaxAllocationSingleFund_Formal',
                         'MaxAllocationSingleManager',
                         'MaxFundCount_Formal',
                         'MaxLeverageVolUnadj',
                         'MaxSclPositionImpact',
                         'MaxDesignatedAmount',
                         'MaxSLFAllocation',
                         'MaxAllocationSingleFund',
                         'MaxDrawdownConstraint',
                         'DrawdownFundsEligible']
    formal_attribute_subset = {key: pd.DataFrame({reference_attributes.get(key)}) for key in formal_attributes}

    for k, v in formal_attribute_subset.items():
        if v.squeeze() is None:
            formal_attribute_subset[k] = pd.DataFrame({"--"})
    return formal_attribute_subset


def _nullify_data_for_zero_weights(weights, metrics):
    cash = weights['dollar_allocation_summary'].loc[weights['dollar_allocation_summary']['Fund'] == 'Cash & Other']
    zero_out = cash[['Current', 'ShortTermOptimal', 'Delta_ST', 'LongTermOptimal', 'Delta_LT']].abs() == 100
    zero_out_fields = zero_out.T.loc[zero_out.T.values].index

    for field in zero_out_fields.tolist():
        for m in metrics.keys():
            metrics[m].loc[:, field] = np.nan
        for w in weights.keys():
            weights[w].loc[:, field] = np.nan
    return weights, metrics


def _get_fund_lookthrough_strategy_allocations(optim_inputs, multi_strat_lookthrough):
    strategies = optim_inputs.fundInputs.fundData.strategies
    multi_strat_lookthrough = multi_strat_lookthrough.melt(id_vars='InvestmentGroupName',
                                                           value_name='LookthruWeight',
                                                           var_name='Strategy')
    single_strats = strategies[~strategies.index.isin(multi_strat_lookthrough['InvestmentGroupName'])]
    single_strats = single_strats.reset_index().rename(columns={'Fund': 'InvestmentGroupName'})
    single_strats['LookthruWeight'] = 1

    lookthru_strats = pd.concat([multi_strat_lookthrough, single_strats], axis=0)
    return lookthru_strats


def _get_strategy_weights_with_lookthrough(optim_inputs, multi_strat_lookthrough, weights):
    lookthru_strats = _get_fund_lookthrough_strategy_allocations(optim_inputs=optim_inputs,
                                                                 multi_strat_lookthrough=multi_strat_lookthrough)

    weights = weights * 100
    weights = weights.merge(lookthru_strats, left_index=True, right_on='InvestmentGroupName')
    lookthru_contribs = weights[['Current', 'ShortTermOptimal', 'LongTermOptimal']].multiply(
        weights["LookthruWeight"],
        axis="index")
    lookthru_contribs['Strategy'] = weights['Strategy']
    lookthru_contribs.loc[lookthru_contribs['Strategy'] == 'Multi-Strategy', 'Strategy'] = 'Other'
    strat_wts_lookthru = lookthru_contribs.groupby('Strategy').sum()
    strat_wts_lookthru['Delta_ST'] = strat_wts_lookthru['ShortTermOptimal'] - strat_wts_lookthru['Current']
    strat_wts_lookthru['Delta_LT'] = strat_wts_lookthru['LongTermOptimal'] - strat_wts_lookthru['Current']
    strat_wts_lookthru = strat_wts_lookthru.reset_index()
    column_order = ['Strategy', 'Current', 'ShortTermOptimal', 'Delta_ST', 'LongTermOptimal', 'Delta_LT']
    strat_wts_lookthru = strat_wts_lookthru[column_order].sort_values('Current', ascending=False)
    return strat_wts_lookthru


def _get_stress_contributions(weights, stresses):
    stresses = stresses.set_index('InvestmentGroupName').loc[weights.index]
    stresses['GrossExp'] = stresses['GrossExp'] / 100
    contribs = pd.DataFrame(index=weights.index)
    for s in stresses.columns:
        contrib_s = 100 * (weights.T * stresses[s]).T
        contrib_s.columns = contrib_s.columns + '_' + s
        contribs = pd.concat([contribs, contrib_s], axis=1)
    return contribs


def _get_fund_expectations(optim_inputs):
    expectations = pd.DataFrame({
        "Return": optim_inputs.fundInputs.fundData.correlatedSimulations.mean(),
        "Vol": optim_inputs.fundInputs.fundData.expectedVols
    })
    rf = optim_inputs.config.expRf
    expectations['Sharpe'] = (expectations['Return'] - rf) / expectations['Vol']

    eq_beta = optim_inputs.fundInputs.fundData.fundStresses['LongRunSelloff'] / -0.15
    eq_return = 0.055 + rf
    arb_return = (eq_beta * eq_return) + (1 - eq_beta) * rf
    expectations['ExcessReturn'] = expectations['Return'] - arb_return
    expectations = expectations[['Return', 'ExcessReturn', 'Vol', 'Sharpe']]
    return expectations


def _get_fund_expected_outcomes(optim_inputs):
    outcomes = optim_inputs.fundInputs.fundData.correlatedSimulations.apply(np.percentile, q=[25, 50, 75], axis=0)
    outcomes = outcomes.T
    outcomes.columns = ['25th', '50th', '75th']
    return outcomes


def _get_fund_risk_and_exposure(optim_inputs, stresses):
    # fund_stresses = optim_inputs.fundInputs.fundData.fundStresses[['LongRunSelloff', 'CreditLiquidityTechnicals']]
    fund_stresses = stresses.copy()
    fund_stresses = fund_stresses.set_index('InvestmentGroupName')
    fund_stresses = fund_stresses.reindex(optim_inputs.fundInputs.fundData.fundIdios.index)
    return fund_stresses


def _get_fund_other_attributes(optim_inputs, fund_attributes):
    annual_liq = optim_inputs.fundInputs.fundData.liquidityTerms['annual']
    annual_liq.name = 'PctAnnualLiquidity'
    attributes = pd.concat([fund_attributes, annual_liq], axis=1)
    attributes = attributes.reindex(annual_liq.index)
    attributes = attributes[['BenchmarkStrategy',
                             'BenchmarkSubStrategy',
                             'ExcessRtnPeerPtile',
                             'InputsAccuracy',
                             'PctActiveRisk',
                             'PctAnnualLiquidity',
                             'AvailableCapacity']]
    return attributes


def _get_all_fund_roster_inputs(optim_inputs, stresses, fund_attributes):
    fund_expectations = _get_fund_expectations(optim_inputs)
    fund_exp_outcomes = _get_fund_expected_outcomes(optim_inputs)
    fund_stresses = _get_fund_risk_and_exposure(optim_inputs, stresses)
    fund_other_attributes = _get_fund_other_attributes(optim_inputs, fund_attributes)
    fund_roster_inputs = pd.concat([fund_expectations, fund_exp_outcomes, fund_stresses,
                                    fund_other_attributes], axis=1)
    return fund_roster_inputs


def _format_strategy_roster_inputs(optim_inputs, stresses, fund_attributes):
    roster_inputs = _get_all_fund_roster_inputs(optim_inputs, stresses, fund_attributes)
    roster_inputs = roster_inputs.reset_index().rename(columns={'index': 'InvestmentGroupName'})

    front_columns = ['BenchmarkSubStrategy', 'InvestmentGroupName', 'Return', 'ExcessReturn', 'Vol', 'Sharpe',
                     '25th', '50th', '75th']
    back_columns = ['ExcessRtnPeerPtile', 'InputsAccuracy', 'PctActiveRisk',
                    'PctAnnualLiquidity', 'AvailableCapacity']

    credit = roster_inputs[roster_inputs['BenchmarkStrategy'] == 'Credit']
    credit = credit[front_columns + ['LongRunSelloff', 'CreditLiquidityTechnicals'] + back_columns]

    equity = roster_inputs[roster_inputs['BenchmarkStrategy'] == 'Long/Short Equity']
    equity = equity[front_columns + ['LongRunSelloff', 'GrowthSelloff'] + back_columns]

    ds = roster_inputs[~roster_inputs['BenchmarkStrategy'].isin(['Credit', 'Long/Short Equity'])]
    ds = ds[front_columns + ['LongRunSelloff', 'HFDelever'] + back_columns]

    strategy_roster_inputs = {'credit_roster_inputs': credit,
                              'equity_roster_inputs': equity,
                              'ds_roster_inputs': ds}

    for s in strategy_roster_inputs.keys():
        strategy = strategy_roster_inputs.get(s)
        strategy['Priority'] = 1
        headings = pd.DataFrame({'BenchmarkSubStrategy': strategy['BenchmarkSubStrategy'].unique(),
                                 'InvestmentGroupName': strategy['BenchmarkSubStrategy'].unique(),
                                 'Priority': 0})
        strategy = pd.concat([strategy, headings], axis=0)
        strategy = strategy.sort_values(['BenchmarkSubStrategy', 'Priority',
                                         'ExcessReturn', 'InvestmentGroupName'],
                                        ascending=[True, True, False, True])
        strategy.drop(columns={'BenchmarkSubStrategy', 'Priority'}, inplace=True)
        strategy_roster_inputs[s] = strategy

    return strategy_roster_inputs


def _combine_weights_and_strategy_lookthru_weights(weights, strat_weights):
    weights_ex_total = weights[:-1]
    strat_weights.rename(columns={'Strategy': 'Fund'}, inplace=True)
    strat_weights['Fund'] = '   ' + strat_weights['Fund']
    total = weights.tail(1)
    spacer_row = total.copy()
    spacer_row.iloc[0] = None
    heading_row = pd.DataFrame({'Fund': ['With Lookthrough to Multi-Strats']})

    strat_weights = strat_weights[weights_ex_total.columns].reset_index(drop=True)

    summary = pd.concat([weights_ex_total, spacer_row, heading_row, strat_weights, total], axis=0)
    return summary


def generate_excel_report_data(
    acronym: str,
    scenario_name: str,
    as_of_date: dt.date,
    report_data: _RawData,
):
    weights = report_data.weights
    optim_inputs = report_data.optim_inputs
    portfolio_attributes = report_data.adhoc_portfolio_attributes
    multi_strat_lookthrough = report_data.adhoc_fund_attributes.multi_strat_lookthrough
    stresses = report_data.adhoc_fund_attributes.stresses
    fund_attributes = report_data.adhoc_fund_attributes.fund_attributes

    # TODO map weights to ids
    weights = weights.rename(columns={'InvestmentGroupName': 'Fund'}).set_index('Fund')
    weights.drop(columns={"InvestmentGroupId"}, inplace=True)

    header_info = _format_header_info(acronym=acronym, scenario_name=scenario_name, as_of_date=as_of_date)
    non_config_attributes = _format_non_config_attributes(reference_attributes=portfolio_attributes)

    # TODO - need to do this upfront because bug in config processing overwrites
    #  optim_inputs to only allocated-to subset
    full_optim_inputs = _process_optimization_inputs(inputs=optim_inputs, fund_order=optim_inputs.config.fundSubset)
    roster_inputs = _format_strategy_roster_inputs(optim_inputs=full_optim_inputs,
                                                   stresses=stresses,
                                                   fund_attributes=fund_attributes)

    config_attributes = _format_config_attributes(weights=weights,
                                                  optim_inputs=optim_inputs,
                                                  rf=optim_inputs.config.expRf)
    metrics = _combine_metrics_across_weights(weights=weights,
                                              optim_inputs=optim_inputs,
                                              rf=optim_inputs.config.expRf)

    stress_contribs = _get_stress_contributions(weights=weights, stresses=stresses)

    formatted_weights = _format_allocations_summary(weights=weights, metrics=metrics, stress_contribs=stress_contribs)

    strat_weights_lookthru = _get_strategy_weights_with_lookthrough(optim_inputs=optim_inputs,
                                                                    multi_strat_lookthrough=multi_strat_lookthrough,
                                                                    weights=weights)

    formatted_weights['dollar_allocation_summary'] = _combine_weights_and_strategy_lookthru_weights(
        weights=formatted_weights['dollar_allocation_summary'],
        strat_weights=strat_weights_lookthru
    )

    risk_weights = metrics['risk_allocations'].loc[weights.index]
    risk_weights.columns = weights.columns
    strat_risk_weights_lookthru = _get_strategy_weights_with_lookthrough(optim_inputs=optim_inputs,
                                                                         multi_strat_lookthrough=multi_strat_lookthrough,
                                                                         weights = risk_weights.fillna(0))
    strat_risk_weights_lookthru.columns = ['Fund'] + (strat_risk_weights_lookthru.columns[1:] + ' - %Risk').tolist()

    # TODO refactor with _get_strategy_weights_with_lookthrough
    lookthru_strats = _get_fund_lookthrough_strategy_allocations(optim_inputs=optim_inputs,
                                                                 multi_strat_lookthrough=multi_strat_lookthrough)

    weights = stress_contribs * 100
    weights = weights.merge(lookthru_strats, left_index=True, right_on='InvestmentGroupName')
    lookthru_contribs = weights[stress_contribs.columns].multiply(
        weights["LookthruWeight"],
        axis="index")
    lookthru_contribs['Strategy'] = weights['Strategy']
    lookthru_contribs.loc[lookthru_contribs['Strategy'] == 'Multi-Strategy', 'Strategy'] = 'Other'
    strat_wts_lookthru = lookthru_contribs.groupby('Strategy').sum()
    strat_wts_lookthru = strat_wts_lookthru.reset_index()
    # column_order = ['Strategy', 'Current', 'ShortTermOptimal', 'Delta_ST', 'LongTermOptimal', 'Delta_LT']

    strat_risk_weights_lookthru.drop(columns={'Delta_LT - %Risk', 'Delta_ST - %Risk'}, inplace=True)
    strat_risk_weights_lookthru = pd.concat([strat_risk_weights_lookthru, strat_wts_lookthru], axis=1)
    strat_risk_weights_lookthru.rename(columns={'Fund': "Strategy"}, inplace=True)

    strat_risk_weights_lookthru = strat_risk_weights_lookthru.iloc[:,~strat_risk_weights_lookthru.columns.duplicated()]

    tmp = _combine_weights_and_strategy_lookthru_weights(
        weights=formatted_weights['risk_allocation_summary'],
        strat_weights=strat_risk_weights_lookthru
    )

    formatted_weights['risk_allocation_summary'] = tmp

    metric_subset = ("distribution_of_returns",
                     "exp_risk_adj_performance",
                     "strategy_allocation",
                     "objective_measures",
                     "liquidity")
    metrics = {k: metrics[k] for k in metric_subset}

    formatted_weights, metrics = _nullify_data_for_zero_weights(weights=formatted_weights, metrics=metrics)

    excel_data = {}
    excel_data.update(header_info)
    excel_data.update(non_config_attributes)
    excel_data.update(config_attributes)

    excel_data.update(metrics)
    excel_data.update(formatted_weights)
    excel_data.update(roster_inputs)

    return excel_data
