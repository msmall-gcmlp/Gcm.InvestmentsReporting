import numpy as np
import pandas as pd
from gcm.inv.models.portfolio_construction.portfolio_metrics.portfolio_metrics import \
    summarize_obs_cons, summarize_portfolio_metrics, ProcessedFundInputs

from _legacy.Reports.reports.portfolio_construction.portfolio_construction_report_data import ReportData, PortfolioData, \
    PortfolioInputs


def _create_strategy_allocation_block(weights, strategy):
    wts_g = weights[weights['Strategy'] == strategy]
    wts_g.drop(columns={'Strategy'}, inplace=True)

    strategy_g = wts_g.sum().to_frame().T

    strategy_g['Fund'] = strategy
    spacer_row = strategy_g.copy()
    spacer_row[0:] = None
    return pd.concat([strategy_g, wts_g, spacer_row], axis=0)


def _denote_fund_capacity_status(summary, capacity_status):
    summary = summary.merge(capacity_status, how='left', left_on='Fund', right_index=True)
    is_cc = summary['PctNewCapacityAvailable'] < 1
    summary.loc[is_cc, 'Fund'] = summary.loc[is_cc, 'Fund'] + ' *'
    is_closed = summary['PctNewCapacityAvailable'] == 0
    summary.loc[is_closed, 'Fund'] = summary.loc[is_closed, 'Fund'] + '*'
    summary.drop(columns={"PctNewCapacityAvailable"}, inplace=True)
    return summary


def _add_allocation_delta_columns(summary):
    summary['Delta_LT'] = summary['LongTermOptimal'] - summary['Current']
    summary['Delta_ST'] = summary['ShortTermOptimal'] - summary['Current']
    summary = summary[[summary.columns[0], 'Current', 'ShortTermOptimal', 'Delta_ST', 'LongTermOptimal', 'Delta_LT']]
    return summary


def _convert_weights_to_int(summary):
    summary.iloc[:, 1:] = (summary.iloc[:, 1:] * 100).astype(float).round(0)
    return summary


def _add_strategy_subtotals(summary, strategies):
    strategies = pd.concat([strategies, pd.DataFrame({'Strategy': 'Cash & Other'}, index=['Cash & Other'])], axis=0)
    wts_and_strategies = summary.merge(strategies, left_on='Fund', right_index=True, how='left')
    wts_formatted = pd.DataFrame(columns=summary.columns.tolist())
    non_cash_strategies = sorted(wts_and_strategies['Strategy'].unique())
    non_cash_strategies.remove('Cash & Other')
    for s in non_cash_strategies:
        wts_formatted = pd.concat([wts_formatted,
                                   _create_strategy_allocation_block(weights=wts_and_strategies, strategy=s)], axis=0)

    cash_and_other_subtotal = summary[summary['Fund'] == 'Cash & Other']
    # spacer_row = pd.DataFrame(np.nan, index=[0], columns=wts_formatted.columns)
    wts_formatted = pd.concat([wts_formatted, cash_and_other_subtotal], axis=0)

    return wts_formatted


def _add_allocations_by_strategy_with_look_through(summary, strategy_weights):
    heading_row = pd.DataFrame({'Fund': ['With Lookthrough to Multi-Strats']})
    strategy_weights.rename(columns={'Strategy': 'Fund'}, inplace=True)
    strategy_weights['Fund'] = '   ' + strategy_weights['Fund']
    strategy_allocations = pd.concat([heading_row, strategy_weights], axis=0)
    spacer_row = pd.DataFrame(np.nan, index=[0], columns=strategy_allocations.columns)
    summary_with_strategies = pd.concat([summary, spacer_row, strategy_allocations], axis=0)
    return summary_with_strategies


def _add_final_total_row(summary, pre_subtotal_weights):
    total_row = pre_subtotal_weights.sum()
    total_row['Fund'] = 'Total'
    total_row = total_row.to_frame().T
    summary_with_total = pd.concat([summary, total_row], axis=0)
    return summary_with_total


def _sort_weights_alphabetically(raw_fund_weights):
    return raw_fund_weights.sort_index().reset_index()


def _add_cash_and_other_weight(raw_fund_weights):
    cash_and_other = (1 - raw_fund_weights.sum()).to_frame('Cash & Other').T
    weights = pd.concat([raw_fund_weights, cash_and_other], axis=0)
    weights.index.name = 'Fund'
    return weights


def _add_cash_and_other_risk_contribution(risk_contributions):
    cash_and_other = pd.DataFrame(0, index=['Cash & Other'], columns=risk_contributions.columns)
    risk_contributions = pd.concat([risk_contributions, cash_and_other], axis=0)
    risk_contributions.index.name = 'Fund'
    return risk_contributions


def _create_optimized_allocations_sheet_data(weights, strategy_lookthrough_weights, strategies, capacities):
    weights = _add_cash_and_other_weight(raw_fund_weights=weights)
    wts = _sort_weights_alphabetically(raw_fund_weights=weights)
    wts = _add_allocation_delta_columns(summary=wts)
    pre_subtotal_weights = wts
    wts = _add_strategy_subtotals(summary=wts, strategies=strategies)
    strategy_weights = _add_allocation_delta_columns(summary=strategy_lookthrough_weights)
    wts = _add_allocations_by_strategy_with_look_through(summary=wts, strategy_weights=strategy_weights)
    wts = _add_final_total_row(summary=wts, pre_subtotal_weights=pre_subtotal_weights)
    wts = _convert_weights_to_int(summary=wts)
    wts = _denote_fund_capacity_status(summary=wts, capacity_status=capacities)

    weights = {"dollar_allocation_summary": wts}

    return weights


def _order_risk_utilization_columns(summary):
    weight_order = ['Current', 'ShortTermOptimal', 'LongTermOptimal']
    stress_order = ['RiskAlloc', 'LongRunSelloff', 'CreditLiquidityTechnicals',
                    'GrowthSelloff', 'HFDelever', 'GrossExp']
    column_order = ['Fund'] + [w + '_' + s for s in stress_order for w in weight_order]
    summary = summary[column_order]
    return summary


def _create_optimized_risk_utilizations_sheet_data(
    risk_contributions, strategy_lookthrough_risk_contributions, strategies, capacities
):
    risk_contributions = _add_cash_and_other_risk_contribution(risk_contributions=risk_contributions)
    wts = _sort_weights_alphabetically(raw_fund_weights=risk_contributions)
    pre_subtotal_weights = wts
    wts = _add_strategy_subtotals(summary=wts, strategies=strategies)
    wts = _add_allocations_by_strategy_with_look_through(summary=wts,
                                                         strategy_weights=strategy_lookthrough_risk_contributions)
    wts = _add_final_total_row(summary=wts, pre_subtotal_weights=pre_subtotal_weights)
    wts = _convert_weights_to_int(summary=wts)
    wts = _denote_fund_capacity_status(summary=wts, capacity_status=capacities)
    wts = _order_risk_utilization_columns(summary=wts)

    weights = {"risk_allocation_summary": wts}

    return weights


def _add_conditional_return_contributions(conditional_returns, raw_fund_weights):
    condl_return_contribs = pd.DataFrame()
    for w in ['Current', 'LongTermOptimal']:
        condl_returns = conditional_returns[w] * 100
        condl_returns.columns = [c + '_' + w for c in condl_returns.columns]
        condl_returns.loc['Cash & Other', :] = [0, 0, 0]
        wts = raw_fund_weights[['Fund', w]].set_index('Fund')
        condl_returns = condl_returns.loc[raw_fund_weights['Fund']]
        new_contribs = condl_returns.multiply(wts[w], axis="index")
        condl_return_contribs = pd.concat([condl_return_contribs, new_contribs], axis=1)
    summary = raw_fund_weights.merge(condl_return_contribs, left_on='Fund', right_index=True, how='left')
    return summary


def _create_conditional_returns_sheet_data(
    weights, strategies, capacities, conditional_fund_returns
):
    weights = _add_cash_and_other_weight(raw_fund_weights=weights)
    wts = _sort_weights_alphabetically(raw_fund_weights=weights)
    pre_subtotal_weights = _add_conditional_return_contributions(conditional_returns=conditional_fund_returns,
                                                                 raw_fund_weights=wts)

    wts = _add_strategy_subtotals(summary=pre_subtotal_weights, strategies=strategies)
    wts = _add_final_total_row(summary=wts, pre_subtotal_weights=pre_subtotal_weights)

    wts = _convert_weights_to_int(summary=wts)
    wts = _denote_fund_capacity_status(summary=wts, capacity_status=capacities)

    weights = {"conditional_return_summary": wts}

    return weights


def _combine_metrics_across_weights(weights, fund_data, portfolio_fees, portfolio_target_return, rf):
    portfolio_metrics = {k: summarize_portfolio_metrics(
        weights=weights[k],
        fund_inputs=fund_data.allocated_optim_inputs,
        portfolio_fees=portfolio_fees,
        rf=rf,
        target_return=portfolio_target_return) for k in weights.columns
    }

    metrics = {key: None for key in [f.name for f in portfolio_metrics['Current'].__attrs_attrs__]}
    metric_names = list(metrics.keys())
    metric_names = set(metric_names) - set(['conditional_manager_returns'])
    for m in metric_names:
        metrics[m] = pd.concat([getattr(portfolio_metrics['Current'], m),
                                getattr(portfolio_metrics['ShortTermOptimal'], m),
                                getattr(portfolio_metrics['LongTermOptimal'], m)], axis=1)
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

    metrics['conditional_current_returns'] = getattr(portfolio_metrics['Current'],
                                                     'conditional_manager_returns')

    metrics['conditional_long_term_returns'] = getattr(portfolio_metrics['LongTermOptimal'],
                                                       'conditional_manager_returns')

    metrics["strategy_dollar_weights_with_lt"] = _get_strategy_weights_with_lookthrough(
        weights=weights,
        fund_strategies=fund_data.allocated_optim_inputs.strategies,
        multi_strat_lookthrough=fund_data.multi_strat_lookthrough
    )

    stress_contributions = _get_stress_contributions(weights=weights, stresses=fund_data.adhoc_stresses)

    metrics['risk_allocations'].columns = metrics['risk_allocations'].columns + '_RiskAlloc'
    metrics['risk_allocations'] = metrics['risk_allocations'].merge(stress_contributions, left_index=True,
                                                                    right_index=True, how='left')

    metrics["strategy_risk_weights_with_lt"] = _get_strategy_weights_with_lookthrough(
        weights=metrics['risk_allocations'],
        fund_strategies=fund_data.allocated_optim_inputs.strategies,
        multi_strat_lookthrough=fund_data.multi_strat_lookthrough
    )

    return metrics


def _format_header_info(portfolio: PortfolioInputs):
    header_info = pd.DataFrame(
        {
            "header_info": [
                portfolio.acronym,
                portfolio.scenario,
                portfolio.as_of_date,
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


def _format_config_attributes(objectives, constraints, attributes, rf):
    obs_cons = summarize_obs_cons(objectives=objectives, constraints=constraints, attributes=attributes)
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


def _format_adhoc_portfolio_attributes(portfolio_attributes):
    portfolio_attributes = dict(zip(portfolio_attributes['Field'], portfolio_attributes['Value']))

    # format fees for display
    fee_items = ['MgmtFee', 'IncentiveFee', 'HurdleRate']
    fees = {key: str(round(100 * portfolio_attributes[key], 2)) + '%' for key in fee_items}
    portfolio_attributes['Fees'] = fees['MgmtFee'] + ' | ' + fees['IncentiveFee'] + ' | ' + fees['HurdleRate']

    # format beta for display
    beta = portfolio_attributes.get('MaxBeta')
    if beta is None:
        portfolio_attributes["FormalBetaConstraint"] = "--"
    else:
        beta = "{:.2f}".format(portfolio_attributes['MaxBeta'])
        bmrk = portfolio_attributes.get('BetaBenchmark', 'Market')
        portfolio_attributes["FormalBetaConstraint"] = beta + ' vs ' + bmrk

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
    formal_attribute_subset = {key: pd.DataFrame({portfolio_attributes.get(key)}) for key in formal_attributes}

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


def _get_fund_lookthrough_strategy_allocations(fund_strategies, multi_strat_lookthrough):
    multi_strat_lookthrough = multi_strat_lookthrough.melt(id_vars='InvestmentGroupName',
                                                           value_name='LookthruWeight',
                                                           var_name='Strategy')
    single_strats = fund_strategies[~fund_strategies.index.isin(multi_strat_lookthrough['InvestmentGroupName'])]
    single_strats = single_strats.reset_index().rename(columns={'Fund': 'InvestmentGroupName'})
    single_strats['LookthruWeight'] = 1

    lookthru_strats = pd.concat([multi_strat_lookthrough, single_strats], axis=0)
    return lookthru_strats


def _get_strategy_weights_with_lookthrough(weights, fund_strategies, multi_strat_lookthrough):
    lookthru_strats = _get_fund_lookthrough_strategy_allocations(fund_strategies=fund_strategies,
                                                                 multi_strat_lookthrough=multi_strat_lookthrough)

    weight_columns = weights.columns
    weights = weights.merge(lookthru_strats, left_index=True, right_on='InvestmentGroupName')
    lookthru_contribs = weights[weight_columns].multiply(weights["LookthruWeight"], axis="index")
    lookthru_contribs['Strategy'] = weights['Strategy']
    lookthru_contribs.loc[lookthru_contribs['Strategy'] == 'Multi-Strategy', 'Strategy'] = 'Other'
    strat_wts_lookthru = lookthru_contribs.groupby('Strategy').sum()
    strat_wts_lookthru = strat_wts_lookthru.reset_index()
    strat_wts_lookthru = strat_wts_lookthru.sort_values(strat_wts_lookthru.columns[1], ascending=False)
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


def _get_fund_expectations(fund_inputs, rf):
    expectations = pd.DataFrame({
        "Return": fund_inputs.correlatedSimulations.mean(),
        "Vol": fund_inputs.expectedVols
    })
    expectations['Sharpe'] = (expectations['Return'] - rf) / expectations['Vol']

    eq_beta = fund_inputs.fundStresses['LongRunSelloff'] / -0.15
    eq_return = 0.055 + rf
    arb_return = (eq_beta * eq_return) + (1 - eq_beta) * rf
    expectations['ExcessReturn'] = expectations['Return'] - arb_return
    expectations = expectations[['Return', 'ExcessReturn', 'Vol', 'Sharpe']]
    return expectations


def _get_fund_expected_outcomes(fund_inputs):
    outcomes = fund_inputs.correlatedSimulations.apply(np.percentile, q=[25, 50, 75], axis=0)
    outcomes = outcomes.T
    outcomes.columns = ['25th', '50th', '75th']
    return outcomes


def _get_fund_risk_and_exposure(fund_inputs, adhoc_stresses):
    # fund_stresses = fund_inputs.fundStresses[['LongRunSelloff', 'CreditLiquidityTechnicals']]
    fund_stresses = adhoc_stresses.copy()
    fund_stresses = fund_stresses.set_index('InvestmentGroupName')
    fund_stresses = fund_stresses.reindex(fund_inputs.fundIdios.index)
    return fund_stresses


def _get_fund_other_attributes(fund_inputs, adhoc_fund_attributes):
    annual_liq = fund_inputs.liquidityTerms['annual']
    annual_liq.name = 'PctAnnualLiquidity'
    attributes = pd.concat([adhoc_fund_attributes, annual_liq], axis=1)
    attributes = attributes.reindex(annual_liq.index)
    attributes = attributes[['BenchmarkStrategy',
                             'BenchmarkSubStrategy',
                             'ExcessRtnPeerPtile',
                             'InputsAccuracy',
                             'PctActiveRisk',
                             'PctAnnualLiquidity',
                             'AvailableCapacity']]
    return attributes


def _get_all_fund_roster_inputs(fund_inputs, adhoc_stresses, adhoc_attributes, rf):
    fund_expectations = _get_fund_expectations(fund_inputs, rf)
    fund_exp_outcomes = _get_fund_expected_outcomes(fund_inputs)
    fund_stresses = _get_fund_risk_and_exposure(fund_inputs, adhoc_stresses)
    fund_other_attributes = _get_fund_other_attributes(fund_inputs, adhoc_attributes)
    fund_roster_inputs = pd.concat([fund_expectations, fund_exp_outcomes, fund_stresses,
                                    fund_other_attributes], axis=1)
    return fund_roster_inputs


def _format_strategy_roster_inputs(eligible_fund_inputs: ProcessedFundInputs, stresses, fund_attributes, rf):
    roster_inputs = _get_all_fund_roster_inputs(fund_inputs=eligible_fund_inputs,
                                                adhoc_stresses=stresses,
                                                adhoc_attributes=fund_attributes,
                                                rf=rf)
    roster_inputs = roster_inputs.reset_index().rename(columns={'index': 'InvestmentGroupName'})

    front_columns = ['BenchmarkSubStrategy', 'InvestmentGroupName', 'Return', 'ExcessReturn', 'Vol', 'Sharpe',
                     '25th', '50th', '75th']
    back_columns = ['ExcessRtnPeerPtile', 'InputsAccuracy', 'PctActiveRisk',
                    'PctAnnualLiquidity', 'AvailableCapacity']

    credit = roster_inputs[roster_inputs['BenchmarkStrategy'] == 'Credit']
    credit = credit[front_columns + ['LongRunSelloff', 'CreditLiquidityTechnicals'] + back_columns]

    equity = roster_inputs[roster_inputs['BenchmarkStrategy'] == 'Long/Short Equity']
    equity = equity[front_columns + ['LongRunSelloff', 'HFDelever'] + back_columns]

    ds = roster_inputs[~roster_inputs['BenchmarkStrategy'].isin(['Credit', 'Long/Short Equity'])]
    ds = ds[front_columns + ['LongRunSelloff', 'GrossExp'] + back_columns]

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


def _summarize_portfolio_obs_cons(portfolio_data: PortfolioData, rf):
    obs_cons = {}
    optim_dependent_attribs = _format_config_attributes(objectives=portfolio_data.objectives,
                                                        constraints=portfolio_data.constraints,
                                                        attributes=portfolio_data.optim_attributes,
                                                        rf=rf)
    report_only_attribs = _format_adhoc_portfolio_attributes(portfolio_attributes=portfolio_data.adhoc_attributes)

    obs_cons.update(optim_dependent_attribs)
    obs_cons.update(report_only_attribs)

    return obs_cons


def _create_obs_cons_sheet_data(portfolio_data, metrics, rf):
    sheet_data = {}
    obs_cons = _summarize_portfolio_obs_cons(portfolio_data=portfolio_data, rf=rf)

    metric_subset = ("distribution_of_returns",
                     "exp_risk_adj_performance",
                     "strategy_allocation",
                     "objective_measures",
                     "liquidity")
    summarized_metrics = {k: metrics[k] for k in metric_subset}

    sheet_data.update(obs_cons)
    sheet_data.update(summarized_metrics)
    return sheet_data


def generate_excel_report_data(inputs: ReportData):
    metrics = _combine_metrics_across_weights(weights=inputs.allocations,
                                              fund_data=inputs.fund_data,
                                              portfolio_fees=inputs.portfolio_data.optim_attributes.fees,
                                              portfolio_target_return=inputs.portfolio_data.objectives.maxThresholdLikelihood.targetReturn,
                                              rf=inputs.rf)

    header_info = _format_header_info(portfolio=inputs.portfolio)
    obs_cons_sheet_data = _create_obs_cons_sheet_data(portfolio_data=inputs.portfolio_data,
                                                      metrics=metrics,
                                                      rf=inputs.rf)

    optimized_allocations_sheet_data = _create_optimized_allocations_sheet_data(
        weights=inputs.allocations,
        strategy_lookthrough_weights=metrics["strategy_dollar_weights_with_lt"],
        strategies=inputs.fund_data.allocated_optim_inputs.strategies,
        capacities=inputs.fund_data.allocated_optim_inputs.fundCapacityLimits
    )

    optimized_risk_utilizations_sheet_data = _create_optimized_risk_utilizations_sheet_data(
        risk_contributions=metrics['risk_allocations'],
        strategy_lookthrough_risk_contributions=metrics["strategy_risk_weights_with_lt"],
        strategies=inputs.fund_data.allocated_optim_inputs.strategies,
        capacities=inputs.fund_data.allocated_optim_inputs.fundCapacityLimits
    )

    roster_inputs = _format_strategy_roster_inputs(eligible_fund_inputs=inputs.fund_data.eligible_optim_inputs,
                                                   stresses=inputs.fund_data.adhoc_stresses,
                                                   fund_attributes=inputs.fund_data.adhoc_attributes,
                                                   rf=inputs.rf)

    conditional_returns = {
        "Current": metrics['conditional_current_returns'],
        "LongTermOptimal": metrics['conditional_long_term_returns'],
    }

    conditional_returns_sheet_data = _create_conditional_returns_sheet_data(
        weights=inputs.allocations,
        strategies=inputs.fund_data.allocated_optim_inputs.strategies,
        capacities=inputs.fund_data.allocated_optim_inputs.fundCapacityLimits,
        conditional_fund_returns=conditional_returns
    )

    # TODO !!! BROKEN

    optimized_allocations_sheet_data, obs_cons_sheet_data = _nullify_data_for_zero_weights(
        weights=optimized_allocations_sheet_data,
        metrics=obs_cons_sheet_data
    )

    excel_data = {}
    excel_data.update(header_info)
    excel_data.update(obs_cons_sheet_data)
    excel_data.update(optimized_allocations_sheet_data)
    excel_data.update(optimized_risk_utilizations_sheet_data)
    excel_data.update(roster_inputs)
    excel_data.update(conditional_returns_sheet_data)

    return excel_data
