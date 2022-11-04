import pandas as pd
import re
import numpy as np
from pyxirr import xirr




class RunPeracs(object):
    def __init__(self, runner=None):
        self._runner = runner

    def to_camelcase(self, s):
        s = re.sub(r"(_|-)+", " ", s).title().replace(" ", "").replace("*","")
        return ''.join([s[0], s[1:]])

    def get_attrib_and_cfs(self):
        raw = pd.read_excel('C:/Tmp/INTERNAL-USE-Gross-Par-All - Greenbriar v35 202206 v01 GR - 2022.09.22 15.58.13.xlsx',
                            sheet_name='Deals_Data')
        raw.rename(
            columns=lambda x: re.sub("[^A-Za-z0-9]+", "", self.to_camelcase(str(x))), inplace=True
        )
        df = raw[~raw.SyntheticDeal]

        df['TotalValue'] = df.RealizedValueGross + df.UnrealizedValueGross
        df['InvestmentGain'] = df.TotalValue - df.EquityInvested
        # df['GrossMultiple'] = df.TotalValue / df.EquityInvested

        cf_raw = pd.read_excel(
            'C:/Tmp/INTERNAL-USE-Gross-Par-All - Greenbriar v35 202206 v01 GR - 2022.09.22 15.58.13.xlsx',
            sheet_name='CF_Data')
        cf_raw.rename(
            columns=lambda x: re.sub("[^A-Za-z0-9]+", "", self.to_camelcase(str(x))), inplace=True
        )
        deal_attrib = df[['DealName', 'FundName', 'Country', 'IndustryCategory', 'AcquisitionDate', 'ExitDate',
           'EquityInvested', 'Status', 'UnrealizedValueGross', 'RealizedValueGross', 'TotalValue', 'InvestmentGain']]
        deal_cf = cf_raw[['DealName', 'FundName', 'CashflowDate', 'Cf', 'CashflowType']]

        fund_attrib_raw = pd.read_excel(
            'C:/Tmp/INTERNAL-USE-Gross-Par-All - Greenbriar v35 202206 v01 GR - 2022.09.22 15.58.13.xlsx',
            sheet_name='Funds_Data')
        fund_attrib_raw.rename(
            columns=lambda x: re.sub("[^A-Za-z0-9]+", "", self.to_camelcase(str(x))), inplace=True
        )
        fund_attrib = fund_attrib_raw[['FundName', 'Region', 'VintageYear', 'CommittedCapital', 'Focus',
       'NetIrr', 'GrossIrr', 'NetDpi', 'NetMoic', 'ReportingDate',
       'ReportingCurrency']]

        return fund_attrib, deal_attrib, deal_cf


    def calc_multiple(self, cf, group_cols=None, type='Gross'):
        # all funds/deals in cfs dataframe are what the result will reflect (i.e. do filtering beforehand)
        if group_cols is None:
            multiple = cf[cf.CashflowType.isin(['D', 'R'])].Cf.sum() / abs(cf[
                cf.CashflowType.isin(['T'])].Cf.sum())
        else:
            multiple = cf[cf.CashflowType.isin(['D', 'R'])].groupby(group_cols).Cf.sum() / cf[cf.CashflowType.isin(['T'])].groupby(
                group_cols).Cf.sum().abs()
            multiple = multiple.reset_index().rename(columns={'Cf': type + 'Multiple'})
        return multiple

    def calc_irr(self, cf, group_cols=None, type='Gross'):
        # all funds/deals in cfs dataframe are what the result will reflect (i.e. do filtering beforehand)
        if group_cols is None:
            irr = xirr(cf[["CashflowDate", "Cf"]].groupby('CashflowDate').sum().reset_index())
        else:
            irr = cf.groupby(group_cols)[["CashflowDate", "Cf"]].apply(xirr)
            irr = irr.reset_index().rename(columns={0: type+'Irr'})
        return irr

    def get_perf_concentration_rpt(self, df, cf):
        # concentration
        df['PctTotalGain'] = df.InvestmentGain / df.InvestmentGain.sum()
        df['PctCapital'] = df.EquityInvested / df.EquityInvested.sum()
        df['Rank'] = df.sort_values('InvestmentGain', ascending=False) \
                         .reset_index() \
                         .sort_values('index') \
                         .index + 1
        df['Rank'] = np.where(df.Rank.isin([1, 2, 3, 4, 5]), df.Rank, 'Other')
        top_deals = df[df.Rank != 'Other']
        top_deals_multiple = self.calc_multiple(cf[cf.DealName.isin(top_deals.DealName)], group_cols=['DealName'], type='Gross')
        top_deal_irr = self.calc_irr(cf[cf.DealName.isin(top_deals.DealName)], group_cols=['DealName'], type='Gross')

        top_deals_rslt = top_deals.sort_values('Rank').merge(top_deals_multiple, how='left',
                                                        left_on='DealName', right_on='DealName').merge(top_deal_irr, how='left',
                                                        left_on='DealName', right_on='DealName')[['DealName',
                                                                'FundName',
                                                                'AcquisitionDate',
                                                                'EquityInvested',
                                                                'ExitDate',
                                                                'UnrealizedValueGross',
                                                                'TotalValue',
                                                                'InvestmentGain',
                                                                'GrossMultiple',
                                                                'GrossIrr',
                                                                'PctTotalGain']]
        others = df[df.Rank == 'Other']
        others = others[['Rank',
                        'EquityInvested',
                        'UnrealizedValueGross',
                        'TotalValue',
                        'InvestmentGain',
                        'PctTotalGain']].groupby(['Rank']).sum()
        others['GrossMultiple'] = self.calc_multiple(cf[cf.DealName.isin(df[df.Rank == 'Other'].DealName)], group_cols=None, type='Gross')
        others['GrossIrr'] = self.calc_irr(cf[cf.DealName.isin(df[df.Rank == 'Other'].DealName)], group_cols=None, type='Gross')
        others['DealName'] = 'Other'
        others_rslt = others[['DealName',
                              'EquityInvested',
                              'UnrealizedValueGross',
                              'TotalValue',
                              'InvestmentGain',
                              'GrossMultiple',
                              'GrossIrr',
                              'PctTotalGain']]

        df['Group'] = 'all'
        total = df[['Group',
                         'EquityInvested',
                         'UnrealizedValueGross',
                         'TotalValue',
                         'InvestmentGain',
                         'PctTotalGain']].groupby('Group').sum()
        total['GrossMultiple'] = self.calc_multiple(cf[cf.DealName.isin(df.DealName)], group_cols=None,
                                                type='Gross')
        total['GrossIrr'] = self.calc_irr(cf[cf.DealName.isin(df.DealName)], group_cols=None, type='Gross')
        total['DealName'] = 'Total'
        total_rslt = total[['DealName',
                              'EquityInvested',
                              'UnrealizedValueGross',
                              'TotalValue',
                              'InvestmentGain',
                              'GrossMultiple',
                              'GrossIrr',
                              'PctTotalGain']]

        perf_concentration_rslt = pd.concat([top_deals_rslt, others_rslt, total_rslt])[['DealName',
                                                                'FundName',
                                                                'AcquisitionDate',
                                                                'EquityInvested',
                                                                'ExitDate',
                                                                'UnrealizedValueGross',
                                                                'TotalValue',
                                                                'InvestmentGain',
                                                                'GrossMultiple',
                                                                'GrossIrr',
                                                                'PctTotalGain']]

        return perf_concentration_rslt

    def get_distrib_returns(self, df, cf):
        # above = df[df.CostType == 'Above Cost']
        df['PctTotalGain'] = df.InvestmentGain / df.InvestmentGain.sum()
        df['PctCapital'] = df.EquityInvested / df.EquityInvested.sum()
        if len(df) == 0:
            return pd.DataFrame({'TotalValue': [0],
                                 'EquityInvested': [0],
                                 'PctCapital': [0],
                                 'GrossMultiple': [0],
                                 'GrossIrr': [0],
                                 'NumInvestments': [0]})

        #"Group" should be flexible
        df['Group'] = 'All'

        rslt = df[['Group',
                   'TotalValue',
                   'EquityInvested',
                   'InvestmentGain',
                   'PctCapital',
                   'PctTotalGain']].groupby(['Group']).sum()
        rslt['GrossMultiple'] = self.calc_multiple(cf[cf.DealName.isin(df.DealName)], group_cols=None,
                                               type='Gross')
        rslt['GrossIrr'] = self.calc_irr(cf[cf.DealName.isin(df.DealName)], group_cols=None, type='Gross')
        rslt['NumInvestments'] = len(df.DealName.unique())

        return rslt

    def get_distrib_returns_rpt(self, df, cf):
        # distribution of returns
        df['PctTotalGain'] = df.InvestmentGain / df.InvestmentGain.sum()
        df['PctCapital'] = df.EquityInvested / df.EquityInvested.sum()

        df = df.assign(
            CostType=lambda v: v.InvestmentGain.apply(
                lambda InvestmentGain: "Below Cost"
                if InvestmentGain < 0
                else "Above Cost"
                if InvestmentGain > 0
                else "At Cost"
                if InvestmentGain == 0
                else "N/A"
            ),
        )
        cost_type_stats = self._generate_stats_by_group(df, cf, 'CostType').set_index('CostType').reindex(['Above Cost', 'At Cost', 'Below Cost'])
        total = self.get_distrib_returns(df=df, cf=cf)

        distribution_rslt = pd.concat([cost_type_stats, total]).reset_index()[['NumInvestments', 'GrossMultiple', 'GrossIrr', 'PctCapital']]
        return distribution_rslt

    def get_return_concentration_rpt(self, df, cf):
        # concentration of returns
        df['PctTotalGain'] = df.InvestmentGain / df.InvestmentGain.sum()
        df['PctCapital'] = df.EquityInvested / df.EquityInvested.sum()
        df['Rank'] = df.sort_values('InvestmentGain', ascending=False) \
                         .reset_index() \
                         .sort_values('index') \
                         .index + 1
        df['Rank'] = np.where(df.Rank.isin([1, 2, 3, 4, 5]), df.Rank, 'Other')

        top_one = self.get_distrib_returns(df=df[df.Rank=='1'], cf=cf)
        top_one['Order'] = 'Top 1'

        top_three = self.get_distrib_returns(df=df[df.Rank.isin(['1', '2', '3'])], cf=cf)
        top_three['Order'] = 'Top 3'

        top_five = self.get_distrib_returns(df=df[df.Rank.isin(['1', '2', '3', '4', '5'])], cf=cf)
        top_five['Order'] = 'Top 5'

        others = self.get_distrib_returns(df=df[df.Rank == 'Other'], cf=cf)
        others['Order'] = 'Others'

        total = self.get_distrib_returns(df=df, cf=cf)
        total['Order'] = 'Total'

        concentration_rslt = pd.concat([top_one, top_three, top_five, others, total]).set_index('Order')[['GrossIrr', 'GrossMultiple', 'PctTotalGain']]
        return concentration_rslt

    def _get_single_fund_stats(self, df, cf):
        df['PctTotalGain'] = df.InvestmentGain / df.InvestmentGain.sum()
        df = df[['DealName', 'AcquisitionDate', 'ExitDate',
                             'HoldingPeriod', 'EquityInvested', 'UnrealizedValueGross',
                             'TotalValue', 'InvestmentGain',
                             'PctTotalGain', 'LossRatio']].reset_index()

        gross_multiple = self.calc_multiple(cf[cf.DealName.isin(df.DealName)], group_cols=['DealName'],
                                            type='Gross')
        gross_irr = self.calc_irr(cf[cf.DealName.isin(df.DealName)], group_cols=['DealName'],
                                  type='Gross')
        deals = df.merge(gross_multiple, how='left', left_on='DealName', right_on='DealName') \
            .merge(gross_irr, how='left', left_on='DealName', right_on='DealName')


        #separate primarily because of weighted average of holding period
        total_df = deals.copy()
        total_df['DealName'] = 'Total'
        total_df['alloc'] = total_df.EquityInvested / total_df.EquityInvested.sum()
        total_df.HoldingPeriod = total_df.alloc * total_df.HoldingPeriod

        total = total_df[['DealName',
                            'HoldingPeriod',
                            'TotalValue',
                            'UnrealizedValueGross',
                            'InvestmentGain',
                            'EquityInvested',
                            'PctTotalGain']].groupby(['DealName']).sum()
        total['GrossMultiple'] = self.calc_multiple(cf[cf.DealName.isin(deals.DealName)], group_cols=None,
                                            type='Gross')
        total['GrossIrr'] = self.calc_irr(cf[cf.DealName.isin(deals.DealName)], group_cols=None,
                                  type='Gross')
        total['LossRatio'] = deals[deals.LossRatio != 0].InvestmentGain.abs().sum() / deals.EquityInvested.sum()

        rslt = pd.concat([deals.sort_values('InvestmentGain', ascending=False), total])[['DealName', 'AcquisitionDate', 'ExitDate', 'HoldingPeriod',
                                                                                         'EquityInvested', 'UnrealizedValueGross', 'TotalValue',
                                                                                         'InvestmentGain', 'GrossMultiple', 'PctTotalGain', 'LossRatio', 'GrossIrr']]
        return rslt

    # calcs realized/unrealized by fund
    def generate_fund_reports(self, df, cf):
        funds = df.FundName.drop_duplicates().to_list()
        #should move loop
        for fund in funds:
            # df_fund and cf_fund represent a single fund's deals and cashflows
            cf_fund = cf[cf.FundName == fund]
            df_fund = df[df.FundName == fund][['DealName', 'AcquisitionDate', 'ExitDate',
                     'EquityInvested', 'Status', 'UnrealizedValueGross',
                     'RealizedValueGross',
                     'TotalValue', 'InvestmentGain']]
            df_fund['HoldingPeriod'] = (df_fund.ExitDate - df_fund.AcquisitionDate) / pd.Timedelta('365 days')
            df_fund['LossRatio'] = np.where(df_fund.InvestmentGain < 0, df_fund.InvestmentGain.abs() / df_fund.EquityInvested, 0)

            #todo write to template
            realized = df_fund[df_fund.Status=='Realized']
            if len(realized) != 0:
                realized_rslt = self._get_single_fund_stats(realized, cf=cf)
                realized_rslt.to_csv('C:/Tmp/' + str(fund) + '_Realized.csv')

            unrealized = df_fund[df_fund.Status == 'Unrealized']
            if len(unrealized) != 0:
                unrealized_rslt = self._get_single_fund_stats(unrealized, cf=cf)
                unrealized_rslt.to_csv('C:/Tmp/' + str(fund) + '_Unealized.csv')

            if len(df_fund) != 0:
                all_rslt = self._get_single_fund_stats(df_fund, cf=cf)
                all_rslt.to_csv('C:/Tmp/' + str(fund) + '_All.csv')


    def generate_manager_tr_sheet(self, attrib, cf):
        # vintage = cf.groupby('FundName').CashflowDate.min().reset_index()
        # vintage['Vintage'] = pd.to_datetime(vintage.CashflowDate).dt.year

        attrib['NumInvestments'] = 1
        summable_df = attrib[['FundName', 'NumInvestments', 'EquityInvested', 'RealizedValueGross', 'UnrealizedValueGross', 'TotalValue']].groupby('FundName').sum()
        sum_rslt = summable_df.reset_index().append(summable_df.sum(), ignore_index=True)
        sum_rslt.FundName = np.where(sum_rslt.FundName.isnull(), 'Total', sum_rslt.FundName)

        gross_multiple = cf[cf.CashflowType.isin(['D', 'R'])].groupby('FundName').Cf.sum() / cf[cf.CashflowType.isin(['T'])].groupby('FundName').Cf.sum().abs()
        gross_multiple_rslt = pd.concat([gross_multiple.reset_index(), pd.DataFrame({'FundName': 'Total',
                                                               'Cf': [cf[cf.CashflowType.isin(['D', 'R'])].Cf.sum() / abs(cf[cf.CashflowType.isin(['T'])].Cf.sum())]})]).rename(columns={'Cf':'GrossMultiple'})

        gross_irr = cf.groupby("FundName")[["CashflowDate", "Cf"]].apply(xirr)
        gross_irr_rslt = pd.concat([gross_irr.reset_index(), pd.DataFrame({'FundName': 'Total',
                                                                                     0: [xirr(cf[["CashflowDate", "Cf"]].groupby('CashflowDate').sum().reset_index())]})]).rename(
            columns={0: 'GrossIrr'})

        attrib['LossRatio'] = np.where(attrib.InvestmentGain < 0, attrib.InvestmentGain.abs() / attrib.EquityInvested,
                                        0)
        loss_ratio = abs(attrib[attrib.LossRatio != 0].groupby('FundName').InvestmentGain.sum()) / attrib.groupby('FundName').EquityInvested.sum()
        loss_ratio_rslt = pd.concat([loss_ratio.reset_index(), pd.DataFrame({'FundName': 'Total',
                                                                                     0: [abs(attrib[attrib.LossRatio != 0].InvestmentGain.sum()) / attrib.EquityInvested.sum()]})]).rename(
            columns={0: 'LossRatio'})

        result = sum_rslt.merge(gross_multiple_rslt).merge(gross_irr_rslt).merge(loss_ratio_rslt)
        result = result[['FundName', 'NumInvestments', 'EquityInvested', 'RealizedValueGross',
           'UnrealizedValueGross', 'TotalValue', 'GrossMultiple', 'GrossIrr',
           'LossRatio']]
        return result

    def _generate_stats_by_group(self, df, cf, group_col):
        # better way than for loop?
        rslt = pd.DataFrame()
        for i in df[group_col].drop_duplicates().to_list():
            print(i)
            group_df = df[df[group_col] == i]
            group_rslt = self.get_distrib_returns(df=group_df, cf=cf[cf.DealName.isin(group_df.DealName)])
            group_rslt[group_col] = i
            rslt = pd.concat([rslt, group_rslt])
        rslt['PctTotalGain'] = rslt.InvestmentGain / rslt.InvestmentGain.sum()
        rslt['PctCapital'] = rslt.EquityInvested / rslt.EquityInvested.sum()
        return rslt

    def run_peracs_analysis(self):
        fund_attrib, attrib, cf = self.get_attrib_and_cfs()

        ### perf concentration
        top_table_rslt = self.get_perf_concentration_rpt(attrib, cf)
        top_table_rslt.to_csv('C:/Tmp/top_table_perf_concen_rslt.csv')

        concen_rlzd_rslt = self.get_perf_concentration_rpt(attrib[attrib.Status == 'Realized'], cf[
            cf.DealName.isin(attrib[attrib.Status == 'Realized'].DealName)])
        concen_rlzd_rslt.to_csv('C:/Tmp/concen_rlzd_rslt.csv')

        distribution_rslt = self.get_distrib_returns_rpt(attrib, cf)
        distribution_rslt.to_csv('C:/Tmp/distribution_rslt.csv')

        distribution_rlzd_rslt = self.get_distrib_returns_rpt(attrib[attrib.Status == 'Realized'], cf)
        distribution_rlzd_rslt.to_csv('C:/Tmp/distribution_rlzd_rslt.csv')

        return_concentration = self.get_return_concentration_rpt(attrib, cf)
        return_concentration.to_csv('C:/Tmp/return_concentration.csv')

        return_concentration_rlzed = self.get_return_concentration_rpt(attrib[attrib.Status == 'Realized'], cf)
        return_concentration_rlzed.to_csv('C:/Tmp/return_concentration_rlzed.csv')

        full_manager_tr = self.generate_manager_tr_sheet(attrib, cf)
        full_manager_tr.to_csv('C:/Tmp/full mgr tr.csv')
        #
        realized_manager_tr = self.generate_manager_tr_sheet(attrib=attrib[attrib.Status == 'Realized'], cf=cf[
            cf.DealName.isin(attrib[attrib.Status == 'Realized'].DealName)])
        realized_manager_tr.to_csv('C:/Tmp/realized_manager_tr.csv')

        unrealized_manager_tr = self.generate_manager_tr_sheet(attrib=attrib[attrib.Status == 'Unrealized'], cf=cf[
            cf.DealName.isin(attrib[attrib.Status == 'Unrealized'].DealName)])
        unrealized_manager_tr.to_csv('C:/Tmp/unrealized_manager_tr.csv')

        self.generate_fund_reports(attrib, cf)
        fund_attrib[['FundName', 'VintageYear', 'CommittedCapital', 'NetIrr', 'GrossIrr', 'NetDpi', 'NetMoic']].to_csv('C:/Tmp/peracs funddimn.csv')

        industry_breakdown = self._generate_stats_by_group(attrib, cf, 'IndustryCategory')
        industry_breakdown.to_csv('C:/Tmp/industry breakdown.csv')

        fund_breakdown = self._generate_stats_by_group(attrib, cf, 'FundName')
        fund_breakdown.to_csv('C:/Tmp/fund breakdown.csv')





if __name__ == "__main__":
    svc = RunPeracs()

    svc.run_peracs_analysis()