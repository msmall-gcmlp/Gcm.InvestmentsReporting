from datetime import datetime as dt
import pandas as pd
import numpy as np
import os
from _legacy.Reports.reports.eof_risk_report import fetch_barra_one_data as fbd
import datetime
from gcm.data import DataAccess, DataSource
import pyodbc
from _legacy.core.ReportStructure.report_structure import (
    ReportingEntityTypes,
    ReportType,
    AggregateInterval,
)
from _legacy.core.Runners.investmentsreporting import (
    InvestmentsReportRunner,
)
from gcm.inv.scenario import Scenario
from _legacy.core.reporting_runner_base import (
    ReportingRunnerBase,
)
from gcm.Dao.DaoRunner import DaoRunner, DaoRunnerConfigArgs
from gcm.Dao.DaoSources import DaoSource


class EofExternalData(ReportingRunnerBase):
    def __init__(self, as_of_date, runner, scenario: list):
        self._as_of_date = as_of_date
        self._scenario = scenario
        self._runner = runner

    def get_header_info(self):
        header = pd.DataFrame({"header_info": [self._as_of_date]})
        return header

    def sector_exposures(self, dao, params):
        raw = """
        select gd.SectorName, id.Id,
        sum(se.DeltaAdjExp)/avg(e.EndingNav) as net_delta
        from [Reporting].SecurityExposureDisaggIndices se
        left join AnalyticsData.IssuerDimn id
        on se.IssuerId = id.Id
        left join AnalyticsData.GicsDimn gd
        on id.GicsSubindustryId = gd.SubIndustryId
        LEFT JOIN AnalyticsData.PortfolioDimn d
        ON se.PortfolioName = d.Name
        LEFT JOIN AnalyticsData.Portfolio e
        ON d.Id = e.PortfolioId and e.Date = se.Date
        where d.Id = 99999 and se.Date = ?
        group by gd.SectorName, id.Id
        """
        gross = pd.read_sql(
            raw, dao.data_engine.session.bind, params=tuple(params)
        )
        gross["SectorName"] = gross["SectorName"].replace(
            "CONSUMER STAPLES", "OTHER"
        )
        gross["SectorName"] = gross["SectorName"].replace(
            "UTILITIES", "OTHER"
        )
        gross["SectorName"] = gross["SectorName"].replace(
            "MATERIALS", "OTHER"
        )
        gross["SectorName"] = gross["SectorName"].fillna("OTHER")

        gross["l/s"] = gross["net_delta"].apply(
            lambda x: "L" if x >= 0 else "S"
        )
        net_exp = gross.groupby("SectorName")["net_delta"].sum()
        ls = gross.groupby(["SectorName", "l/s"])["net_delta"].sum()
        ls = ls.reset_index().pivot_table("net_delta", "SectorName", "l/s")
        ls["% total"] = (
            ls[["L", "S"]].abs().sum(axis=1)
            / ls[["L", "S"]].abs().sum(axis=1).sum()
        )
        gross_exp = pd.concat([net_exp, ls], axis=1)
        # import pdb
        # pdb.set_trace()
        # gross_exp["tmp"] = [1, 2, 3, 4, 5, 6, 7, 9, 8]
        # gross_exp.sort_values("tmp", inplace=True)
        total = pd.DataFrame(gross_exp.sum()).transpose()
        total.index = ["Total"]
        gross_exp = pd.concat([gross_exp, total])
        return gross_exp

    def region_exposures(self, dao, params):
        raw = """
        select CountryOfExposure, id.Id,
        sum(se.DeltaAdjExp)/avg(e.EndingNav) as net_delta
        from [Reporting].SecurityExposureDisaggIndices se
        left join AnalyticsData.IssuerDimn id
        on se.IssuerId = id.Id
        left join AnalyticsData.GicsDimn gd
        on id.GicsSubindustryId = gd.SubIndustryId
        LEFT JOIN AnalyticsData.PortfolioDimn d
        ON se.PortfolioName = d.Name
        LEFT JOIN AnalyticsData.Portfolio e
        ON d.Id = e.PortfolioId and e.Date = se.Date
        where d.Id = 99999 and se.Date = ?
        group by CountryOfExposure, id.Id
        """
        reg = pd.read_sql(
            raw, dao.data_engine.session.bind, params=tuple([params[0]])
        )

        reg_map = pd.read_csv(os.path.join(os.path.dirname(__file__), ".//regionmap.csv"))
        reg = pd.merge(reg, reg_map, how="left", on="CountryOfExposure")
        reg["l/s"] = reg["net_delta"].apply(
            lambda x: "L" if x >= 0 else "S"
        )
        reg_list = ["Americas DM", "Europe DM", "Asia ex Japan"]
        reg["RegionMarket"] = reg["RegionMarket"].apply(
            lambda x: x if x in reg_list else "Other"
        )
        reg["RegionMarket"] = reg["RegionMarket"].fillna("Other")
        reg = (
            reg.groupby(["RegionMarket", "l/s"])["net_delta"]
            .sum()
            .reset_index()
            .pivot_table("net_delta", "RegionMarket", "l/s")
            .reset_index()
        )
        reg = reg.groupby("RegionMarket").sum()
        reg["% Total"] = (
            reg.abs().sum(axis=1) / reg.abs().sum(axis=1).sum()
        )
        reg["Net Exposure"] = reg["L"] + reg["S"]
        reg = reg * 100
        reg["tmp"] = [1, 3, 2, 4]
        reg.sort_values("tmp", inplace=True)
        del reg["tmp"]
        reg = reg[["Net Exposure", "L", "S", "% Total"]]
        reg["% Total"] = reg["% Total"] / 100.0
        total = pd.DataFrame(reg.sum()).transpose()
        total.index = ["Total"]
        reg = pd.concat([reg, total])

        barra_reg = params[1].iloc[1:]
        barra_reg = barra_reg[
            [
                "Grouping: Country Of Exposure+Long-Short",
                "%CR to Total Risk",
            ]
        ]
        barra_reg.columns = ["CountryOfExposure", "%CR to Total Risk"]
        barra_reg = pd.merge(
            barra_reg,
            reg_map[["CountryOfExposure", "RegionMarket"]],
            how="left",
            on="CountryOfExposure",
        )
        barra_reg["RegionMarket"] = barra_reg["RegionMarket"].fillna(
            "Other"
        )
        barra_reg["RegionMarket"] = barra_reg["RegionMarket"].apply(
            lambda x: x if x in reg_list else "Other"
        )
        barra_reg = barra_reg.groupby("RegionMarket")[
            "%CR to Total Risk"
        ].sum()
        barra_reg = pd.concat([barra_reg, pd.Series(1, index=["Total"])])
        barra_reg = barra_reg.loc[reg.index]
        reg["barra"] = barra_reg  # *100
        # reg['barra'].loc['Total'] = 1.0
        return reg

    def cap_exposures(self, dao, params):
        raw = """
        select case when MarketCapitalization <= 2000000000 then 'Small Cap'
	        when MarketCapitalization <= 10000000000 then 'Mid Cap'
		    when MarketCapitalization > 10000000000 then 'Large Cap'
			else 'Other' end as market_cap,
        id.Id,sum(se.DeltaAdjExp)/avg(e.EndingNav) as net_delta
        from [Reporting].SecurityExposureDisaggIndices se
        left join AnalyticsData.IssuerDimn id
        on se.IssuerId = id.Id
        left join AnalyticsData.GicsDimn gd
        on id.GicsSubindustryId = gd.SubIndustryId
        LEFT JOIN AnalyticsData.PortfolioDimn d
        ON se.PortfolioName = d.Name
        LEFT JOIN AnalyticsData.Portfolio e
        ON d.Id = e.PortfolioId and e.Date = se.Date
        left join [AnalyticsData].[SecurityMarketData] sm 
        on se.SecurityId = sm.SecurityId and se.Date = sm.HoldingDate 
        where d.Id = 99999 and se.Date = ?
        group by MarketCapitalization, id.Id
        """
        cap = pd.read_sql(
            raw, dao.data_engine.session.bind, params=tuple([params[0]])
        )
        cap["l/s"] = cap["net_delta"].apply(
            lambda x: "L" if x >= 0 else "S"
        )
        cap = (
            cap.groupby(["market_cap", "l/s"])["net_delta"]
            .sum()
            .reset_index()
            .pivot_table("net_delta", "market_cap", "l/s")
        )
        cap["% Total"] = (
            cap.abs().sum(axis=1) / cap.abs().sum(axis=1).sum()
        )
        cap["Net Exposure"] = cap["L"] + cap["S"]
        cap = cap[["Net Exposure", "L", "S", "% Total"]] * 100
        cap["% Total"] = cap["% Total"] / 100.0
        total = pd.DataFrame(cap.sum()).transpose()
        total.index = ["Total"]
        cap = pd.concat([cap, total])
        cap["tmp"] = [1, 2, 4, 3, 5]
        cap = cap.sort_values("tmp")
        del cap["tmp"]

        barra_cap = params[1]
        barra_cap = barra_cap[
            [
                "Grouping: Market Capitalization+Holdings",
                "%CR to Total Risk",
            ]
        ]
        barra_cap.columns = ["Cap", "%CR to Total Risk"]
        barra_cap["Cap"] = barra_cap["Cap"].fillna("Other")
        barra_cap = barra_cap.iloc[1:]
        cap_dict = {
            "Large": "Large Cap",
            "Micro": "Small Cap",
            "Mega": "Large Cap",
            "Mid": "Mid Cap",
            "Small": "Small Cap",
            "Other": "Other",
        }
        new_cap = []
        for i, row in barra_cap.iterrows():
            new_cap.append(cap_dict[row["Cap"]])

        barra_cap["Cap"] = new_cap
        barra_cap = barra_cap.groupby("Cap")["%CR to Total Risk"].sum()
        barra_cap = pd.concat([barra_cap, pd.Series(1, index=["Total"])])
        cap["% risk"] = barra_cap  # *100
        return cap

    def curr_top(self, dao, params):
        raw = """
        select top(10) id.Id,id.Name,
           sum(se.DeltaAdjExp)/avg(gross.gross) as net_delta
        from [Reporting].SecurityExposureDisaggIndices se
	        left join AnalyticsData.IssuerDimn id
	        on se.IssuerId = id.Id
	        LEFT JOIN AnalyticsData.PortfolioDimn d
	        ON se.PortfolioName = d.Name
	        LEFT JOIN AnalyticsData.Portfolio e
	        ON d.Id = e.PortfolioId and e.Date = se.Date
	        left join (
		        select Date, sum(abs(exposure)) gross
		        from (
			        select nds.Date, SecurityId, sum(DeltaAdjExp) exposure
			        from [Reporting].SecurityExposureDisaggIndices nds
				        LEFT JOIN AnalyticsData.Portfolio p
				        ON nds.Date = p.Date
			        where nds.PortfolioName = 'EOF'  --and nds.Date > '1/1/2022' 
				        and p.PortfolioId = 99999 and nds.Date = p.Date
				        group by nds.Date, SecurityId) nets
		        group by Date
	        ) gross
	        on se.Date = gross.Date
        where d.Id = 99999 and se.Date = ?
        group by id.Id, id.Name
        order by net_delta desc
        """
        top_long = pd.read_sql(
            raw, dao.data_engine.session.bind, params=tuple(params)
        )
        raw = """
        select top(10) id.Id,id.Name,
           sum(se.DeltaAdjExp)/avg(gross.gross) as net_delta
        from [Reporting].SecurityExposureDisaggIndices se
	        left join AnalyticsData.IssuerDimn id
	        on se.IssuerId = id.Id
	        LEFT JOIN AnalyticsData.PortfolioDimn d
	        ON se.PortfolioName = d.Name
	        LEFT JOIN AnalyticsData.Portfolio e
	        ON d.Id = e.PortfolioId and e.Date = se.Date
	        left join (
		        select Date, sum(abs(exposure)) gross
		        from (
			        select nds.Date, SecurityId, sum(DeltaAdjExp) exposure
			        from [Reporting].SecurityExposureDisaggIndices nds
				        LEFT JOIN AnalyticsData.Portfolio p
				        ON nds.Date = p.Date
			        where nds.PortfolioName = 'EOF'  --and nds.Date > '1/1/2022' 
				        and p.PortfolioId = 99999 and nds.Date = p.Date
				        group by nds.Date, SecurityId) nets
		        group by Date
	        ) gross
	        on se.Date = gross.Date
        where d.Id = 99999 and se.Date = ?
        group by id.Id, id.Name
        order by net_delta
        """
        top_short = pd.read_sql(
            raw, dao.data_engine.session.bind, params=tuple([params])
        )
        raw = """
        select se.Date, id.Id,id.Name,
          sum(se.DeltaAdjExp)/avg(e.EndingNav) as net_delta
        from [Reporting].SecurityExposureNoDisaggIndices se
        left join AnalyticsData.IssuerDimn id
        on se.IssuerId = id.Id
        left join AnalyticsData.GicsDimn gd
        on id.GicsSubindustryId = gd.SubIndustryId
        LEFT JOIN AnalyticsData.PortfolioDimn d
        ON se.PortfolioName = d.Name
        LEFT JOIN AnalyticsData.Portfolio e
        ON d.Id = e.PortfolioId and e.Date = se.Date
        where d.Id = 99999 and se.Date <= ? and InstrumentType in ('Contract For Differences','Equity Security','American Depositary Receipt (Equity)','Global Depositary Receipt (Equity)')			
        group by se.Date,id.Id, id.Name order by net_delta asc
        """
        cnt = pd.read_sql(raw, dao.data_engine.session.bind, params=tuple([params]))
        cnt['Date'] = pd.to_datetime(cnt['Date'])
        curr_cnt = cnt[cnt["Date"] == pd.to_datetime(params[0], format="%m-%d-%Y")]

        raw = """
        select IssuerId, SpecificResidualRisk
        from AnalyticsData.SecurityStandaloneRisk a
        left join AnalyticsData.SecurityDimn b
        on a.SecurityId = b.Id
        where HoldingDate = ? and InstrumentType != 'Equity Option'
        """

        idio = pd.read_sql(
            raw, dao.data_engine.session.bind, params=tuple([params])
        )
        idio.columns = ["Id", "idio"]
        top_long = pd.merge(top_long, idio, how="left", on="Id")
        top_long["wt"] = (
            top_long["net_delta"] / top_long["net_delta"].sum()
        )
        wtd_idio_long = top_long["wt"].dot(top_long["idio"])
        top_short = pd.merge(top_short, idio, how="left", on="Id")
        top_short["wt"] = (
            top_short["net_delta"].abs()
            / top_short["net_delta"].abs().sum()
        )
        wtd_idio_short = top_short["wt"].dot(top_short["idio"])
        curr_idio = [
            top_long["idio"].iloc[0],
            wtd_idio_long,
            np.nan,
            top_short["idio"].iloc[0],
            wtd_idio_short,
            np.nan,
        ]

        curr_gmv = [
            top_long["net_delta"].iloc[0],
            top_long["net_delta"].sum(),
            len(curr_cnt[curr_cnt["net_delta"] > 0]),
            top_short["net_delta"].iloc[0],
            top_short["net_delta"].sum(),
            len(curr_cnt[curr_cnt["net_delta"] < 0]),
        ]
        print(5)
        return [curr_gmv, curr_idio]

    def get_top_stats(self, df, dao, params):
        curr_dt = pd.to_datetime(params[0])
        df = df[df["Name"] != "US Dollar"]
        df = df[~df["Name"].str.contains(" SPY ")]
        df_long = df[df["net_delta"] > 0]
        df_long["rank"] = df_long.groupby("Date")["net_delta"].rank(
            ascending=False
        )
        df_long_max = df_long[df_long["rank"] == 1]
        df_long_max = (
            df_long_max.groupby("Name")["net_delta"]
            .max()
            .sort_values(ascending=False)
            .reset_index()
        )

        df_short = df[df["net_delta"] < 0]
        df_short["rank"] = df_short.groupby("Date")["net_delta"].rank(
            ascending=True
        )
        df_short_max = df_short[df_short["rank"] == 1]
        df_short_max = (
            df_short_max.groupby("Name")["net_delta"]
            .max()
            .sort_values(ascending=True)
            .reset_index()
        )

        raw = """
        select se.Date, id.Id,id.Name,
          sum(se.DeltaAdjExp)/avg(e.EndingNav) as net_delta
        from [Reporting].SecurityExposureNoDisaggIndices se
        left join AnalyticsData.IssuerDimn id
        on se.IssuerId = id.Id
        left join AnalyticsData.GicsDimn gd
        on id.GicsSubindustryId = gd.SubIndustryId
        LEFT JOIN AnalyticsData.PortfolioDimn d
        ON se.PortfolioName = d.Name
        LEFT JOIN AnalyticsData.Portfolio e
        ON d.Id = e.PortfolioId and e.Date = se.Date
        where d.Id = 99999 and se.Date <= ? and InstrumentType in ('Contract For Differences','Equity Security','American Depositary Receipt (Equity)','Global Depositary Receipt (Equity)')			
        group by se.Date,id.Id, id.Name order by net_delta asc
        """
        # import pdb
        # pdb.set_trace()
        cnt = pd.read_sql(raw, dao.data_engine.session.bind, params=tuple(params))
        cnt['Date'] = pd.to_datetime(cnt['Date'])
        cnt = cnt[
            (cnt["Date"] <= curr_dt)
            & (cnt["Date"] > curr_dt - pd.DateOffset(years=1))
        ]

        ttm_max_long = df_long_max["net_delta"].max()
        ttm_max_long_10 = (
            df_long[df_long["rank"] <= 10]
            .groupby("Date")["net_delta"]
            .sum()
            .max()
        )
        cnt_long = (
            cnt[cnt["net_delta"] > 0]
            .groupby("Date")
            .apply(lambda x: len(x["Id"].unique()))
            .max()
        )

        ttm_max_short = df_short_max["net_delta"].min()
        ttm_max_short_10 = (
            df_short[df_short["rank"] <= 10]
            .groupby("Date")["net_delta"]
            .sum()
            .min()
        )
        cnt_short = (
            cnt[cnt["net_delta"] < 0]
            .groupby("Date")
            .apply(lambda x: len(x["Id"].unique()))
            .max()
        )
        arr = [
            ttm_max_long,
            ttm_max_long_10,
            cnt_long,
            ttm_max_short,
            ttm_max_short_10,
            cnt_short,
        ]
        return arr

    def ttm_top(self, dao, params):
        raw = """
        select se.Date,id.Id,id.Name,
           sum(se.DeltaAdjExp)/avg(gross.gross) as net_delta
        from [Reporting].SecurityExposureDisaggIndices se
	        left join AnalyticsData.IssuerDimn id
	        on se.IssuerId = id.Id
	        LEFT JOIN AnalyticsData.PortfolioDimn d
	        ON se.PortfolioName = d.Name
	        LEFT JOIN AnalyticsData.Portfolio e
	        ON d.Id = e.PortfolioId and e.Date = se.Date
	        left join (
		        select Date, sum(abs(exposure)) gross
			        from (
				        select nds.Date, SecurityId, sum(DeltaAdjExp) exposure
				        from [Reporting].SecurityExposureDisaggIndices nds
					        LEFT JOIN AnalyticsData.Portfolio p
					        ON nds.Date = p.Date
				        where nds.PortfolioName = 'EOF'  --and nds.Date > '1/1/2022' 
					          and p.PortfolioId = 99999 and nds.Date = p.Date
					          group by nds.Date, SecurityId) nets
		        group by Date 
	        ) gross
	        on se.Date = gross.Date
        where d.Id = 99999 
        group by se.Date,id.Id, id.Name
        """
        df = pd.read_sql(raw, dao.data_engine.session.bind)
        df["Date"] = pd.to_datetime(df["Date"])
        curr_dt = pd.to_datetime(params[0])
        # df = df[(df['Date'] <= '2022-12-30') & (df['Date'] > '2021-12-30')]
        df_ttm = df[
            (df["Date"] <= curr_dt)
            & (df["Date"] > curr_dt - pd.DateOffset(years=1))
        ]
        arr_ttm = self.get_top_stats(df_ttm, dao, params)

        df_itd = df[df["Date"] <= curr_dt]
        arr_itd = self.get_top_stats(df_itd, dao, params)
        return [arr_ttm, arr_itd]

    def get_stress(self, zone, dl_client):
        barra_fac = fbd.load_barra_one_data_from_datalake(
            folder_path=f"{zone}/barraone/position/eof_add_on_reports/",
            report_pkg="EOF Add-On Reports w LT",
            as_of_date=datetime.date(self._as_of_date.year, self._as_of_date.month, self._as_of_date.day),
            csv_partial_name='FactorGroup_ContribRisk',
            data_lake_client=dl_client,
            skip=17
        )
        port_var = barra_fac['Portfolio VaR'].iloc[0]
         
        barra_stress = fbd.load_barra_one_data_from_datalake(
            folder_path=f"{zone}/barraone/position/eof_stress_test/",
            report_pkg="eof_stress_test",
            as_of_date=datetime.date(self._as_of_date.year, self._as_of_date.month, self._as_of_date.day),
            csv_partial_name='eof_stress_test',
            data_lake_client=dl_client,
            skip = 11
        )
        port_var = barra_fac['Portfolio VaR'].iloc[0]
        init_mkt_val = barra_stress.iloc[0]['Initial Market Value']
        var = port_var / init_mkt_val
        plus_20 = barra_stress['20% Equity Correlated: $P&L'].iloc[0] / init_mkt_val
        minus_20 = barra_stress['20% Equity Correlated: $P&L'].iloc[0] / init_mkt_val
        stress = pd.DataFrame([var, plus_20, minus_20])
        return stress

    def get_liquidity(self, dao, params):
        raw = """
        select * from Reporting.SecurityExposureNoDisaggIndices
        where Date = ?
        """
        eof = pd.read_sql(raw, dao.data_engine.session.bind, params=tuple([params]))
        holding = eof.groupby("SecurityId")["Holding"].sum()
        deltaadjexp = eof.groupby("SecurityId")["DeltaAdjExp"].sum()
        holdings = pd.concat([holding, deltaadjexp], axis=1)
        holdings = pd.merge(
            holdings,
            eof[["SecurityId", "InstrumentType"]],
            how="left",
            on="SecurityId",
        )
        holdings["Date"] = pd.to_datetime(params[0])
        holdings = holdings[
            holdings["InstrumentType"] != "ExchangeTraded Fund"
        ]

        raw = """
        select * from AnalyticsData.SecurityMarketData
        where AnalysisDate = ?
        """
        volume = pd.read_sql(
            raw, dao.data_engine.session.bind, params=tuple([params])
        ).rename(columns={"HoldingDate": "Date"})[
            ["SecurityId", "Date", "AverageDailyTradingVolume"]
        ]
        volume["Date"] = pd.to_datetime(volume["Date"])

        holdings_by_adtv = holdings.merge(
            volume, on=["Date", "SecurityId"], how="left"
        )
        holdings_by_adtv = holdings_by_adtv[
            ~holdings_by_adtv["AverageDailyTradingVolume"].isnull()
        ]
        holdings_by_adtv = holdings_by_adtv[
            holdings_by_adtv["AverageDailyTradingVolume"] > 0.01
        ]
        holdings_by_adtv["days"] = holdings_by_adtv["Holding"].abs() / (
            0.2 * holdings_by_adtv["AverageDailyTradingVolume"]
        )
        holdings_by_adtv["gross_exp"] = holdings_by_adtv[
            "DeltaAdjExp"
        ].abs()

        holdings_by_adtv["pct_of_gross_exp"] = np.asarray(holdings_by_adtv.groupby(
            "Date"
        )["gross_exp"].apply(lambda x: x / float(x.sum())))
        holdings_by_adtv["day_category"] = holdings_by_adtv.days.apply(
            lambda x: "1"
            if x < 1
            else "1-5"
            if x < 5
            else "5-30"
            if x < 30
            else "30"
            if x > 30
            else "1"
        )
        liquidity_days = (
            holdings_by_adtv[["pct_of_gross_exp", "day_category"]]
            .groupby(["day_category"])
            .sum()
            .reindex(["1", "1-5", "5-30", "30"])
        )
        return liquidity_days

    def return_attribution(self, dao, params):
        raw = """
          select Date, FactorGroup, sum(Attribution) Attribution
          from 
          (select 
	      Date,
	      CASE WHEN FactorGroup in ('Country', 'Currency Risk', 'Market') THEN 'Market' ELSE FactorGroup END as FactorGroup, 
	      sum(TotalAttribPctFundNav) Attribution
          from Reporting.PortfolioFactorAttribution
          where PortfolioName = 'EOF' and RiskModel = 'GEMLTL'
          group by Date, FactorGroup) attribRemapped
          group by Date, FactorGroup
          order by Date, FactorGroup
        """
        df = pd.read_sql(raw, dao.data_engine.session.bind).pivot_table(
            "Attribution", "Date", "FactorGroup"
        )
        del df["Unassigned"]
        df = df.cumsum().reset_index()
        df['Date'] = pd.to_datetime(df['Date'])
        df = df[df["Date"] <= pd.to_datetime(params[0], format="%m-%d-%Y")]
        return df

    def historic_exposure_vol(self, dao, params):
        dt = pd.to_datetime(params[0])
        raw = """
        select YEAR, MONTH, avg(net) avg_net						
        from (						
	        select nds.Date, year(nds.Date) YEAR, month(nds.Date) MONTH, sum(DeltaAdjExp)/avg(p.EndingNav) net					
	        from [Reporting].SecurityExposureNoDisaggIndices nds					
		        LEFT JOIN AnalyticsData.Portfolio p				
		        ON nds.Date = p.Date				
	        where nds.PortfolioName = 'EOF'  and nds.Date >= '4/1/2022' 					
		          and p.PortfolioId = 99999 and nds.Date = p.Date				
	        group by nds.Date, year(nds.Date), month(nds.Date) ) net_hist					
        group by YEAR, MONTH					
        order by YEAR, MONTH
        """
        avg_net = pd.read_sql(raw, dao.data_engine.session.bind)
        avg_net["Date"] = pd.to_datetime(
            avg_net[["YEAR", "MONTH"]].assign(DAY=1)
        )
        avg_net = avg_net[avg_net["Date"] <= dt].set_index("Date")[
            "avg_net"
        ]

        raw = """
        select year(Date) YEAR, month(Date) MONTH, avg(gross)						
        from (						
	        select Date, sum(abs(exposure)) gross					
	        from (					
		        select nds.Date, SecurityId, sum(DeltaAdjExp)/avg(p.EndingNav) exposure				
		        from [Reporting].SecurityExposureNoDisaggIndices nds				
			        LEFT JOIN AnalyticsData.Portfolio p			
			        ON nds.Date = p.Date			
		        where nds.PortfolioName = 'EOF'  and nds.Date >= '4/1/2022' 				
			          and p.PortfolioId = 99999 and nds.Date = p.Date			
			          group by nds.Date, SecurityId) nets			
	        group by Date ) grss_hist					
        group by year(Date), month(Date) 						
        order by year(Date), month(Date) 
        """
        avg_gross = pd.read_sql(raw, dao.data_engine.session.bind)
        avg_gross.columns = ["YEAR", "MONTH", "avg_gross"]
        avg_gross["Date"] = pd.to_datetime(
            avg_gross[["YEAR", "MONTH"]].assign(DAY=1)
        )
        avg_gross = avg_gross[avg_gross["Date"] <= dt].set_index("Date")[
            "avg_gross"
        ]

        raw = """
        select year(HoldingDate) YEAR, month(HoldingDate) MONTH, avg(ExpVol)						
        from (						
	        select HoldingDate, sum(RiskContrib) 'ExpVol'					
	        from AnalyticsData.PortfolioContribRisk prc 					
		         left join (select distinct ExternalDescription, FactorGroup1 from Reporting.Factors) as f				
	        on prc.Factor = f.ExternalDescription					
	        where Factor != 'All'					
	        group by HoldingDate) vol_hist					
        group by  year(HoldingDate), month(HoldingDate)						
        order by  year(HoldingDate), month(HoldingDate)	
        """
        avg_vol = pd.read_sql(raw, dao.data_engine.session.bind)
        avg_vol.columns = ["YEAR", "MONTH", "avg_vol"]
        avg_vol["Date"] = pd.to_datetime(
            avg_vol[["YEAR", "MONTH"]].assign(DAY=1)
        )
        avg_vol = avg_vol[avg_vol["Date"] <= dt]
        avg_vol = avg_vol[
            avg_vol["Date"] >= pd.to_datetime("2022-04-01")
        ].set_index("Date")["avg_vol"]
        df = pd.concat(
            [avg_gross, avg_net, avg_vol], axis=1
        ).reset_index()  # [['avg_gross','avg_net','Date','avg_vol']]
        df["Year"] = df["Date"].dt.year
        df["Month"] = df["Date"].dt.month
        df = df[
            ["Year", "Month", "avg_gross", "avg_net", "Date", "avg_vol"]
        ].tail(12)
        return df

    def _get_athena_data(self, query):
        conn = pyodbc.connect(self.get_db_properties("athena.url"))
        result = pd.read_sql(query, conn)
        conn.close()
        return result

    def net_returns(self, date):
        conn = pyodbc.connect(
            "DSN=Athena;UID=RiskReadOnly;PWD=kQ!3Gy+rk9-KkN!N"
        )
        query = """
        select AsOfDate Date, ROR as MonthlyEofRtn_Net1_10
        from [dbo].[PortfolioFundCubeReturnStreamStat]
        WHERE IndexReturnStream = 4551 and NumberOfMonths = -1
        order by AsOfDate
        """
        result = pd.read_sql(query, conn)
        result["Date"] = pd.to_datetime(result["Date"])
        result = result.set_index("Date").loc[:date].reset_index()
        result.columns = ["date", "ret"]
        result["Month"] = result["date"].dt.month
        result["Year"] = result["date"].dt.year
        result_trans = (
            result.pivot_table("ret", "Year", "Month")
            .reset_index()
            .sort_values("Year", ascending=False)
        )
        result_trans["ytd"] = (
            np.asarray(
                result_trans.set_index("Year")
                .fillna(0)
                .add(1)
                .cumprod(axis=1)[12]
            )
            - 1
        )
        return [result, result_trans]

    def get_snapshot(self, dao, params):
        raw = """
        select EndingNav
        from analyticsdata.Portfolio
        where Date = ?
        """
        
        nav = pd.read_sql(
            raw, dao.data_engine.session.bind, params=tuple(params[0])
        )
        raw = """
        select sum(abs(exposure)) gross	 
        from (	 	 
 	        select SecurityId, sum(DeltaAdjExp) exposure	 
 	        from [Reporting].SecurityExposureNoDisaggIndices	 
 	        where Date = ? 
 	        group by SecurityId) nets
        """
        gross_exp = pd.read_sql(
            raw, dao.data_engine.session.bind, params=tuple(params[0])
        )
        gross_exp = float(gross_exp["gross"] / nav["EndingNav"])
        raw = """
        select sum(exposure) net	 
        from (	 	 
 	        select SecurityId, sum(DeltaAdjExp) exposure	 
 	        from [Reporting].SecurityExposureNoDisaggIndices	 
 	        where Date = ?	 
 	        group by SecurityId) nets
        """
        net_exp = pd.read_sql(
            raw, dao.data_engine.session.bind, params=tuple(params[0])
        )
        net_exp = float(net_exp["net"] / nav["EndingNav"])

        returns = params[1][0]

        ytd_net = (
            returns[returns["Year"] == pd.to_datetime(params[0]).year[0]][
                "ret"
            ]
            .add(1)
            .cumprod()
            - 1
        )[-1:]
        itd_ann_ret = (
            np.prod(1 + returns["ret"]) ** (12 / len(returns)) - 1
        )
        itd_ann_vol = returns["ret"].std(ddof=0) * np.sqrt(12)

        conn = pyodbc.connect(
            "DSN=Athena;UID=RiskReadOnly;PWD=kQ!3Gy+rk9-KkN!N"
        )
        raw = """
        select AsOfDate Date, ROR as MonthlyEofRtn_Net1_10
        from [dbo].[PortfolioFundCubeReturnStreamStat]
        WHERE IndexReturnStream = 218 and NumberOfMonths = -1
        order by AsOfDate
        """
        rfr = params[2]
        rfr = rfr.loc[returns.set_index("date").index]
        returns_ = pd.concat([returns.set_index("date"), rfr], axis=1)
        ret_rfr = returns_["ret"] - returns_["rfr"]
        itd_sharpe = (np.prod(1 + ret_rfr) ** (12 / len(ret_rfr)) - 1) / (
            ret_rfr.std(ddof=0) * np.sqrt(12)
        )

        risk_itd = returns["ret"].std(ddof=0) * np.sqrt(12)

        spx = params[3].loc[returns.set_index("date").index]
        ret_spx = pd.concat([returns.set_index("date"), spx], axis=1)
        beta_spx = self.compute_beta(ret_spx)
        max_dd = self.mdd(returns)

        nav_ = "$" + str(nav.iloc[0][0])[0:3] + "M"
        snap = pd.DataFrame(
            pd.Series(
                [
                    nav_,
                    gross_exp,
                    net_exp,
                    np.nan,
                    ytd_net.iloc[0],
                    itd_ann_ret,
                    itd_sharpe,
                    np.nan,
                    risk_itd,
                    beta_spx,
                    max_dd,
                ]
            )
        )
        return snap

    def get_rfr(self, dao, params):
        raw = """
        SELECT [PeriodDate] Date
              ,[RateOfReturn] Ror
          FROM [analyticsdata].[FinancialIndexReturnFact]
          where FinancialIndexMasterId = (select distinct MasterId from analyticsdata.FinancialIndexDimn where Name = '3M FTSE Tbill')
          order by PeriodDate
        """
        rfr = pd.read_sql(raw, dao.data_engine.session.bind)
        rfr.columns = ["date", "rfr"]
        rfr["date"] = pd.to_datetime(rfr["date"])
        rfr = rfr.set_index("date")
        return rfr

    def mdd(self, returns):
        cum_rets = returns["ret"].add(1).cumprod() - 1
        nav = ((1 + cum_rets) * 100).fillna(100)
        hwm = nav.cummax()
        dd = nav / hwm - 1
        return min(dd)

    # athena method
    def compute_beta(self, returns):
        spx_mean = returns["spx"].mean()
        eof_mean = returns["ret"].mean()
        X = 0
        den = 0
        for i, row in returns.iterrows():
            lhs = row["spx"] - spx_mean
            rhs = row["ret"] - eof_mean
            X += lhs * rhs

            den += (row["spx"] - spx_mean) ** 2
        return X / den

    def get_spx(self, dao, params):
        raw = """
        SELECT [PeriodDate] Date
              ,[RateOfReturn] Ror
          FROM [analyticsdata].[FinancialIndexReturnFact]
          where FinancialIndexMasterId = (select distinct MasterId from analyticsdata.FinancialIndexDimn where Name = 'S&P 500')
          order by PeriodDate
        """
        spx = pd.read_sql(raw, dao.data_engine.session.bind)
        spx.columns = ["date", "spx"]
        spx["date"] = pd.to_datetime(spx["date"])
        return spx.set_index("date")

    def eof_external_report(self):
        print('test')
        date = pd.to_datetime(self._as_of_date).strftime("%m-%d-%Y")
        sub = "prd"
        zone = "mscidev" if sub == "nonprd" else "msci"
        dl_client = DataAccess().get(
            DataSource.DataLake,
            target_name=f"gcmdatalake{sub}",
        )
        dt_str = pd.to_datetime(date).strftime("%Y%m%d")

        rets = self.net_returns(date)
        print("rfr")
        rfr = self._runner.execute(
            params=[date],
            source=DaoSource.PubDwh,
            operation=self.get_rfr,
        )
        print("spx")
        spx = self._runner.execute(
            params=[date],
            source=DaoSource.PubDwh,
            operation=self.get_spx,
        )
        print("snapshot")
        snapshot = self._runner.execute(
            params=[[date], rets, rfr, spx],
            source=DaoSource.InvestmentsDwh,
            operation=self.get_snapshot,
        )
        print("hist_vol")
        hist_vol = self._runner.execute(
            params=[date],
            source=DaoSource.InvestmentsDwh,
            operation=self.historic_exposure_vol,
        )
        print("attribution")
        attribution = self._runner.execute(
            params=[date],
            source=DaoSource.InvestmentsDwh,
            operation=self.return_attribution,
        )
        attribution["Date"] = (
            pd.to_datetime(attribution["Date"])
            .dt.strftime("%m/%d/%Y")
            .str.lstrip("0")
        )

        print("ttm")
        ttm = self._runner.execute(
            params=[date],
            source=DaoSource.InvestmentsDwh,
            operation=self.ttm_top,
        )

        print("gross_exp")
        gross_exp = self._runner.execute(
            params=[date],
            source=DaoSource.InvestmentsDwh,
            operation=self.sector_exposures,
        )

        # single date load
        barra_sec = fbd.load_barra_one_data_from_datalake(
            folder_path=f"{zone}/barraone/position/eof_add_on_reports/",
            report_pkg="EOF Add-On Reports w LT",
            as_of_date=datetime.date(
                self._as_of_date.year,
                self._as_of_date.month,
                self._as_of_date.day,
            ),
            csv_partial_name="Idio Vol",
            data_lake_client=dl_client,
        )
        barra_sec["%CR to Total Risk"] = (
            barra_sec["%CR to Total Risk"].str[:-1].astype(float) / 100.0
        )
        barra_sec["Grouping: GICS Sector+GICS Sub-Industry"] = barra_sec[
            "Grouping: GICS Sector+GICS Sub-Industry"
        ].fillna("Other")

        gics_order = [
            "Communication Services",
            "Consumer Discretionary",
            "Energy",
            "Financials",
            "Health Care",
            "Industrials",
            "Information Technology",
            "Real Estate",
        ]
        sec_risk = barra_sec.set_index(
            "Grouping: GICS Sector+GICS Sub-Industry"
        ).loc[gics_order]["%CR to Total Risk"]
        sec_risk = pd.concat([sec_risk, pd.Series(1 - sec_risk.sum(), index=["Other"])])
        sec_risk = pd.concat([sec_risk, pd.Series(1, index=["Total"])])
        
        gross_exp["% total risk"] = sec_risk
        gross_exp["L"] = gross_exp["L"] * 100
        gross_exp["S"] = gross_exp["S"] * 100
        gross_exp["% total"] = gross_exp["% total"]  # * 100
        gross_exp["net_delta"] = gross_exp["net_delta"] * 100
        gross_exp["% total"].loc["Total"] = 1.0

        barra_reg = fbd.load_barra_one_data_from_datalake(
            folder_path=f"{zone}/barraone/position/eof_add_on_reports/",
            report_pkg="EOF Add-On Reports w LT",
            as_of_date=datetime.date(
                self._as_of_date.year,
                self._as_of_date.month,
                self._as_of_date.day,
            ),
            csv_partial_name="Geography",
            data_lake_client=dl_client,
        )
        barra_reg["%CR to Total Risk"] = (
            barra_reg["%CR to Total Risk"].str[:-1].astype(float) / 100.0
        )

        print("reg_exp")
        reg_exp = self._runner.execute(
            params=[date, barra_reg],
            source=DaoSource.InvestmentsDwh,
            operation=self.region_exposures,
        )

        # Look Through: Composites and ETFs
        barra_cap = fbd.load_barra_one_data_from_datalake(
            folder_path=f"{zone}/barraone/position/eof_add_on_reports/",
            report_pkg="EOF Add-On Reports w LT",
            as_of_date=datetime.date(
                self._as_of_date.year,
                self._as_of_date.month,
                self._as_of_date.day,
            ),
            csv_partial_name="Market Cap",
            data_lake_client=dl_client,
        )
        barra_cap["%CR to Total Risk"] = (
            barra_cap["%CR to Total Risk"].str[:-1].astype(float) / 100.0
        )

        print("cap_exp")
        cap_exp = self._runner.execute(
            params=[date, barra_cap],
            source=DaoSource.InvestmentsDwh,
            operation=self.cap_exposures,
        )
        print("top")
        top = self._runner.execute(
            params=[date],
            source=DaoSource.InvestmentsDwh,
            operation=self.curr_top,
        )

        top_data = pd.DataFrame(ttm).transpose()
        top_data[2] = top[0]
        top_data = top_data[[2, 0, 1]]
        top_data[3] = top[1]
        top_data[3] = top_data[3] / 100
        print("stress")
        stress = self.get_stress(zone, dl_client)
        print("liq")
        liq = self._runner.execute(
            params=[date],
            source=DaoSource.InvestmentsDwh,
            operation=self.get_liquidity,
        )
        print("header_info")
        header_info = self.get_header_info()
        input_data = {
            "header_info": header_info,
            "date2": pd.DataFrame(
                [pd.to_datetime(date).strftime("%m/%d/%Y")]
            ),
            "snapshot": snapshot,
            "net_returns1": rets[1][[1, 2, 3, 4, 5, 6, 7]],
            "net_returns2": rets[1][[8, 9, 10, 11, 12, "ytd"]],
            "sector_exposure": gross_exp,
            "region_exposure": reg_exp,
            "cap_exposure": cap_exp,
            "risk": top_data,
            "stress": stress,
            "liquidity": liq,
            "attribution": attribution,
            "historical_vol": hist_vol,
            "itd_ret": pd.DataFrame(
                [
                    np.prod(1 + rets[0]["ret"]) - 1,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                ]
            ),
        }

        as_of_date = dt.combine(self._as_of_date, dt.min.time())
        with Scenario(as_of_date=as_of_date).context():
            InvestmentsReportRunner().execute(
                data=input_data,
                template="eof_external_testing.xlsx",
                save=True,
                runner=self._runner,
                entity_type=ReportingEntityTypes.manager_fund_group,
                entity_name="Equity Opps Fund Ltd",
                entity_display_name="EOF",
                entity_ids=[19163],
                entity_source=DaoSource.PubDwh,
                report_name="external_test",
                report_type=ReportType.Risk,
                report_frequency="Daily",
                aggregate_intervals=AggregateInterval.Daily,
            )

    def run(self, **kwargs):
        return self.eof_external_report()


class EofReport(ReportingRunnerBase):
    def __init__(self, runner, as_of_date):
        super().__init__(runner=Scenario.get_attribute("dao"))
        self._as_of_date = Scenario.get_attribute("as_of_date")

    def get_header_info(self):
        header = pd.DataFrame({"header_info": [self._as_of_date]})
        return header

    def get_sector_exposures(self):
        return 0

    def generate_eof_report(self):
        header_info = self.get_header_info()


if __name__ == "__main__":
    as_of_date = "2023-08-08"
    scenario = ["EOF External"]
    as_of_date = dt.strptime(as_of_date, "%Y-%m-%d").date()

    runner = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.InvestmentsDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                }
            },
            DaoSource.PubDwh.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
            DaoSource.ReportingStorage.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
        },
    )

    runner_pub = DaoRunner(
        container_lambda=lambda b, i: b.config.from_dict(i),
        config_params={
            DaoRunnerConfigArgs.dao_global_envs.name: {
                DaoSource.PubDwh.name: {
                    "Environment": "prd",
                    "Subscription": "prd",
                }
            },
            DaoSource.PubDwh.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
            DaoSource.ReportingStorage.name: {
                "Environment": "prd",
                "Subscription": "prd",
            },
        },
    )

    with Scenario(dao=runner, as_of_date=as_of_date).context():
        input_data = EofExternalData(
            # runner = Scenario.get_attribute("dao"),
            as_of_date=as_of_date,
            scenario=["EOF External"],
        ).execute()
