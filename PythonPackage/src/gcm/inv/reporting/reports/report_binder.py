from PyPDF2 import PdfFileMerger
from .reporting_runner_base import ReportingRunnerBase
from gcm.inv.dataprovider.portfolio_holdings import PortfolioHoldings
from gcm.inv.dataprovider.pub_dwh.pub_portfolio_holdings import PubPortfolioHoldingsQuery
from gcm.inv.dataprovider.entity_master import EntityMaster
from os.path import exists


class ReportBinder(ReportingRunnerBase):

    def __init__(self, runner, as_of_date, portfolio_acronyms):
        super().__init__(runner=runner)
        pub_portfolio_holdings_query = PubPortfolioHoldingsQuery(runner=runner, as_of_date=as_of_date)
        entity_master = EntityMaster(runner=runner, as_of_date=as_of_date)
        self._as_of_date = as_of_date
        self._portfolio_holdings = PortfolioHoldings(pub_portfolio_holdings_query=pub_portfolio_holdings_query,
                                                     entity_master=entity_master)
        self._portfolio_acronyms = portfolio_acronyms

    def aggregate_reports(self):
        file_path = "C:/Users/CMCNAMEE/OneDrive - GCM Grosvenor/Desktop/tmp/"
        path1 = 'ARS PFUND_PFUND_PerformanceQuality_'
        path2 = '_2022-03-31.pdf'
        holdings = self._portfolio_holdings.get_portfolio_holdings(allocation_date=self._as_of_date,
                                                                   portfolio_acronyms=portfolio_acronyms)

        portfolios = holdings['Acronym'].unique()
        for portfolio in portfolios:
            investments = holdings[holdings['Acronym'] == portfolio]['InvestmentGroupName']

            pdfs = [file_path + path1 + fund + path2 for fund in list(investments)]
            pdfs = [path for path in pdfs if exists(path)]
            merger = PdfFileMerger()

            for pdf in pdfs:
                merger.append(pdf)

            merger.write(file_path + portfolio + "_PerformanceQuality.pdf")
            merger.close()

    def run(self, **kwargs):
        self.aggregate_reports()
        return True
