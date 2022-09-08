import os
import glob
from PyPDF2 import PdfFileMerger
from gcm.inv.reporting.core.reporting_runner_base import ReportingRunnerBase
from gcm.inv.dataprovider.portfolio import Portfolio
from os.path import exists


class ReportBinder(ReportingRunnerBase):
    def __init__(self, runner, as_of_date, portfolio_acronyms=None):
        super().__init__(runner=runner)
        self._as_of_date = as_of_date
        self._portfolio = Portfolio(acronyms=portfolio_acronyms)

    def aggregate_reports(self):
        file_path = "C:/Users/CMCNAMEE/OneDrive - GCM Grosvenor/Desktop/tmp/"
        path1 = "Performance Quality_"
        path2 = "_PFUND_Risk_2022-03-31.pdf"
        holdings = self._portfolio.get_holdings(allocation_date=self._as_of_date)

        portfolios = holdings["Acronym"].unique()
        for portfolio in portfolios:
            portfolio_path = "Performance Quality_" + portfolio + "_PORTFOLIO_Risk_2022-03-31.pdf"

            investments = holdings[holdings["Acronym"] == portfolio]["InvestmentGroupName"]

            investment_paths = [file_path + path1 + fund + path2 for fund in list(investments)]
            pdfs = [file_path + portfolio_path] + investment_paths
            pdfs = [path for path in pdfs if exists(path)]
            merger = PdfFileMerger()

            for pdf in pdfs:
                merger.append(pdf)

            merger.write(file_path + "Performance Quality_" + portfolio + "_FundAggregate_Risk_2022-03-31.pdf")
            merger.close()

        # combine all pfunds
        os.chdir(file_path)
        pfunds = [file_path + file for file in glob.glob("*PFUND_Risk*.pdf")]
        merger = PdfFileMerger()

        for pdf in pfunds:
            merger.append(pdf)

        merger.write(file_path + "Performance Quality_" + "AllActive" + "_PFUND_Risk_2022-03-31.pdf")
        merger.close()

        # combine all funds
        funds = [file_path + file for file in glob.glob("*PORTFOLIO_Risk*.pdf")]
        merger = PdfFileMerger()

        for pdf in funds:
            merger.append(pdf)

        merger.write(file_path + "Performance Quality_" + "AllActive" + "_PORTFOLIO_Risk_2022-03-31.pdf")
        merger.close()

    def run(self, **kwargs):
        self.aggregate_reports()
        return True
