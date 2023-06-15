import math
from functools import cached_property

from ....core.report_structure import (
    ReportStructure,
    ReportMeta,
)
from gcm.Dao.DaoRunner import AzureDataLakeDao
from ....core.report_structure import (
    EntityDomainTypes,
    AvailableMetas,
    ReportType,
    Frequency,
    FrequencyType,
    AggregateInterval,
    ReportConsumer,
    Calendar,
    EntityDomainProvider,
)

import pandas as pd
from ....core.components.report_table import ReportTable
from typing import List, Callable
from ...report_names import ReportNames
from ..utils.pvm_performance_utils.pvm_performance_helper import (
    PvmPerformanceHelper,
)
from gcm.inv.scenario import Scenario
import datetime as dt
from ....core.components.report_workbook_handler import (
    ReportWorkBookHandler,
)
from ....core.components.report_worksheet import ReportWorksheet
from gcm.inv.dataprovider.entity_provider.entity_domains.portfolio import (
    PortfolioEntityProvider,
)
from gcm.inv.utils.parsed_args.parsed_args import ParsedArgs
from gcm.inv.dataprovider.entity_provider.entity_domains.synthesis_unit.type_controller import (
    SynthesisUnitType,
    get_synthesis_unit_provider_by_type,
    PvmDealAssetClass,
)


# Run all PEREI entities:
# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-12-31&ReportName=PvmPerformanceBreakoutReport&test=True&frequency=Quarterly&save=True&aggregate_interval=Multi&EntityDomainTypes=Portfolio&GetBy=PEREI_Entities
# Run a single entity:
# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-12-31&ReportName=PvmPerformanceBreakoutReport&test=True&frequency=Quarterly&save=True&aggregate_interval=Multi&EntityDomainTypes=Portfolio&EntityNames=[%22The%20Consolidated%20Edison%20Pension%20Plan%20Master%20Trust%20-%20GCM%20PE%20Account%22]
# For synthesisunits:
# http://localhost:7071/orchestrators/ReportOrchestrator?as_of_date=2022-12-31&ReportName=PvmPerformanceBreakoutReport&test=True&frequency=Quarterly&save=True&aggregate_interval=Multi&EntityDomainTypes=SynthesisUnit&SynthesisUnitType=PvmDealAssetClass
class PvmPerformanceBreakoutReport(ReportStructure):
    def __init__(self, report_meta: ReportMeta,
                 recursion_iterate_controller,
                 attributes_needed,
                 iteration_number):
        super().__init__(
            ReportNames.PvmPerformanceBreakoutReport, report_meta
        )
        self.recursion_iterate_controller = recursion_iterate_controller
        self.attributes_needed = attributes_needed
        self.iteration_number = iteration_number


    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["PvmPerformanceBreakdown_Template.xlsx"],
            # path=["TWROR_Template_hitrate.xlsx"],
        )

    @classmethod
    def available_metas(cls):
        return AvailableMetas(
            report_type=ReportType.Performance,
            frequencies=[
                Frequency(FrequencyType.Quarterly, Calendar.AllDays),
            ],
            aggregate_intervals=[AggregateInterval.Multi],
            consumer=ReportConsumer(
                horizontal=[ReportConsumer.Horizontal.FIRM],
                vertical=ReportConsumer.Vertical.PE,
            ),
            entity_groups=[
                EntityDomainTypes.Portfolio,
                EntityDomainTypes.InvestmentManager,
                EntityDomainTypes.SynthesisUnit,
            ],
        )

    def report_name_metadata(self):
        domain_name = self.report_meta.entity_domain.name
        return f"PE {domain_name} Performance Breakout"

    @classmethod
    def standard_entity_get_callable(
        cls, domain: EntityDomainProvider, pargs: ParsedArgs
    ) -> Callable[..., pd.DataFrame]:
        if domain.domain_table.domain == EntityDomainTypes.Portfolio:
            port: PortfolioEntityProvider = domain
            return port.get_pe_only_portfolios
        if domain.domain_table.domain == EntityDomainTypes.SynthesisUnit:
            syn_unit_type = SynthesisUnitType[pargs.SynthesisUnitType]
            syn_unit = get_synthesis_unit_provider_by_type(syn_unit_type)
            return syn_unit.get_all_in_unit
        else:
            return domain.get_perei_med_entities

    def assign_components(self) -> List[ReportWorkBookHandler]:
        entity_name: str = self.report_meta.entity_info[
            "EntityName"
        ].unique()[0]

        if (
            self.report_meta.entity_domain
            == EntityDomainTypes.SynthesisUnit
        ):
            # example: this is "Private Eqtuiy"
            # get all deals associated with Private Equity
            deals_within_scope: pd.DataFrame = (
                PvmDealAssetClass().get_deals_for_asset_class(entity_name)
            )
            deals_within_scope.drop_duplicates(inplace=True)
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        domain = self.report_meta.entity_domain
        entity_info = self.report_meta.entity_info

        # actually calls report process
        p = PvmPerformanceHelper(domain,
                                 entity_info=entity_info,
                                 recursion_iterate_controller=self.recursion_iterate_controller,
                                 attributes_needed=self.attributes_needed,
                                 iteration_number=self.iteration_number)
        final_data: dict = p.generate_components_for_this_entity(
            as_of_date,
        )

        tables: List[ReportTable] = []
        for k, v in final_data.items():
            this_table = ReportTable(k, v)
            tables.append(this_table)


        # below is this-report specific logic to derive render params
        # other reports may use different logic
        sheet_name = "Industry Breakdown"
        primary_named_range = "Data"
        primary_df = [
            x.df for x in tables if x.component_name == primary_named_range
        ][0]

        # trim rows for all ranges in this sheet
        # regions_to_trim: List[str] = [x.component_name for x in tables]

        # 22 = number of excel header rows before primary_df range starts (not scalable)
        print_region = "B1:AC" + str(len(primary_df) + 21)
        # print_region = "B1:AC50"
        hide_columns = []
        # if (self.iteration_number + 1) <= 3:
        #     page_number = int(math.ceil((len(primary_df) + 20) / 77) + 1)
        # else:
        #     page_number = None

        # TODO: check portfolio inception date to set these dynamically
        if primary_df.loc[0, "3Y_AnnRor"] is None:
            # hide 3Y and 5Y columns
            # TODO: generic:
            #    https://stackoverflow.com/questions/16060899/alphabet-range-in-python
            hide_columns = [
                "N",
                "O",
                "P",
                "Q",
                "R",
                "S",
                "T",
                "U",
                "V",
                "W",
                "X",
                "Y",
            ]
        elif primary_df.loc[0, "5Y_AnnRor"] is None:
            # hide 5Y columns
            hide_columns = ["N", "O", "P", "Q", "R", "S"]

        breakdown_tables = [
            x for x in tables if 'Page' not in x.component_name
        ]
        toc_sheet = [
            x for x in tables if 'Page' in x.component_name or x.component_name == 'Date' or x.component_name == 'PortfolioName'
        ]

        this_worksheet = ReportWorksheet(
            sheet_name,
            report_tables=breakdown_tables,
            render_params=ReportWorksheet.ReportWorkSheetRenderer(
                # trim_region=regions_to_trim,
                print_region=print_region,
                hide_columns=hide_columns,
            ),
        )
        toc_worksheet = ReportWorksheet(
            'Table of Contents',
            report_tables=toc_sheet,
            render_params=ReportWorksheet.ReportWorkSheetRenderer(
                # trim_region=regions_to_trim,
                # print_region=print_region,
                # hide_columns=hide_columns,
            ),
        )
        workbook = ReportWorkBookHandler(
            f"{self.report_meta.entity_domain.name}_Perf",
            template_location=self.excel_template_location,
            report_sheets=[this_worksheet, toc_worksheet],
        )
        return [workbook]

