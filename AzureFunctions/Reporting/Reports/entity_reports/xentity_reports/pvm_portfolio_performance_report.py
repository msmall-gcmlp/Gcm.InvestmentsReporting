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
class PvmPerformanceBreakoutReport(ReportStructure):
    def __init__(self, report_meta: ReportMeta):
        super().__init__(
            ReportNames.PvmPerformanceBreakoutReport, report_meta
        )

    @property
    def excel_template_location(self):
        return AzureDataLakeDao.BlobFileStructure(
            zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
            sources="investmentsreporting",
            entity="exceltemplates",
            path=["TWROR_Template_threey.xlsx"],
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
                vertical=ReportConsumer.Vertical.PEREI,
            ),
            entity_groups=[
                EntityDomainTypes.Portfolio,
                EntityDomainTypes.InvestmentManager,
            ],
        )

    @classmethod
    def standard_entity_get_callable(
        cls, domain: EntityDomainProvider, parsed_args: ParsedArgs
    ) -> Callable[..., pd.DataFrame]:
        if domain.domain_table.domain == EntityDomainTypes.Portfolio:
            port: PortfolioEntityProvider = domain
            return port.get_pe_only_portfolios
        if domain.domain_table.domain == EntityDomainTypes.SynthesisUnit:
            syn_unit_type = SynthesisUnitType[
                parsed_args.SynthesisUnitType
            ]
            syn_unit = get_synthesis_unit_provider_by_type(syn_unit_type)
            return syn_unit.get_all_in_unit
        else:
            return domain.get_perei_med_entities

    def assign_components(self) -> List[ReportWorkBookHandler]:
        entity_name: str = self.report_meta.entity_info[
            "EntityName"
        ].unique()[0]
        # example: this is "Private Eqtuiy"
        # get all deals associated with Private Equity
        if self.report_meta.entity_domain == SynthesisUnitType:
            PvmDealAssetClass().get_deals_for_asset_class[entity_name]
        as_of_date: dt.date = Scenario.get_attribute("as_of_date")
        domain = self.report_meta.entity_domain
        entity_info = self.report_meta.entity_info
        p = PvmPerformanceHelper(domain, entity_info=entity_info)
        final_data: dict = p.generate_components_for_this_entity(
            as_of_date
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

        # 19 = number of excel header rows before primary_df range starts (not scalable)
        print_region = "B1:AC" + str(len(primary_df) + 19)

        # identifying hide_columns could be more generic, not worth it currently
        # below hide_col conditions would be better to use
        # an entity-specific "track record length" property
        # rather than inferring from whether 3Y/5Y ROR exists
        # similarly, setting up report dictionary of df metric-to-excel column
        # would be better for determining columns to hide
        hide_columns = []

        # TODO: check portfolio inception date to set these dynamically
        if primary_df.loc[0, "3Y_AnnRor"] is None:
            # hide 3Y and 5Y columns
            # TODO: generic:
            #    https://stackoverflow.com/questions/16060899/alphabet-range-in-python
            hide_columns = [
                "M",
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
            ]
        elif primary_df.loc[0, "5Y_AnnRor"] is None:
            # hide 5Y columns
            hide_columns = ["M", "N", "O", "P", "Q", "R"]

        this_worksheet = ReportWorksheet(
            sheet_name,
            report_tables=tables,
            render_params=ReportWorksheet.ReportWorkSheetRenderer(
                # trim_region=regions_to_trim,
                print_region=print_region,
                hide_columns=hide_columns,
            ),
        )
        workbook = ReportWorkBookHandler(
            f"{self.report_meta.entity_domain.name}_Perf",
            template_location=self.excel_template_location,
            report_sheets=[this_worksheet],
        )
        return [workbook]
