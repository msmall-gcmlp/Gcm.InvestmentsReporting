
from enum import Enum
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

class PvmPerformanceBreakoutReport(ReportStructure):

    # TODO: DT note: move report_name_enum into report_meta. It belongs under the report_meta scope
    # TODO: DT note: ReportName.py functionality has changed:
    #  ReportName.name.name is currently being used to:
    #       1. determine report file name
    #       2. is part of DL unique key
    #   Now, ReportName.name.value is used for the above and is also referenced in report code
    #       to determine calc and query params (x sector, x vintage & realization type etc)

    def __init__(self,
                 report_meta: ReportMeta,
                 report_name_enum: Enum):
        super().__init__(
            report_meta=report_meta,
            report_name=report_name_enum
        )
        self.report_name_enum = report_name_enum

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

        p = PvmPerformanceHelper(domain,
                                 entity_info=entity_info,
                                 report_name_enum=self.report_name_enum)
        final_data: dict = p.generate_components_for_this_entity(
            as_of_date,
        )

        # TODO: DT note: I don't love that this table loop
        tables: List[ReportTable] = []
        for k, v in final_data.items():
            this_table = ReportTable(k, v)
            tables.append(this_table)


        ########## TODO: Begin formatting and weird stuff #######################################################
        # below is this-report specific logic to derive render params

        sheet_name = "Performance Breakout"
        primary_named_range = "Data"
        primary_df = [
            x.df for x in tables
            if x.component_name == primary_named_range
        ][0]

        # trim rows for all ranges in this sheet
        # regions_to_trim: List[str] = [x.component_name for x in tables]

        row_count_excel_header = 21
        print_region = "B1:AC" + str(len(primary_df) + row_count_excel_header)
        # if (self.iteration_number + 1) <= 3:
        #     page_number = int(math.ceil((len(primary_df) + 20) / 77) + 1)
        # else:
        #     page_number = None

        # TODO: check portfolio inception date to set these dynamically
        # always hide 5Y cols
        hide_columns = [
            "N",
            "O",
            "P",
            "Q",
            "R",
            "S"
        ]
        if primary_df.loc[0, "3Y_AnnRor"] is None:
            # hide 3Y and 5Y columns
            # TODO: generic:
            #    https://stackoverflow.com/questions/16060899/alphabet-range-in-python
            hide_columns.extend(
                [
                    "T",
                    "U",
                    "V",
                    "W",
                    "X",
                    "Y",
                ]
            )

        ########## TODO: Done formatting and weird stuff ##########################################################################

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

