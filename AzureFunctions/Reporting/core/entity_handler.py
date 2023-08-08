from gcm.inv.entityhierarchy.NodeHierarchy import (
    EntityDomainTypes,
    List,
    Standards as EntityStandardNames,
)
import pandas as pd
from functools import cached_property
from gcm.inv.utils.misc.table_cache_base import Singleton
from gcm.inv.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner, DaoSource
import json
import numpy as np


class EntityReportingMetadata:
    class gcm_entity_metadata:
        generic_entity_name = "gcm_entity_name"
        generic_entity_id = "gcm_entity_ids"
        generic_entity_type = "gcm_entity_type"
        generic_entity_source = "gcm_entity_source"

        class gcm_ars_hardcodes:
            gcm_manager_fund_ids = "gcm_manager_fund_ids"
            gcm_manager_fund_group_ids = "gcm_manager_fund_group_ids"
            gcm_portfolio_ids = "gcm_portfolio_ids"

    class EntityMasterSourceDimn(metaclass=Singleton):
        def __init__(self):
            pass

        @cached_property
        def table(self) -> pd.DataFrame:
            dao: DaoRunner = Scenario.get_attribute("dao")
            source_table: pd.DataFrame = dao.execute(
                source=DaoSource.InvestmentsDwh,
                params={"table": "SourceDimn", "schema": "EntityMaster"},
                operation=lambda d, p: d.get_data(p),
            )[
                [
                    EntityStandardNames.SourceId,
                    EntityStandardNames.SourceName,
                ]
            ]
            return source_table

    @classmethod
    def _generate_custom_items(
        cls,
        g: pd.DataFrame,
        source_tyes: List[object],
    ) -> List[int]:
        external_ids = g[
            g[EntityStandardNames.SourceName].isin(source_tyes)
        ]
        external_ids = list(
            external_ids[EntityStandardNames.ExternalId].unique()
        )
        external_ids = [x for x in external_ids]
        return external_ids

    @classmethod
    def _check_is_of_type(
        cls, avail_srcs_this_entity: List[str], source_list: List[str]
    ):
        return (
            len(
                [
                    source_name
                    for source_name in list(map(str.upper, source_list))
                    if source_name
                    in list(map(str.upper, avail_srcs_this_entity))
                ]
            )
            > 0
        )

    @staticmethod
    def __coerce_to_int(item: object, strict=True):
        try:
            if type(item) == int:
                return item
            if type(item) == np.int64:
                return int(item)
            if type(item) == float:
                return int(float(item))
            if type(item) == str:
                try:
                    return int(item)
                except ValueError:
                    return EntityReportingMetadata.__coerce_to_int(
                        float(item), strict=strict
                    )
        except ValueError:
            return None if strict else item

    @classmethod
    def generate(cls, entity_info: pd.DataFrame):
        # if this is a PVM MED entity (or any ARS MED entity),
        # prioritize the MED ID as the Entity Tag
        # If is ARS entity, establish tags in pre-defined location
        # as given by Mark / Armando for RH integration
        # Else
        # Take the internal EntityId from IDW
        class_type = EntityReportingMetadata.gcm_entity_metadata
        if EntityStandardNames.SourceName not in entity_info.columns:
            if EntityStandardNames.SourceId in entity_info.columns:
                mapping = (
                    EntityReportingMetadata.EntityMasterSourceDimn().table
                )
                entity_info = pd.merge(
                    entity_info,
                    mapping,
                    on=[EntityStandardNames.SourceId],
                    how="left",
                )
            else:
                raise RuntimeError("No Source Name!")
        grouped_on_generic_entity = entity_info.groupby(
            [
                EntityStandardNames.EntityName,
                EntityStandardNames.EntityId,
                EntityStandardNames.EntityDomain,
            ]
        )
        # we should only be applying to one entity at a time
        assert grouped_on_generic_entity.ngroups == 1
        for n, g in grouped_on_generic_entity:
            entity_type = str(n[2])
            avail_srcs_this_entity = list(
                [
                    str(x)
                    for x in g[EntityStandardNames.SourceName].unique()
                ]
            )

            coerced_dict = {
                class_type.generic_entity_name: str(n[0]),
                class_type.generic_entity_type: entity_type,
            }

            # TODO: DT note: revisit - better solution is creating a "pub" and "pvm-med" classification above existing SourceName
            pub_med_identifiers = ["AltSoft.Pub", "Pub.InvestmentDimn"]
            pvm_med_sources = ["PVM.MED", "pvm-med"]

            if EntityReportingMetadata._check_is_of_type(
                avail_srcs_this_entity, pub_med_identifiers
            ):
                sub_class = class_type.gcm_ars_hardcodes
                external_ids = [
                    int(x)
                    for x in (
                        EntityReportingMetadata._generate_custom_items(
                            g, pub_med_identifiers
                        )
                    )
                ]

                # yeesh this is messy. But this is what reporting hub requires!
                mapping_type = {
                    EntityDomainTypes.Investment.name: sub_class.gcm_manager_fund_ids,
                    EntityDomainTypes.InvestmentGroup.name: sub_class.gcm_manager_fund_group_ids,
                    EntityDomainTypes.Portfolio.name: sub_class.gcm_portfolio_ids,
                }
                coerced_dict[
                    class_type.generic_entity_source
                ] = "pub-med"  # update
                _mapped = mapping_type[entity_type]
                coerced_dict[_mapped] = external_ids
            elif EntityReportingMetadata._check_is_of_type(
                avail_srcs_this_entity, pvm_med_sources
            ):
                # TODO: change when MW / AA give us intructions:
                __coerce = EntityReportingMetadata.__coerce_to_int
                external_ids = (
                    EntityReportingMetadata._generate_custom_items(
                        g, pvm_med_sources
                    )
                )
                external_ids = [__coerce(x) for x in external_ids]

                external_ids = [
                    x
                    for x in set(external_ids)
                    if x is not None and isinstance(x, int)
                ]
                coerced_dict[class_type.generic_entity_id] = external_ids
                coerced_dict[
                    class_type.generic_entity_source
                ] = "pvm-med"  # update
            else:
                coerced_dict[class_type.generic_entity_id] = [int(n[1])]
                coerced_dict[
                    class_type.generic_entity_source
                ] = "IDW"  # update

            if coerced_dict is not None:
                for k, v in coerced_dict.items():
                    if isinstance(v, list):
                        coerced_dict[k] = json.dumps(v)

            return coerced_dict
