from gcm.inv.entityhierarchy.NodeHierarchy import (
    NodeHierarchyDomain,
    EntityDomainTypes,
    List,
    Standards as EntityStandardNames,
    EntityDomain,
    get_domain,
)
import pandas as pd
from functools import cached_property
from gcm.inv.utils.misc.table_cache_base import Singleton
from gcm.inv.scenario import Scenario
from gcm.Dao.DaoRunner import DaoRunner, DaoSource
import json


class HierarchyHandler(object):
    def __init__(self, domain: EntityDomainTypes, entity_name: str):
        self.domain = domain
        self.name = entity_name

    class HierarchyStruct(object):
        def __init__(
            self,
            edges: pd.DataFrame,
            vertex: pd.DataFrame,
            sources: pd.DataFrame,
        ):
            self.edges = edges
            self.vertex = vertex
            self.sources = sources

    @cached_property
    def hierarchy_down(
        self,
    ) -> "HierarchyStruct":
        val = HierarchyHandler.get_hierarchy(
            self.domain, [self.name], True
        )
        val = HierarchyHandler.HierarchyStruct(val[0], val[1], val[2])
        return val

    @cached_property
    def hierarchy_up(
        self,
    ) -> "HierarchyStruct":
        val = HierarchyHandler.get_hierarchy(
            self.domain, [self.name], False
        )
        val = HierarchyHandler.HierarchyStruct(val[0], val[1], val[2])
        return val

    @cached_property
    def entity_info(self):
        entity_domain: EntityDomain = get_domain(self.domain)
        [entities, sources] = entity_domain.get_by_entity_names(
            [self.name],
        )
        merged = EntityDomain.merge_ref_and_sources(entities, sources)
        return merged

    def get_entities_directly_related_by_name(
        self,
        neighbor_domain_type: EntityDomainTypes,
        starting_node_id: List[int] = None,
        down=True,
    ) -> pd.DataFrame:
        item = self.hierarchy_down if down else self.hierarchy_up
        # changed data type. handle accordingly
        [edges, vertex, sources] = [item.edges, item.vertex, item.sources]
        if starting_node_id is None:
            starting_node_id = list(
                set(self.entity_info[EntityStandardNames.NodeId].to_list())
            )
        remap = (
            EntityStandardNames.Child_NodeId
            if down
            else EntityStandardNames.Parent_NodeId
        )
        neighbors_of_type: pd.DataFrame = vertex[
            vertex[EntityStandardNames.EntityDomain]
            == neighbor_domain_type.name
        ].rename(columns={EntityStandardNames.NodeId: remap})
        graby_by = (
            EntityStandardNames.Parent_NodeId
            if down
            else EntityStandardNames.Child_NodeId
        )
        neighbors_of_type = pd.merge(
            edges[edges[graby_by].isin(starting_node_id)],
            neighbors_of_type,
            on=remap,
        )
        # clean up
        neighbors_of_type.rename(
            columns={remap: EntityStandardNames.NodeId},
            inplace=True,
        )
        temp_sources = sources[
            sources[EntityStandardNames.EntityDomain]
            == neighbor_domain_type.name
        ]
        temp_sources[
            temp_sources[EntityStandardNames.EntityId].isin(
                neighbors_of_type[EntityStandardNames.EntityId].to_list()
            )
        ]
        entity_info_full = pd.merge(
            temp_sources,
            neighbors_of_type,
            on=[
                EntityStandardNames.EntityDomain,
                EntityStandardNames.EntityId,
            ],
        )
        # merge against sources
        return entity_info_full

    @staticmethod
    def get_hierarchy(
        domain: EntityDomainTypes,
        current_entity_name: List[str],
        recurse_down=True,
    ):
        [
            edges,
            vertex,
            sources,
        ] = NodeHierarchyDomain().get_edges_and_vertex(
            domain, current_entity_name, recurse_down
        )
        assert (
            edges is not None
            and vertex is not None
            and sources is not None
        )
        val = [edges, vertex, sources]
        return val


class EntityReportingMetadata:
    class gcm_entity_metadata:
        generic_entity_name = "gcm_entity_name"
        generic_entity_id = "gcm_entity_id"
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
        return any(
            [x.upper() in source_list for x in avail_srcs_this_entity]
        )

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
            pub_med_identifiers = [
                "ALTSOFT.PUB",
                "PUB.INVESTMENTDIMN",
            ]
            pvm_med_sources = ["PVM.MED"]

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
                coerced_dict[class_type.generic_entity_source] = "PUB"
                _mapped = mapping_type[entity_type]
                coerced_dict[_mapped] = external_ids
            elif EntityReportingMetadata._check_is_of_type(
                avail_srcs_this_entity, pvm_med_sources
            ):
                # TODO: change when MW / AA give us intructions:
                external_ids = [
                    int(x)
                    for x in (
                        EntityReportingMetadata._generate_custom_items(
                            g, pvm_med_sources
                        )
                    )
                ]
                coerced_dict[class_type.generic_entity_id] = external_ids
                coerced_dict[class_type.generic_entity_source] = "PVM.MED"
            else:
                coerced_dict[class_type.generic_entity_id] = [int(n[1])]
                coerced_dict[class_type.generic_entity_source] = "IDW"

            if coerced_dict is not None:
                for k, v in coerced_dict.items():
                    if isinstance(v, list):
                        coerced_dict[k] = json.dumps(v)

            return coerced_dict
