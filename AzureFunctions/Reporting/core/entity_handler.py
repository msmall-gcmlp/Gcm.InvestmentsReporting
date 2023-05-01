from gcm.inv.entityhierarchy.NodeHierarchy import (
    NodeHierarchyDomain,
    EntityDomainTypes,
    List,
    Standards as EntityStandardNames,
    EntityDomain,
    get_domain,
)
import pandas as pd


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

    @property
    def hierarchy_down(
        self,
    ) -> "HierarchyStruct":
        __name = "__h_down"
        __if = getattr(self, __name, None)
        if __if is None:
            val = HierarchyHandler.get_hierarchy(
                self.domain, [self.name], True
            )
            val = HierarchyHandler.HierarchyStruct(val[0], val[1], val[2])
            setattr(self, __name, val)
        return getattr(self, __name, None)

    @property
    def hierarchy_up(
        self,
    ) -> "HierarchyStruct":
        __name = "__h_up"
        __if = getattr(self, __name, None)
        if __if is None:
            val = HierarchyHandler.get_hierarchy(
                self.domain, [self.name], False
            )
            val = HierarchyHandler.HierarchyStruct(val[0], val[1], val[2])
            setattr(self, __name, val)
        return getattr(self, __name, None)

    @property
    def entity_info(self):
        __name = "__info_entity"
        __if = getattr(self, __name, None)
        if __if is None:
            entity_domain: EntityDomain = get_domain(self.domain)
            [entities, sources] = entity_domain.get_by_entity_names(
                [self.name],
            )
            merged = EntityDomain.merge_ref_and_sources(entities, sources)
            setattr(self, __name, merged)
        return getattr(self, __name, None)

    def get_entities_directly_related_by_name(
        self, neighbor_domain_type: EntityDomainTypes, down=True
    ) -> pd.DataFrame:
        item = self.hierarchy_down if down else self.hierarchy_up
        # changed data type. handle accordingly
        [edges, vertex, sources] = [item.edges, item.vertex, item.sources]
        this_id = list(
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
            edges[edges[graby_by].isin(this_id)],
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
