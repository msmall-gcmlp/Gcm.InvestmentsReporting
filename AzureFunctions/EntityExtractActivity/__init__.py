from gcm.inv.entityhierarchy.az_func.entity_extract_activity_base import (
    EntityParsedArgs,
    get_domain,
    EntityDomain,
    EntityDomainTypes,
    EntityNames,
)
from gcm.inv.utils.azure.durable_functions.base_activity import (
    BaseActivity,
)

import pandas as pd


class EntityExtractActivityBase(BaseActivity):
    def __init__(self):
        super().__init__()

    @property
    def parg_type(self):
        return EntityParsedArgs

    def activity(self, **kwargs):
        # this MUST be available
        domain_type: EntityDomainTypes = self.pargs.EntityDomainTypes
        # now, we can use existing APIs
        if domain_type != EntityDomainTypes.NONE:
            df: pd.DataFrame = None
            entity_domain: EntityDomain = get_domain(domain_type)
            entity_names: str = getattr(self.pargs, EntityNames, None)
            if entity_names is None:
                [ref, sources] = entity_domain.get_all()
                df = pd.merge(
                    ref,
                    sources,
                    on=list(
                        set(ref.columns).intersection(set(sources.columns))
                    ),
                    how="left",
                )
            else:
                [ref, sources] = entity_domain.get_by_entity_names(
                    entity_names.split(",")
                )
                df = pd.merge(
                    ref,
                    sources,
                    on=list(
                        set(ref.columns).intersection(set(sources.columns))
                    ),
                    how="left",
                )
            if df is not None:
                return df.to_json()
        else:
            return ""
        # now - check if names are available:


def main(context):
    return EntityExtractActivityBase().execute(context=context)
