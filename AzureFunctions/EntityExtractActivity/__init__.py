from gcm.inv.dataprovider.entity_provider.azure_extension.extended_entity_activity import (
    ExtendedEntityExtractActivity,
)


def main(context):
    return ExtendedEntityExtractActivity().execute(context=context)
