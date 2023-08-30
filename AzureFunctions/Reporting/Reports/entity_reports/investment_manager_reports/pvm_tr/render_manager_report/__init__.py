from gcm.Dao.DaoRunner import AzureDataLakeDao

TEMPLATE = AzureDataLakeDao.BlobFileStructure(
    zone=AzureDataLakeDao.BlobFileStructure.Zone.raw,
    sources="investmentsreporting",
    entity="exceltemplates",
    path=["PvmManagerTrackRecordTemplate.xlsx"],
)
