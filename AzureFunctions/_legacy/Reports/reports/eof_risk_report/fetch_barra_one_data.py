import pandas as pd
import datetime as dt
from io import BytesIO, StringIO
from zipfile import ZipFile
from gcm.data import DataAccess, DataSource
from gcm.data.storage import StorageQueryParams, DataLakeZone


def _read_barra_zip_from_datalake(
    folder_path, report_pkg, as_of_date, data_lake_client
):
    query = StorageQueryParams(
        file_system=DataLakeZone.raw,
        path=f"{folder_path}{report_pkg}-{as_of_date.strftime('%Y%m%d')}-BDT export job.zip",
    )

    return data_lake_client.get_blob(query).content


def _extract_single_csv_from_zip(zip_bytes, csv_partial_name):
    zip_file = BytesIO(zip_bytes)
    zip_file_obj = ZipFile(zip_file)
    zipped_file_names = zip_file_obj.namelist()        
    found_file = next(
        (f for f in zipped_file_names if csv_partial_name in f), None
    )
    
    raw_file_bytes = zip_file_obj.open(found_file).read()
    return StringIO(raw_file_bytes.decode("utf-8"))
    #return StringIO(raw_file_bytes.decode("ISO-8859-1"))


def _process_barra_dataframe(csv_bytes, header_rows_to_ignore=15):
    df = pd.read_csv(csv_bytes, skiprows=header_rows_to_ignore)
    #if factor or stress
    if header_rows_to_ignore == 17 or 11:
        return df
    df.columns = df.columns.str.strip()
    df.drop(df.head(1).index, inplace=True)
    return df


def load_barra_one_data_from_datalake(
    folder_path,
    report_pkg,
    as_of_date,
    csv_partial_name,
    data_lake_client,
    skip=15,
):
    zip_bytes = _read_barra_zip_from_datalake(
        folder_path=folder_path,
        report_pkg=report_pkg,
        as_of_date=as_of_date,
        data_lake_client=data_lake_client,
    )

    csv_bytes = _extract_single_csv_from_zip(
        zip_bytes=zip_bytes, csv_partial_name=csv_partial_name
    )

    return _process_barra_dataframe(
        csv_bytes=csv_bytes, header_rows_to_ignore=skip
    )


def backfill_barra_data(
    start_date,
    end_date,
    folder_path,
    report_pkg,
    csv_partial_name,
    data_lake_client,
):
    full_backfill = pd.DataFrame()
    for date in pd.date_range(start=start_date, end=end_date):
        try:
            single_date_df = load_barra_one_data_from_datalake(
                folder_path=folder_path,
                report_pkg=report_pkg,
                as_of_date=date,
                csv_partial_name=csv_partial_name,
                data_lake_client=dl_client,
            )
            single_date_df["Date"] = date
            full_backfill = pd.concat([full_backfill, single_date_df])
        except:
            pass
    first_column = full_backfill.pop("Date")
    full_backfill.insert(0, "Date", first_column)
    return full_backfill.reset_index(drop=True)


if __name__ == "__main__":
    sub = "prd"
    zone = "mscidev" if sub == "nonprd" else "msci"
    dl_client = DataAccess().get(
        DataSource.DataLake,
        target_name=f"gcmdatalake{sub}",
    )

    # single date load
    single_day_data = load_barra_one_data_from_datalake(
        folder_path=f"{zone}/barraone/position/eof_add_on_reports/",
        report_pkg="EOF Add-On Reports w LT",
        as_of_date=dt.date(2023, 8, 14),
        csv_partial_name="FactorGroup_ContribRisk",
        data_lake_client=dl_client,
    )
    import pdb

    pdb.set_trace()
    # multi-date load
    # multi_day_data = backfill_barra_data(start_date=dt.date(2023, 5, 1),
    #                                     end_date=dt.date(2023, 5, 31),
    #                                     folder_path=f"{zone}/barraone/position/eof_add_on_reports/",
    #                                     report_pkg="EOF Add-On Reports w LT",
    #                                     csv_partial_name='Geography',  # i.e. Geography, Idio Vol, or Market Cap
    #                                     data_lake_client=dl_client)
    import pdb

    pdb.set_trace()
