from datetime import timedelta, datetime, date
from typing import Union, List
from uuid import UUID

import numpy as np
import pandas as pd
import pytz

from app.db.session import SessionLocal
from app.enums.transaction_status import TransactionStatus
from app.lib.google_clients.google_drive_client.google_drive_client import GoogleDriveAPIClient
from app.lib.google_clients.google_sheets_client.google_sheets_client import GoogleSheetsAPIClient
from app.repositories.merchant_repository import MerchantRepository
from app.repositories.transaction_repository import TransactionRepository
from app.schemas.merchant import MerchantFromDB
from app.schemas.transaction import TransactionFromDBWithExtraData
from app.schemas.transaction_data import TransactionDataFromDB
from app.services.transaction_service import _build_transaction_csv_detail

AMOUNT_COLUMN_FOR_BACKEND_KEY = "amount_column_for_backend_key"
TRANSACTION_AMOUNT_FOR_AGGREGATOR_KEY = "transaction_amount_for_aggregator_key"
COMISION = "Comision"
CUOTA_FIJA = "Cuota Fija"
CONTRACARGOS = "Contracargos"
LIQUIDACION = "Liquidacion"
RATE = "TASA"


class ConciliationServiceBase:

    def __init__(self, aggregator_sheet_id: str, begin_date: date, end_date: date):
        self.backend_df = None
        self.backend_failed_transactions_df = None
        self.initial_aggregator_df = None  # Includes all transactions included in the aggregator's report
        self.aggregator_df = None  # Includes all transactions that are not chargebacks
        self.aggregator_chargebacks_df = None
        self.drive_folder_id = None
        self.google_sheets_client = GoogleSheetsAPIClient
        self.google_drive_client = GoogleDriveAPIClient
        self.aggregator_sheet_id = aggregator_sheet_id
        self.charge_code = self._get_charge_code()
        self.return_code = self._get_return_code()
        self.affiliation_number_column_name = self._get_affiliation_number_column_name()
        self.flat_fee_column_name = self._get_flat_fee_column_name()
        self.flat_fee_vat_column_name = self._get_flat_fee_vat_column_name()
        self.net_sale_column_name = self._get_net_sale_column_name()
        self.processing_date_column_name = self._get_processing_date_column_name()
        self.transaction_amount_column_name = self._get_transaction_amount_column_name()
        self.transaction_key_column_name = self._get_transaction_key_column_name()
        self.transaction_date_column_name = self._get_transaction_date_column_name()
        self.TRANSACTION_AMOUNT_FOR_AGGREGATOR_KEY = TRANSACTION_AMOUNT_FOR_AGGREGATOR_KEY
        self.begin_date = begin_date
        self.begin_search_date = datetime.combine(self.begin_date, datetime.min.time()) - timedelta(days=90)
        self.end_date = end_date
        self.end_search_date = datetime.combine(self.end_date, datetime.max.time()) + timedelta(hours=10)

        self._conglomerate_tab_mapping_dictionary = self._get_conglomerate_tab_mapping_dictionary()
        self.key_columns_for_regular_backend_transactions = [AMOUNT_COLUMN_FOR_BACKEND_KEY, "card_number_reference",
                                                             "affiliation_number", "provider_transaction_id"]
        self.key_columns_for_failed_backend_transactions = [AMOUNT_COLUMN_FOR_BACKEND_KEY, "card_number_reference",
                                                            "affiliation_number"]
        self.key_columns_for_payment_aggregator = self._get_key_columns_for_payment_aggregator()
        self.key_columns_for_failed_payment_aggregator = self._get_key_columns_for_failed_payment_aggregator()
        self._aggregator_sheet_range = self._get_aggregator_sheet_range()
        self._aggregator_folder_id = self._get_aggregator_folder_id()
        self._aggregator_template_sheet_id = self._get_aggregator_template_sheet_id()

    def generate_conciliation(self) -> str:
        """Generates the general conciliation, as well as individual conciliation reports.
        The information is stored in generated Google Sheets inside a generated Google Drive folder.
        Returns the Drive folder id"""
        self.initial_aggregator_df = self._construct_aggregator_df()

        # Separate chargebacks.
        self.aggregator_df = self._get_aggregator_df()
        self.aggregator_chargebacks_df = self._get_chargebacks_df()

        self.aggregator_df = \
            self._create_key_for_dataframe(self.aggregator_df, key_columns=self.key_columns_for_payment_aggregator)

        # These are used to make the searching on generate_general_report.
        charges_keys = self.aggregator_df[self.aggregator_df[self.transaction_key_column_name] == self.charge_code][
            "KEY"].values.tolist()
        returns_keys = self.aggregator_df[self.aggregator_df[self.transaction_key_column_name] == self.return_code][
            "KEY"].values.tolist()

        self.backend_df, self.backend_failed_transactions_df = self._construct_backend_dfs()
        self.backend_df = self._create_key_for_dataframe(self.backend_df,
                                                         key_columns=self.key_columns_for_regular_backend_transactions)

        merged_aggregator_df, merged_backend_df = self.generate_general_report(charges_keys, returns_keys)
        merchant_ids = merged_backend_df["merchant_id"].unique().tolist()

        self.generate_individual_reports_and_fill_conglomerate_files(merchant_ids, merged_aggregator_df,
                                                                     merged_backend_df)
        return self.drive_folder_id

    def _get_chargebacks_df(self) -> pd.DataFrame:
        raise NotImplementedError

    def generate_general_report(self, charges_keys, returns_keys):
        # Do cross-search and generate dataframes with all the relevant, valid transactions.
        # Search aggregator transactions in BE transactions.
        self.aggregator_df = self._mark_found_keys(self.aggregator_df, self.backend_df['KEY'])

        aggregator_df_found_in_backend_df = self.aggregator_df[self.aggregator_df['FOUND']]

        aggregator_df_found_in_backend_charges = \
            aggregator_df_found_in_backend_df[
                aggregator_df_found_in_backend_df[self.transaction_key_column_name] == self.charge_code]
        aggregator_df_found_in_backend_df_returns = \
            aggregator_df_found_in_backend_df[
                aggregator_df_found_in_backend_df[self.transaction_key_column_name] == self.return_code]

        # aggregator_df_found_charges_amount = \
        #     aggregator_df_found_in_backend_charges[self.transaction_amount_column_name].sum()
        # aggregator_df_found_in_backend_df_returns_amount = \
        #     aggregator_df_found_in_backend_df_returns[self.transaction_amount_column_name].sum()

        merged_aggregator_df = pd.concat(
            [aggregator_df_found_in_backend_charges, aggregator_df_found_in_backend_df_returns], ignore_index=True)

        # total_aggregator = aggregator_df_found_charges_amount + aggregator_df_found_in_backend_df_returns_amount

        # Search BE transactions in aggregator transactions
        self.backend_df = self._mark_found_keys(self.backend_df, self.aggregator_df['KEY'])

        backend_df_found = self.backend_df[self.backend_df['FOUND']]

        backend_df_found_charges, backend_df_found_returns = \
            self._get_charges_and_returns_of_df_from_keys(backend_df_found, charges_keys, returns_keys)

        # backend_df_found_charges_amount = \
        #     backend_df_found_charges['provider_amount'].sum()
        # backend_df_found_returns_amount = \
        #     backend_df_found_returns['provider_amount'].sum()
        # total_be = (
        #         backend_df_found_charges_amount
        #         - backend_df_found_returns_amount
        # )
        merged_backend_df = self._create_df_for_general_report(backend_df_found_charges, backend_df_found_returns)

        ####
        aggregator_transactions_missing_on_backend_df = \
            self._get_missing_transactions_on_backend_df(merged_backend_df, self.aggregator_df)
        if not aggregator_transactions_missing_on_backend_df.empty:
            pass
            # OMITTED

        self._write_file_to_aggregator_drive_folder(merged_backend_df)
        return merged_aggregator_df, merged_backend_df

    def _get_charges_and_returns_of_df_from_keys(self, backend_df_found, charges_keys, returns_keys) -> (
            pd.DataFrame, pd.DataFrame):
        backend_df_found_charges = self._filter_by_keys(
            backend_df_found,
            charges_keys
        )
        backend_df_found_returns = self._filter_by_keys(
            backend_df_found,
            returns_keys
        )
        return backend_df_found_charges, backend_df_found_returns

    def generate_individual_reports_and_fill_conglomerate_files(self, merchant_ids, merged_aggregator_df,
                                                                merged_backend_df):
        for merchant_id in merchant_ids:
            # Generate report with BE transactions' information.
            merchant_df = merged_backend_df[merged_backend_df["merchant_id"] == merchant_id]
            mid_number = merchant_df["mid_number"].iloc[0]
            copied_template_id = self.google_drive_client.copy_file(file_id=self._aggregator_template_sheet_id,
                                                                    new_file_name=f'Payments {mid_number} {self.begin_date.day} {self.begin_date.month} - {self.end_date.day} {self.end_date.month}_{self.end_date.year}',
                                                                    parent_folder_id=self.drive_folder_id)
            self.google_sheets_client.write_df_to_sheet(df=merchant_df, sheet_id=copied_template_id, sheet_tab="Detail",
                                                        sheet_column="A", sheet_row=2)

            affiliation_number = merchant_df["affiliation_number"].iloc[0]

            if mid_number not in self._conglomerate_tab_mapping_dictionary.keys():
                continue

            # Get the Google Sheet's tab name and ID.
            google_sheet_id = self._conglomerate_tab_mapping_dictionary[mid_number]['sheet_id']
            google_sheet_tab = self._conglomerate_tab_mapping_dictionary[mid_number]['tab']

            # Generate report with merchant's transactions' information.
            aggregator_charges_and_returns_df = merged_aggregator_df[
                merged_aggregator_df[self.affiliation_number_column_name] == affiliation_number]
            aggregator_chargebacks_df = \
                self.aggregator_chargebacks_df[
                    self.aggregator_chargebacks_df[self.affiliation_number_column_name] == affiliation_number]

            conglomerate_df_merged = self._generate_settlement_df(aggregator_chargebacks_df.copy(),
                                                                  aggregator_charges_and_returns_df.copy(),
                                                                  google_sheet_id, google_sheet_tab)

            self.google_sheets_client.write_df_to_sheet(df=conglomerate_df_merged, sheet_id=google_sheet_id,
                                                        sheet_tab=google_sheet_tab, sheet_column="A", sheet_row=77)

            self.google_sheets_client.write_df_to_sheet(df=conglomerate_df_merged, sheet_id=copied_template_id,
                                                        sheet_tab="Conglomerate", sheet_column="B", sheet_row=4)

            # Now we calculate the settlement using the net sale column of all aggregator's transactions.
            # For this, we use the processing date column but subtracting 1 day.
            conglomerate_aggregator_df_with_chargebacks = self._generate_platform_settlement_df(
                aggregator_chargebacks_df.copy(), aggregator_charges_and_returns_df.copy())

            self.google_sheets_client.write_df_to_sheet(df=conglomerate_aggregator_df_with_chargebacks,
                                                        sheet_id=google_sheet_id,
                                                        sheet_tab=google_sheet_tab, sheet_column="H", sheet_row=180)

    def _generate_platform_settlement_df(self, aggregator_chargebacks_df, aggregator_charges_and_returns_df):
        aggregator_df_with_chargebacks = \
            pd.concat([aggregator_charges_and_returns_df, aggregator_chargebacks_df], ignore_index=True)
        aggregator_df_with_chargebacks[self.processing_date_column_name] = \
            aggregator_df_with_chargebacks[self.processing_date_column_name] - pd.Timedelta(1, unit='D')
        conglomerate_aggregator_df_with_chargebacks = \
            aggregator_df_with_chargebacks.groupby(self.processing_date_column_name)[
                [self.net_sale_column_name]].sum().reset_index()
        # Keep only the net sale column.
        conglomerate_aggregator_df_with_chargebacks = \
            conglomerate_aggregator_df_with_chargebacks[[self.net_sale_column_name]]
        return conglomerate_aggregator_df_with_chargebacks

    def _generate_settlement_df(self, aggregator_chargebacks_df: pd.DataFrame,
                                aggregator_charges_and_returns_df: pd.DataFrame,
                                google_sheet_id: str, google_sheet_tab: str) -> pd.DataFrame:
        """Generate the settlement using the transaction amounts, commission, fees, and chargebacks."""
        # Get sums of transaction amount, flat fee, and flat fee vat.
        conglomerate_aggregator_df = aggregator_charges_and_returns_df.groupby(self.transaction_date_column_name)[
            [self.transaction_amount_column_name, self.flat_fee_column_name,
             self.flat_fee_vat_column_name]].sum().reset_index()
        # For making the conglomerate of the chargebacks, we need to subtract 1 day from the processed date column.
        # This is just how the report is generated and has no technical reason behind.
        aggregator_chargebacks_df.loc[:, self.transaction_amount_column_name] *= -1
        aggregator_chargebacks_df.loc[:, self.processing_date_column_name] = \
            aggregator_chargebacks_df[self.processing_date_column_name] - pd.Timedelta(1, unit='D')
        # Get sums for transaction amounts for chargebacks
        conglomerate_aggregator_chargebacks_df = \
            aggregator_chargebacks_df.groupby(self.processing_date_column_name)[
                [self.transaction_amount_column_name]].sum().reset_index()
        conglomerate_df_merged = pd.merge(conglomerate_aggregator_df, conglomerate_aggregator_chargebacks_df,
                                          left_on=self.transaction_date_column_name,
                                          right_on=self.processing_date_column_name,
                                          how='left')
        conglomerate_df_merged = conglomerate_df_merged.fillna(0)
        # Add fixed fee column
        conglomerate_df_merged.loc[:, CUOTA_FIJA] = \
            (conglomerate_aggregator_df[self.flat_fee_column_name] +
             conglomerate_aggregator_df[self.flat_fee_vat_column_name])
        # Add rates column
        rates = self._get_rates(aggregator_charges_and_returns_df, conglomerate_aggregator_df)
        conglomerate_df_merged.loc[:, COMISION] = \
            rates * 1.16 * conglomerate_aggregator_df[self.transaction_amount_column_name]
        conglomerate_df_merged = conglomerate_df_merged.rename(
            columns={f'{self.transaction_amount_column_name}_x': self.transaction_amount_column_name,
                     f'{self.transaction_amount_column_name}_y': CONTRACARGOS})
        conglomerate_df_merged.drop(
            columns=[self.processing_date_column_name, self.flat_fee_column_name, self.flat_fee_vat_column_name],
            axis=1, inplace=True)
        # Add conciliation number.
        # The conciliation number is the last number of the A column of the conglomerate file's tab.
        conciliation_number = \
            int(self.google_sheets_client.get_sheet_data(sheet_id=google_sheet_id,
                                                         sheet_range=f"{google_sheet_tab}!A:A")[-1][0]) + 1
        conglomerate_df_merged.insert(loc=0, column='NUMERO', value=conciliation_number)
        # Add settlement column
        conglomerate_df_merged[LIQUIDACION] = (
                conglomerate_df_merged[self.transaction_amount_column_name] - conglomerate_df_merged[COMISION] -
                conglomerate_df_merged[CUOTA_FIJA] - conglomerate_df_merged[CONTRACARGOS])

        # Reorder columns
        conglomerate_df_merged = \
            conglomerate_df_merged.reindex(columns=["NUMERO",
                                                    self.transaction_date_column_name,
                                                    self.transaction_amount_column_name,
                                                    COMISION, CUOTA_FIJA, LIQUIDACION, CONTRACARGOS])
        return conglomerate_df_merged

    def _get_rates(self, aggregator_charges_and_returns_df, conglomerate_aggregator_df):
        rates = []
        for transaction_date in conglomerate_aggregator_df[self.transaction_date_column_name]:
            rate = \
                float(aggregator_charges_and_returns_df[
                          aggregator_charges_and_returns_df[self.transaction_date_column_name] == transaction_date]
                      [RATE].values[0].rstrip('%')) / 100
            rates.append(rate)
        rates_series = pd.Series(rates)
        return rates_series

    def _write_file_to_aggregator_drive_folder(self, merged_backend_df):
        # Initialize clients
        self.google_drive_client = self.google_drive_client()
        self.google_sheets_client = self.google_sheets_client()
        self.drive_folder_id = self.google_drive_client.get_or_create_folder(
            folder_name=f'{self.begin_date.year}/{self.begin_date.month}/{self.begin_date.day} - {self.end_date.year}/{self.end_date.month}/{self.end_date.day}',
            parent_folder_id=self._aggregator_folder_id)
        # Make a copy of the aggregator's template file
        copied_template_id = self.google_drive_client.copy_file(file_id=self._aggregator_template_sheet_id,
                                                                new_file_name=f'TRANSACCIONES BE',
                                                                parent_folder_id=self.drive_folder_id)
        self.google_sheets_client.write_df_to_sheet(df=merged_backend_df,
                                                    sheet_id=copied_template_id,
                                                    sheet_tab="Detail",
                                                    sheet_column="A",
                                                    sheet_row=2)

    def _create_df_for_general_report(self, charges_df: pd.DataFrame, returns_df: pd.DataFrame) -> pd.DataFrame:
        # Multiply *-1 for presenting the information to the finance department.
        returns_df.loc[:, 'provider_amount'] *= -1
        returns_df.loc[:, 'merchant_amount'] *= -1

        merged_backend_df = pd.concat([charges_df, returns_df], ignore_index=True)

        merged_backend_df = merged_backend_df.drop(columns=['FOUND', AMOUNT_COLUMN_FOR_BACKEND_KEY])
        merged_backend_df = merged_backend_df.drop_duplicates()
        merged_backend_df.fillna('', inplace=True)
        return merged_backend_df

    def _get_transactions_from_db_as_df(self, begin_date: datetime, end_date: datetime,
                                        merchant_id: Union[UUID, List[UUID]] = None):
        with SessionLocal() as db:
            transactions = TransactionRepository(db).get_transactions_summary(
                merchant_id=merchant_id,
                begin_date=begin_date,
                end_date=end_date,
                status=[TransactionStatus.initialized, TransactionStatus.aggregator_success,
                        TransactionStatus.refund_requested, TransactionStatus.refund_completed,
                        # ToDo: Optimize by adding this transactions only if general_report_df is inconsistent.
                        TransactionStatus.aggregator_failed],
                with_extra_data=True,
            )
        transactions_as_object = [self._get_as_transaction_with_extra_data(transaction) for transaction in transactions]
        transactions_info = [_build_transaction_csv_detail(transaction=transaction_object,
                                                           timezone=pytz.timezone("america/mexico_city"))
                             for transaction_object in transactions_as_object]
        transactions_data = [transaction_info.__dict__ for transaction_info in transactions_info]

        transactions_from_db_as_df = pd.DataFrame(transactions_data)
        return transactions_from_db_as_df

    @staticmethod
    def _get_as_transaction_with_extra_data(transaction):
        transaction_dict = transaction.__dict__
        transaction_dict["transaction_id"] = transaction_dict["id"]
        transaction_extra_data = TransactionDataFromDB(**transaction_dict)
        transaction_dict["transaction_data"] = transaction_extra_data
        return TransactionFromDBWithExtraData(**transaction_dict)

    def _construct_backend_dfs(self):
        unique_affiliation_numbers = self.aggregator_df[self.affiliation_number_column_name].unique().tolist()
        with SessionLocal() as db:
            merchants = MerchantRepository(db).find_by_affiliation_number(unique_affiliation_numbers)
        if merchants:
            backend_all_transactions_df = self._get_transactions_from_db_as_df(
                begin_date=self.begin_search_date,
                end_date=self.end_search_date,
                merchant_id=[merchant.id for merchant in merchants])
            backend_all_transactions_df = self._add_merchant_information(df=backend_all_transactions_df,
                                                                         merchants=merchants)
            backend_all_transactions_df = self._create_amount_column_for_key(df=backend_all_transactions_df,
                                                                             amount_column_name="merchant_amount")
            backend_all_transactions_df['created_at'] = pd.to_datetime(backend_all_transactions_df['created_at'])
            # # backend_all_transactions_df.to_csv('be_data.csv', index=False)
            # import os
            # script_directory = os.path.dirname(os.path.abspath(__file__))
            # csv_file_path = os.path.join(script_directory, 'be_data.csv')
            # backend_all_transactions_df = pd.read_csv(csv_file_path)
        else:
            backend_df = pd.DataFrame()
            backend_failed_transactions_df = pd.DataFrame()
            return backend_df, backend_failed_transactions_df
        backend_df = backend_all_transactions_df[
            backend_all_transactions_df['transaction_status'] != TransactionStatus.aggregator_failed]
        backend_failed_transactions_df = backend_all_transactions_df[
            backend_all_transactions_df['transaction_status'] == TransactionStatus.aggregator_failed]
        return backend_df, backend_failed_transactions_df

    @staticmethod
    def _add_affiliation_number_column(df: pd.DataFrame, merchants: List[MerchantFromDB]) -> pd.DataFrame:
        merchant_dict = {str(merchant.id): merchant.affiliation_number for merchant in merchants}

        def _get_affiliation_number(merchant_id):
            return merchant_dict.get(merchant_id, None)

        df['affiliation_number'] = df['merchant_id'].apply(_get_affiliation_number)

        return df

    @staticmethod
    def _create_amount_column_for_key(df: pd.DataFrame, amount_column_name: str) -> pd.DataFrame:
        df[AMOUNT_COLUMN_FOR_BACKEND_KEY] = df[amount_column_name]
        df[AMOUNT_COLUMN_FOR_BACKEND_KEY] = df[AMOUNT_COLUMN_FOR_BACKEND_KEY].astype(str).str.rstrip('.0')
        df[AMOUNT_COLUMN_FOR_BACKEND_KEY] = df[AMOUNT_COLUMN_FOR_BACKEND_KEY].astype(str).str.rstrip('.')
        return df

    @staticmethod
    def _create_key_for_dataframe(df: pd.DataFrame, key_columns: List[str],
                                  key_column_name: str = 'KEY') -> pd.DataFrame:
        """To make the conciliation, a key column is created on both, the providers information df and the backend df
        which is then used to make a look-up search"""
        df.loc[:, key_column_name] = df[key_columns].apply(lambda row: ''.join(row.values.astype(str)), axis=1)
        return df

    @staticmethod
    def _mark_found_keys(df, comparison_keys):
        """
        Mark rows in the DataFrame where the 'KEY' column is in the comparison_keys set.
        Returns a DataFrame with a new 'FOUND' column.
        """
        df.loc[:, 'FOUND'] = df['KEY'].isin(comparison_keys)
        return df

    @staticmethod
    def _filter_by_keys(df, keys):
        """
        Filters the DataFrame by checking if the 'KEY' column values are in the provided keys set.
        """
        return df[df['KEY'].isin(keys)]

    def _construct_aggregator_df(self) -> pd.DataFrame:
        raise NotImplementedError

    def _get_affiliation_number_column_name(self) -> str:
        raise NotImplementedError

    def _get_key_columns_for_payment_aggregator(self) -> List[str]:
        raise NotImplementedError

    def _get_key_columns_for_failed_payment_aggregator(self) -> List[str]:
        raise NotImplementedError

    def _get_aggregator_folder_id(self) -> str:
        raise NotImplementedError

    def _get_aggregator_template_sheet_id(self) -> str:
        raise NotImplementedError

    def _get_aggregator_sheet_range(self) -> str:
        raise NotImplementedError

    def _add_merchant_information(self, df: pd.DataFrame, merchants: List[MerchantFromDB]) -> pd.DataFrame:
        mid_number_map = {merchant.id: merchant.mid_number for merchant in merchants}
        affiliation_number_map = {merchant.id: merchant.affiliation_number for merchant in merchants}
        df['affiliation_number'] = df['merchant_id'].map(affiliation_number_map)
        df['mid_number'] = df['merchant_id'].map(mid_number_map)
        return df

    def _format_dataframe_for_conciliation(self, aggregator_df: pd.DataFrame) -> pd.DataFrame:
        aggregator_df = self._format_general_df_for_conciliation(aggregator_df)
        aggregator_df = self._format_specific_df_for_conciliation(aggregator_df)

        return aggregator_df

    def _format_general_df_for_conciliation(self, aggregator_df) -> pd.DataFrame:
        def clean_numeric_column(column: pd.Series) -> pd.Series:
            column = column.astype(str)
            column = column.str.replace(r'[ (),$-]', '', regex=True)
            column = column.str.replace('.00', '').str.rstrip('0')
            return column

        aggregator_df.replace('', np.nan, inplace=True)
        aggregator_df.dropna(subset=[self.affiliation_number_column_name,
                                     self.transaction_amount_column_name,
                                     self.transaction_key_column_name],
                             how='any', inplace=True)
        aggregator_df[self.transaction_date_column_name] = pd.to_datetime(
            aggregator_df[self.transaction_date_column_name], format='mixed')
        aggregator_df[self.affiliation_number_column_name] = \
            aggregator_df[self.affiliation_number_column_name].astype(str).str.replace(',', '').str.replace('.00', '')
        # Clean numeric columns
        aggregator_df[self.TRANSACTION_AMOUNT_FOR_AGGREGATOR_KEY] = \
            clean_numeric_column(aggregator_df[self.TRANSACTION_AMOUNT_FOR_AGGREGATOR_KEY])

        return aggregator_df

    def _format_specific_df_for_conciliation(self, aggregator_df) -> pd.DataFrame:
        raise NotImplementedError

    def _get_missing_transactions_on_backend_df(self, merged_backend_df, merged_aggregator_df) -> pd.DataFrame:
        """Returns the transactions that were found on the aggregator's df but are missing on the backend df"""

        # Get the set of keys from both DataFrames
        backend_keys = set(merged_backend_df['KEY'])
        aggregator_keys = set(merged_aggregator_df['KEY'])

        # Find keys present in the aggregator but not in the backend
        missing_keys = aggregator_keys - backend_keys

        # Filter rows from merged_aggregator_df where the 'KEY' is in missing_keys
        missing_rows_df = merged_aggregator_df[merged_aggregator_df['KEY'].isin(missing_keys)]

        return missing_rows_df

