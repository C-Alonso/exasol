from datetime import date
from typing import List

from app.core.config import settings
from app.lib.utils import strip_spaces_from_df
from app.services.conciliation_services.conciliation_service_base import ConciliationServiceBase
import pandas as pd

CLAVE_COMERCIO = "CLAVE COMERCIO"
NUMERO_AUTORIZACION = "NUMERO AUTORIZACION"
CUENTA = "CUENTA"
FECHA_PROCESO = "FECHA PROCESO"
FLAT_FEE = "FLAT FEE"
IMPORTE_TRANSACCION = "IMPORTE TRANSACCION"
IVA_FLAT_FEE = "IVA FLAT FEE"
VENTA_NETA = "VENTA NETA"

class ExasolConciliationService(ConciliationServiceBase):

    def __init__(self, aggregator_sheet_id: str, begin_date: date, end_date: date):
        super().__init__(aggregator_sheet_id, begin_date, end_date)
        self.flat_fee_vat_column_name = self._get_flat_fee_vat_column_name()
        self.net_sale_column_name = self._get_net_sale_column_name()
        self.processing_date_column_name = self._get_processing_date_column_name()
        self.chargeback_code = self._get_chargeback_code()

    def _construct_aggregator_df(self) -> pd.DataFrame:
        google_sheets_service = self.google_sheets_client()
        sheet_data = google_sheets_service.get_sheet_data(sheet_id=self.aggregator_sheet_id,
                                                          sheet_range=self._aggregator_sheet_range)
        unformatted_sheet_data = google_sheets_service.get_sheet_data(sheet_id=self.aggregator_sheet_id,
                                                                      sheet_range=self._aggregator_sheet_range,
                                                                      unformatted=True)
        exasol_df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])
        unformatted_exasol_df = pd.DataFrame(unformatted_sheet_data[1:], columns=unformatted_sheet_data[0])

        exasol_df = strip_spaces_from_df(exasol_df)

        exasol_df[self.TRANSACTION_AMOUNT_FOR_AGGREGATOR_KEY] = exasol_df[IMPORTE_TRANSACCION]
        exasol_df = self._format_dataframe_for_conciliation(exasol_df)
        exasol_df[IMPORTE_TRANSACCION] = unformatted_exasol_df[IMPORTE_TRANSACCION]
        exasol_df[VENTA_NETA] = unformatted_exasol_df[VENTA_NETA]
        exasol_df[FLAT_FEE] = unformatted_exasol_df[FLAT_FEE]
        exasol_df[IVA_FLAT_FEE] = unformatted_exasol_df[IVA_FLAT_FEE]
        return exasol_df

    def _get_affiliation_number_column_name(self) -> str:
        return CLAVE_COMERCIO

    def _get_key_columns_for_payment_aggregator(self) -> List[str]:
        return [self.TRANSACTION_AMOUNT_FOR_AGGREGATOR_KEY, CUENTA, CLAVE_COMERCIO, NUMERO_AUTORIZACION]

    def _get_key_columns_for_failed_payment_aggregator(self) -> List[str]:
        return [self.TRANSACTION_AMOUNT_FOR_AGGREGATOR_KEY, CUENTA, CLAVE_COMERCIO]

    def _get_charge_code(self):
        return "1"

    def _get_return_code(self):
        return "21"

    def _get_chargeback_code(self):
        return "15"

    def _get_aggregator_folder_id(self) -> str:
        return settings.EXASOL_CONCILIATIONS_DRIVE_FOLDER_ID

    def _get_aggregator_template_sheet_id(self) -> str:
        return settings.EXASOL_TEMPLATE_SHEET_ID

    def _get_aggregator_sheet_range(self) -> str:
        return "A:AQ"

    def _get_aggregator_df(self):
        return self.initial_aggregator_df[
            self.initial_aggregator_df[self.transaction_key_column_name] != self.chargeback_code].copy()

    def _get_chargebacks_df(self) -> pd.DataFrame:
        return self.initial_aggregator_df[
            self.initial_aggregator_df[self.transaction_key_column_name] == self.chargeback_code].copy()

    def _format_specific_df_for_conciliation(self, exasol_df) -> pd.DataFrame:
        exasol_df[FECHA_PROCESO] = pd.to_datetime(exasol_df[FECHA_PROCESO])
        return exasol_df

    def _get_conglomerate_tab_mapping_dictionary(self):
        return settings.EXASOL_MIDS_CONGLOMERATE_DICT
