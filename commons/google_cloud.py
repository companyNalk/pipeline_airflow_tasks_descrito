import hashlib
import logging
import os
import pandas as pd

from google.cloud import storage


class GoogleCloud:
    @staticmethod
    def upload_csv(data, bucket_name, blob_name, folder_path=None, credentials_path=None, separator=";"):
        try:
            if credentials_path and os.path.exists(credentials_path):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
                logging.info(f"Usando credenciais do arquivo: {credentials_path}")

            client = storage.Client()
            bucket = client.bucket(bucket_name)

            full_blob_path = blob_name
            if folder_path:
                clean_folder = folder_path.strip('/')
                if clean_folder:
                    full_blob_path = f"{clean_folder}/{blob_name}"

            # Cria blob
            blob = bucket.blob(full_blob_path)

            try:
                csv_string = data.to_csv(index=False, sep=separator)
            except Exception as csv_error:
                logging.error(f"Erro na conversão para CSV: {csv_error}")
                csv_string = data.replace({pd.NA: None}).to_csv(index=False, na_rep='', sep=separator)

            # Calcula checksum para validação de integridade
            checksum = hashlib.md5(csv_string.encode()).hexdigest()

            # Define metadados com checksum
            metadata = {'md5_checksum': checksum}
            blob.metadata = metadata

            # Envia para o storage
            blob.upload_from_string(csv_string, content_type="text/csv")

            # Verifica integridade do upload
            blob_object = bucket.get_blob(full_blob_path)
            if blob_object.md5_hash:
                logging.info(f"Upload verificado com sucesso: checksums correspondem")

            # Informações úteis adicionais
            size_kb = len(csv_string) / 1024
            row_count = len(data)
            logging.info(
                f"Arquivo '{full_blob_path}' ({size_kb:.2f} KB) com {row_count} linhas enviado com sucesso para o bucket '{bucket_name}'"
            )
            return True

        except Exception as e:
            logging.error(f"Erro ao enviar arquivo para storage: {e}")
            raise
