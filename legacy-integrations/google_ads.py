"""
Google ADS module for data extraction functions.
This module contains functions specific to the Google ADS integration.
"""

from core import gcs


def run_extract_locations(customer):
    from datetime import timedelta
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
    import logging
    import pandas as pd
    from datetime import datetime
    from google.cloud import storage
    import os
    import pathlib

    # Variáveis globais
    BUCKET_NAME = customer['bucket_name']
    DEVELOPER_TOKEN = customer['developer_token']
    CLIENT_ID = customer['client_id']
    CLIENT_SECRET = customer['client_secret']
    REFRESH_TOKEN = customer['refresh_token']
    LOGIN_CUSTOMER_ID = customer['login_customer_id']
    GCS_FOLDER = "google_ads_locations"

    accounts_ids_raw = customer['subaccount_ids']
    if isinstance(accounts_ids_raw, str):
        raw_ids = [acc.strip() for acc in accounts_ids_raw.split(',') if acc.strip()]
    else:
        raw_ids = accounts_ids_raw

    SUBACCOUNT_IDS = raw_ids

    START_DATE = customer['start_date']
    END_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # Até ontem

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # 🔧 Configuração de logs
    logging.basicConfig(level=logging.WARNING)  # Só mostrar erros importantes

    class GoogleAdsLocationExtractor:
        def __init__(self):
            """Inicializa o extrator de localização do Google Ads"""
            print("🌍 Conectando ao Google Ads...")

            config = {
                "developer_token": DEVELOPER_TOKEN,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": REFRESH_TOKEN,
                "login_customer_id": LOGIN_CUSTOMER_ID,
                "use_proto_plus": True
            }

            try:
                self.client = GoogleAdsClient.load_from_dict(config)
                self.service = self.client.get_service("GoogleAdsService")
                print("✅ Conexão com Google Ads estabelecida!")
            except Exception as e:
                print(f"❌ Erro na conexão Google Ads: {e}")
                raise

            # Inicializar Google Cloud Storage
            try:
                print("☁️ Conectando ao Google Cloud Storage...")
                self.storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
                self.bucket = self.storage_client.bucket(BUCKET_NAME)
                print("✅ Conexão com GCS estabelecida!")
            except Exception as e:
                print(f"❌ Erro na conexão GCS: {e}")
                raise

            # Cache para mapeamento de IDs geográficos para nomes
            self.geo_mapping_cache = {}
            self.geo_mapping_loaded = False

        def validate_access(self):
            """Verifica se tem acesso às contas"""
            print("\n🔍 Verificando acesso às contas...")

            # Verificar MCC
            try:
                query = "SELECT customer.id, customer.descriptive_name FROM customer LIMIT 1"
                request = self.client.get_type("SearchGoogleAdsRequest")
                request.customer_id = LOGIN_CUSTOMER_ID
                request.query = query

                response = self.service.search(request)
                for row in response:
                    print(f"✅ Conta principal: {row.customer.descriptive_name}")
                    break
            except Exception as e:
                print(f"❌ Erro no acesso à conta principal: {e}")
                return False

            # Verificar subcontas
            valid_accounts = []
            print("📊 Verificando contas de anúncios...")

            for account_id in SUBACCOUNT_IDS:
                try:
                    request = self.client.get_type("SearchGoogleAdsRequest")
                    request.customer_id = account_id
                    request.query = query

                    response = self.service.search(request)
                    for row in response:
                        formatted_id = f"{account_id[:3]}-{account_id[3:6]}-{account_id[6:]}"
                        print(f"   ✅ {row.customer.descriptive_name} ({formatted_id})")
                        valid_accounts.append(account_id)
                        break
                except Exception as e:
                    formatted_id = f"{account_id[:3]}-{account_id[3:6]}-{account_id[6:]}"
                    print(f"   ❌ Conta {formatted_id}: Sem acesso")

            if not valid_accounts:
                print("❌ Nenhuma conta acessível encontrada!")
                return False

            print(f"✅ Total: {len(valid_accounts)} contas acessíveis")
            return valid_accounts

        def is_date_processed(self, date_str: str) -> bool:
            """Verifica se a data já foi processada no GCS"""
            date_parts = date_str.split('-')
            filename = f"google_ads_locations_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"
            blob_name = f"{GCS_FOLDER}/{filename}"
            return self.bucket.blob(blob_name).exists()

        def get_dates_to_process(self, start_date: str, end_date: str) -> list:
            """Retorna lista de datas que precisam ser processadas"""
            print(f"\n📅 Verificando datas já processadas no bucket...")

            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')

            dates_to_process = []
            total_dates = 0
            processed_dates = 0

            current = start
            while current <= end:
                date_str = current.strftime('%Y-%m-%d')
                total_dates += 1

                if self.is_date_processed(date_str):
                    date_formatted = current.strftime('%d/%m/%Y')
                    print(f"   ✅ {date_formatted}: Já processado")
                    processed_dates += 1
                else:
                    date_formatted = current.strftime('%d/%m/%Y')
                    print(f"   ❌ {date_formatted}: Pendente")
                    dates_to_process.append(date_str)

                current += timedelta(days=1)

            print(f"\n📊 Resumo da verificação:")
            print(f"   📅 Total de datas: {total_dates}")
            print(f"   ✅ Já processadas: {processed_dates}")
            print(f"   ❌ Pendentes: {len(dates_to_process)}")

            return dates_to_process

        def load_geo_mapping(self, customer_ids):
            """Carrega mapeamento de IDs geográficos para nomes"""
            if self.geo_mapping_loaded:
                return

            print("🗺️ Carregando mapeamento de localização geográfica...")

            # Query para obter todos os geo targets disponíveis
            geo_query = """
                        SELECT geo_target_constant.id,
                               geo_target_constant.name,
                               geo_target_constant.resource_name,
                               geo_target_constant.country_code,
                               geo_target_constant.target_type,
                               geo_target_constant.canonical_name
                        FROM geo_target_constant
                        WHERE geo_target_constant.status = 'ENABLED' \
                        """

            # Usar qualquer customer_id válido para a consulta
            if customer_ids:
                try:
                    request = self.client.get_type("SearchGoogleAdsStreamRequest")
                    request.customer_id = customer_ids[0]
                    request.query = geo_query

                    response = self.service.search_stream(request)

                    for batch in response:
                        for row in batch.results:
                            geo_id = str(row.geo_target_constant.id)
                            geo_name = row.geo_target_constant.name
                            canonical_name = row.geo_target_constant.canonical_name

                            # Usar o nome canônico se disponível, senão usar o nome simples
                            display_name = canonical_name if canonical_name else geo_name

                            self.geo_mapping_cache[geo_id] = display_name

                    print(f"   ✅ {len(self.geo_mapping_cache):,} localizações carregadas")
                    self.geo_mapping_loaded = True

                except Exception as e:
                    print(f"   ⚠️ Erro ao carregar mapeamento geográfico: {e}")
                    print("   └─ Continuando sem mapeamento (IDs serão mantidos)")

        def get_geo_name(self, geo_resource_name):
            """Converte resource_name geográfico para nome legível"""
            if not geo_resource_name or geo_resource_name == 'N/A':
                return 'N/A'

            # Extrair ID do resource_name (formato: geoTargetConstants/12345)
            try:
                geo_id = geo_resource_name.split('/')[-1]
                return self.geo_mapping_cache.get(geo_id, geo_resource_name)
            except:
                return geo_resource_name

        def parse_geo_location(self, geo_name):
            """
            Extrai componentes geográficos do nome completo
            Ex: "Rio Branco,State of Acre,Brazil" → {"city": "Rio Branco", "state": "Acre", "country": "Brazil"}
            """
            if not geo_name or geo_name == 'N/A':
                return {"city": "N/A", "state": "N/A", "country": "N/A"}

            try:
                # Dividir por vírgulas
                parts = [part.strip() for part in geo_name.split(',')]

                if len(parts) >= 3:
                    city = parts[0]

                    # Limpar "State of" do estado
                    state = parts[1]
                    if state.startswith("State of "):
                        state = state.replace("State of ", "")

                    country = parts[2]

                    return {
                        "city": city,
                        "state": state,
                        "country": country
                    }
                elif len(parts) == 2:
                    # Pode ser Estado,País ou Cidade,País
                    if parts[0].startswith("State of"):
                        return {
                            "city": "N/A",
                            "state": parts[0].replace("State of ", ""),
                            "country": parts[1]
                        }
                    else:
                        return {
                            "city": parts[0],
                            "state": "N/A",
                            "country": parts[1]
                        }
                else:
                    # Só um componente
                    return {
                        "city": "N/A",
                        "state": "N/A",
                        "country": parts[0]
                    }

            except Exception:
                # Se der erro, retorna o valor original
                return {
                    "city": geo_name,
                    "state": "N/A",
                    "country": "N/A"
                }

        def clean_geo_field(self, geo_name):
            """
            Remove tudo após a primeira vírgula do campo geográfico
            Ex: "Rio Branco,State of Acre,Brazil" → "Rio Branco"
            Ex: "State of Acre,Brazil" → "State of Acre"
            """
            if not geo_name or geo_name == 'N/A':
                return geo_name

            # Dividir por vírgula e pegar apenas o primeiro componente
            return geo_name.split(',')[0].strip()

        def format_currency(self, value_micros):
            """Converte micros para reais com 2 casas decimais"""
            if value_micros is None:
                return 0.00
            return round(float(value_micros) / 1_000_000, 2)

        def format_percentage(self, decimal_value):
            """Converte decimal para percentual"""
            if decimal_value is None:
                return "0.00%"
            return f"{float(decimal_value * 100):.2f}%"

        def extract_location_data(self, customer_ids, date):
            """Extrai dados de geolocalização para uma data específica"""
            date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
            print(f"\n🌍 Extraindo dados de localização para {date_formatted}...")

            # Carregar mapeamento geográfico
            self.load_geo_mapping(customer_ids)

            # Query para dados de geolocalização usando geographic_view
            # Baseada nos campos do Windsor.ai: geo_target_city, geo_target_region
            # Nota: absolute_top_impression_percentage não é compatível com geographic_view
            location_query = f"""
                SELECT 
                    customer.id,
                    customer.descriptive_name,
                    campaign.name,
                    ad_group.name,
                    segments.date,
                    segments.geo_target_city,
                    segments.geo_target_region,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.ctr,
                    metrics.average_cpc,
                    metrics.average_cpm,
                    metrics.conversions_from_interactions_rate,
                    metrics.cost_per_conversion
                FROM geographic_view
                WHERE segments.date = '{date}'
                AND geographic_view.location_type = 'LOCATION_OF_PRESENCE'
                ORDER BY customer.id, campaign.name
            """

            location_data = []

            for customer_id in customer_ids:
                try:
                    print(f"   └─ Processando localização da conta: {customer_id}")

                    request = self.client.get_type("SearchGoogleAdsStreamRequest")
                    request.customer_id = customer_id
                    request.query = location_query

                    response = self.service.search_stream(request)
                    account_records = 0

                    for batch in response:
                        for row in batch.results:
                            try:
                                # Converter IDs geográficos para nomes usando mapeamento
                                geo_city_raw = self.get_geo_name(row.segments.geo_target_city) if hasattr(row.segments,
                                                                                                          'geo_target_city') and row.segments.geo_target_city else 'N/A'
                                geo_region_raw = self.get_geo_name(row.segments.geo_target_region) if hasattr(
                                    row.segments,
                                    'geo_target_region') and row.segments.geo_target_region else 'N/A'

                                # Extrair componentes geográficos estruturados
                                city_components = self.parse_geo_location(geo_city_raw)
                                region_components = self.parse_geo_location(geo_region_raw)

                                # Usar a cidade mais específica disponível
                                final_city = city_components["city"] if city_components["city"] != "N/A" else \
                                    region_components["city"]
                                final_state = city_components["state"] if city_components["state"] != "N/A" else \
                                    region_components["state"]
                                final_country = city_components["country"] if city_components["country"] != "N/A" else \
                                    region_components["country"]

                                # Estrutura otimizada com campos organizados apenas
                                data_row = {
                                    'account_id': str(row.customer.id),
                                    'account_name': row.customer.descriptive_name,
                                    'campaign': row.campaign.name,
                                    'ad_group_name': row.ad_group.name if hasattr(row, 'ad_group') and hasattr(
                                        row.ad_group,
                                        'name') else 'N/A',
                                    'date': date,

                                    # Campos organizados (únicos)
                                    'city': final_city,
                                    'state': final_state,
                                    'country': final_country,

                                    # Métricas
                                    'impressions': int(row.metrics.impressions),
                                    'clicks': int(row.metrics.clicks),
                                    'spend': self.format_currency(row.metrics.cost_micros),
                                    'conversions': round(float(row.metrics.conversions), 2),
                                    'ctr': self.format_percentage(row.metrics.ctr),
                                    'average_cpc': self.format_currency(row.metrics.average_cpc),
                                    'average_cpm': self.format_currency(row.metrics.average_cpm),
                                    'conversion_rate': self.format_percentage(
                                        row.metrics.conversions_from_interactions_rate),
                                    'cost_per_conversion': self.format_currency(row.metrics.cost_per_conversion),
                                    'absolute_top_impression_percentage': '0.00%'  # Não disponível em geographic_view
                                }

                                location_data.append(data_row)
                                account_records += 1

                            except Exception as e:
                                print(f"   ⚠️ Erro ao processar linha: {e}")
                                continue

                    if account_records > 0:
                        formatted_id = f"{customer_id[:3]}-{customer_id[3:6]}-{customer_id[6:]}"
                        print(f"   📊 {formatted_id}: {account_records:,} registros de localização")

                except Exception as e:
                    formatted_id = f"{customer_id[:3]}-{customer_id[3:6]}-{customer_id[6:]}"
                    print(f"   ❌ {formatted_id}: Erro na extração de localização - {str(e)[:100]}...")

            print(f"✅ Total extraído: {len(location_data):,} registros de localização")
            return location_data

        def save_to_gcs(self, data, date):
            """Salva arquivo no Google Cloud Storage"""
            if not data:
                print("⚠️ Nenhum dado para salvar no GCS")
                return None

            # Nome do arquivo: google_ads_locations_02_08_2025.csv
            date_parts = date.split('-')
            filename = f"google_ads_locations_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"
            blob_name = f"{GCS_FOLDER}/{filename}"

            try:
                # Criar DataFrame e converter para CSV
                df = pd.DataFrame(data)
                csv_data = df.to_csv(index=False).encode('utf-8')

                # Upload para GCS
                blob = self.bucket.blob(blob_name)
                blob.upload_from_string(csv_data, content_type='text/csv')

                print(f"☁️ Arquivo salvo no GCS: {filename}")
                print(f"   📊 {len(df):,} linhas salvas")

                return blob_name

            except Exception as e:
                print(f"❌ Erro ao salvar no GCS: {e}")
                return None

    def main():
        print("🌍 EXTRATOR DE LOCALIZAÇÃO GOOGLE ADS")
        print("=" * 50)
        print("📊 Gerando arquivos CSV por dia com dados de geolocalização")
        print("🎯 Campos organizados: city, state, country")

        # Inicializar extrator
        try:
            extractor = GoogleAdsLocationExtractor()
        except Exception as e:
            print(f"❌ Falha na inicialização: {e}")
            return

        # Validar acesso
        valid_accounts = extractor.validate_access()
        if not valid_accounts:
            print("❌ Sem acesso às contas. Verifique as credenciais.")
            return

        # Verificar datas pendentes PRIMEIRO
        dates_to_process = extractor.get_dates_to_process(START_DATE, END_DATE)

        if not dates_to_process:
            print("\n✅ Todas as datas de localização já foram processadas!")
            print("   Nenhuma extração necessária.")
            return

        # Informações do período
        start_formatted = datetime.strptime(START_DATE, '%Y-%m-%d').strftime('%d/%m/%Y')
        end_formatted = datetime.strptime(END_DATE, '%Y-%m-%d').strftime('%d/%m/%Y')

        print(f"\n🗓️ Período configurado: {start_formatted} até {end_formatted}")
        print(f"📅 Datas para processar: {len(dates_to_process)}")
        print(f"🏢 Contas: {len(valid_accounts)}")

        # Extrair dados dia por dia
        print(f"\n{'=' * 50}")
        print("🔄 INICIANDO EXTRAÇÃO DE LOCALIZAÇÃO...")
        print(f"{'=' * 50}")

        files_created = []
        total_records = 0

        for i, date in enumerate(dates_to_process, 1):
            print(f"\n[{i}/{len(dates_to_process)}] Processando localização para {date}...")

            try:
                location_data = extractor.extract_location_data(valid_accounts, date)
                if location_data:
                    # Salvar no GCS
                    gcs_path = extractor.save_to_gcs(location_data, date)
                    if gcs_path:
                        files_created.append(gcs_path)

                    total_records += len(location_data)
                else:
                    date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
                    print(f"   ⚠️ {date_formatted}: Sem dados de localização encontrados")

            except Exception as e:
                date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
                print(f"   ❌ {date_formatted}: Erro na extração - {e}")

        # Resumo final
        print(f"\n{'=' * 50}")
        print("🎉 EXTRAÇÃO DE LOCALIZAÇÃO CONCLUÍDA!")
        print(f"{'=' * 50}")
        print(f"☁️ Arquivos salvos no GCS: {len(files_created)}")
        print(f"📊 Total de registros: {total_records:,}")
        print(f"📂 Bucket GCS: {BUCKET_NAME}/{GCS_FOLDER}/")

        if files_created:
            print(f"\n📋 Últimos arquivos gerados:")
            for gcs_path in files_created[-3:]:  # Mostrar últimos 3
                filename = gcs_path.split('/')[-1]
                print(f"   ☁️ {filename}")
            if len(files_created) > 3:
                print(f"   ... e mais {len(files_created) - 3} arquivos")

        print(f"\n✅ Pronto! Dados de localização salvos no Google Cloud Storage")
        print(f"   └─ Bucket: gs://{BUCKET_NAME}/{GCS_FOLDER}/")

    # START
    main()


def run_extract_keywords(customer):
    from datetime import timedelta
    from google.ads.googleads.client import GoogleAdsClient
    import logging
    import pandas as pd
    from datetime import datetime
    from google.cloud import storage
    import os
    import pathlib

    # Variáveis globais
    BUCKET_NAME = customer['bucket_name']
    DEVELOPER_TOKEN = customer['developer_token']
    CLIENT_ID = customer['client_id']
    CLIENT_SECRET = customer['client_secret']
    REFRESH_TOKEN = customer['refresh_token']
    LOGIN_CUSTOMER_ID = customer['login_customer_id']
    GCS_FOLDER = "google_ads_keywords"
    OUTPUT_FOLDER = 'google_ads_exports'

    accounts_ids_raw = customer['subaccount_ids']
    if isinstance(accounts_ids_raw, str):
        raw_ids = [acc.strip() for acc in accounts_ids_raw.split(',') if acc.strip()]
    else:
        raw_ids = accounts_ids_raw

    SUBACCOUNT_IDS = raw_ids

    START_DATE = customer['start_date']
    END_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # Até ontem

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # 🔧 Configuração de logs
    logging.basicConfig(level=logging.WARNING)  # Só mostrar erros importantes

    class GoogleAdsExtractor:
        def __init__(self):
            """Inicializa o extrator do Google Ads"""
            print("🔧 Conectando ao Google Ads...")

            config = {
                "developer_token": DEVELOPER_TOKEN,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": REFRESH_TOKEN,
                "login_customer_id": LOGIN_CUSTOMER_ID,
                "use_proto_plus": True
            }

            try:
                self.client = GoogleAdsClient.load_from_dict(config)
                self.service = self.client.get_service("GoogleAdsService")
                print("✅ Conexão com Google Ads estabelecida!")
            except Exception as e:
                print(f"❌ Erro na conexão Google Ads: {e}")
                raise

            # Inicializar Google Cloud Storage
            try:
                print("☁️ Conectando ao Google Cloud Storage...")
                self.storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
                self.bucket = self.storage_client.bucket(BUCKET_NAME)
                print("✅ Conexão com GCS estabelecida!")
            except Exception as e:
                print(f"❌ Erro na conexão GCS: {e}")
                raise

        def validate_access(self):
            """Verifica se tem acesso às contas"""
            print("\n🔍 Verificando acesso às contas...")

            # Verificar MCC
            try:
                query = "SELECT customer.id, customer.descriptive_name FROM customer LIMIT 1"
                request = self.client.get_type("SearchGoogleAdsRequest")
                request.customer_id = LOGIN_CUSTOMER_ID
                request.query = query

                response = self.service.search(request)
                for row in response:
                    print(f"✅ Conta principal: {row.customer.descriptive_name}")
                    break
            except Exception as e:
                print(f"❌ Erro no acesso à conta principal: {e}")
                return False

            # Verificar subcontas
            valid_accounts = []
            print("📊 Verificando contas de anúncios...")

            for account_id in SUBACCOUNT_IDS:
                try:
                    request = self.client.get_type("SearchGoogleAdsRequest")
                    request.customer_id = account_id
                    request.query = query

                    response = self.service.search(request)
                    for row in response:
                        formatted_id = f"{account_id[:3]}-{account_id[3:6]}-{account_id[6:]}"
                        print(f"   ✅ {row.customer.descriptive_name} ({formatted_id})")
                        valid_accounts.append(account_id)
                        break
                except Exception as e:
                    formatted_id = f"{account_id[:3]}-{account_id[3:6]}-{account_id[6:]}"
                    print(f"   ❌ Conta {formatted_id}: Sem acesso")

            if not valid_accounts:
                print("❌ Nenhuma conta acessível encontrada!")
                return False

            print(f"✅ Total: {len(valid_accounts)} contas acessíveis")
            return valid_accounts

        def is_date_processed(self, date_str: str) -> bool:
            """Verifica se a data já foi processada no GCS"""
            date_parts = date_str.split('-')
            filename = f"google_ads_keywords_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"
            blob_name = f"{GCS_FOLDER}/{filename}"
            return self.bucket.blob(blob_name).exists()

        def get_dates_to_process(self, start_date: str, end_date: str) -> list:
            """Retorna lista de datas que precisam ser processadas"""
            print(f"\n📅 Verificando datas já processadas no bucket...")

            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')

            dates_to_process = []
            total_dates = 0
            processed_dates = 0

            current = start
            while current <= end:
                date_str = current.strftime('%Y-%m-%d')
                total_dates += 1

                if self.is_date_processed(date_str):
                    date_formatted = current.strftime('%d/%m/%Y')
                    print(f"   ✅ {date_formatted}: Já processado")
                    processed_dates += 1
                else:
                    date_formatted = current.strftime('%d/%m/%Y')
                    print(f"   ❌ {date_formatted}: Pendente")
                    dates_to_process.append(date_str)

                current += timedelta(days=1)

            print(f"\n📊 Resumo da verificação:")
            print(f"   📅 Total de datas: {total_dates}")
            print(f"   ✅ Já processadas: {processed_dates}")
            print(f"   ❌ Pendentes: {len(dates_to_process)}")

            return dates_to_process

        def format_currency(self, value_micros):
            """Converte micros para reais com 2 casas decimais"""
            if value_micros is None:
                return 0.00
            return round(float(value_micros) / 1_000_000, 2)

        def format_percentage(self, decimal_value):
            """Converte decimal para percentual"""
            if decimal_value is None:
                return "0.00%"
            return f"{float(decimal_value * 100):.2f}%"

        def get_keyword_mapping(self, customer_ids, date):
            """Mapeia palavras-chave por grupo de anúncios para um dia específico"""
            keyword_map = {}
            search_term_map = {}

            # Query para keywords
            keyword_query = f"""
                SELECT 
                    customer.id,
                    ad_group.id,
                    ad_group_criterion.keyword.text,
                    segments.date
                FROM keyword_view
                WHERE segments.date = '{date}'
            """

            # Query para search terms
            search_term_query = f"""
                SELECT 
                    customer.id,
                    ad_group.id,
                    segments.keyword.info.text,
                    search_term_view.search_term,
                    segments.date,
                    metrics.impressions
                FROM search_term_view
                WHERE segments.date = '{date}'
            """

            for customer_id in customer_ids:
                try:
                    # Mapear keywords
                    request = self.client.get_type("SearchGoogleAdsStreamRequest")
                    request.customer_id = customer_id
                    request.query = keyword_query

                    response = self.service.search_stream(request)
                    for batch in response:
                        for row in batch.results:
                            key = f"{row.customer.id}_{row.ad_group.id}_{date}"
                            if key not in keyword_map:
                                keyword_map[key] = []

                            keyword = row.ad_group_criterion.keyword.text
                            if keyword not in keyword_map[key]:
                                keyword_map[key].append(keyword)

                    # Mapear search terms
                    request.query = search_term_query
                    response = self.service.search_stream(request)
                    for batch in response:
                        for row in batch.results:
                            st_key = f"{row.customer.id}_{row.ad_group.id}_{date}_{row.segments.keyword.info.text}"
                            if st_key not in search_term_map:
                                search_term_map[st_key] = []

                            search_term_map[st_key].append({
                                'term': row.search_term_view.search_term,
                                'impressions': int(row.metrics.impressions)
                            })

                except Exception:
                    continue

            return keyword_map, search_term_map

        def extract_day_data(self, customer_ids, date):
            """Extrai dados de um dia específico"""
            date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
            print(f"\n📅 Extraindo dados de {date_formatted}...")

            # Buscar mapeamentos primeiro
            keyword_map, search_term_map = self.get_keyword_mapping(customer_ids, date)

            # Query principal para dados do ad_group
            main_query = f"""
                SELECT 
                    customer.id,
                    customer.descriptive_name,
                    campaign.id,
                    campaign.name,
                    ad_group.id,
                    ad_group.name,
                    segments.date,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.ctr,
                    metrics.average_cpc,
                    metrics.average_cpm,
                    metrics.conversions_from_interactions_rate,
                    metrics.cost_per_conversion,
                    metrics.absolute_top_impression_percentage,
                    metrics.interactions
                FROM ad_group
                WHERE segments.date = '{date}'
                ORDER BY customer.id, campaign.id, ad_group.id
            """

            day_data = []
            total_groups = 0
            total_keywords = 0

            for customer_id in customer_ids:
                try:
                    request = self.client.get_type("SearchGoogleAdsStreamRequest")
                    request.customer_id = customer_id
                    request.query = main_query

                    response = self.service.search_stream(request)
                    account_groups = 0
                    account_keywords = 0

                    for batch in response:
                        for row in batch.results:
                            # Buscar keywords para este grupo
                            map_key = f"{row.customer.id}_{row.ad_group.id}_{date}"
                            keywords = keyword_map.get(map_key, ["NO_KEYWORDS"])

                            # Dados base do grupo
                            base_data = {
                                'account_id': str(row.customer.id),
                                'account_name': row.customer.descriptive_name,
                                'campaign_id': str(row.campaign.id),
                                'campaign': row.campaign.name,
                                'ad_group_name': row.ad_group.name,
                                'date': date,
                                'impressions': int(row.metrics.impressions),
                                'clicks': int(row.metrics.clicks),
                                'spend': self.format_currency(row.metrics.cost_micros),
                                'conversions': round(float(row.metrics.conversions), 2),
                                'ctr': self.format_percentage(row.metrics.ctr),
                                'average_cpc': self.format_currency(row.metrics.average_cpc),
                                'average_cpm': self.format_currency(row.metrics.average_cpm),
                                'conversion_rate': self.format_percentage(
                                    row.metrics.conversions_from_interactions_rate),
                                'cost_per_conversion': self.format_currency(row.metrics.cost_per_conversion),
                                'absolute_top_impression_percentage': self.format_percentage(
                                    row.metrics.absolute_top_impression_percentage),
                                'interactions': int(row.metrics.interactions)
                            }

                            # Criar uma linha por keyword
                            if len(keywords) == 1:
                                # Grupo com uma keyword ou sem keywords
                                data_row = base_data.copy()
                                keyword = keywords[0]
                                data_row['keyword_text'] = keyword

                                # Buscar search term se disponível
                                if keyword != "NO_KEYWORDS":
                                    st_key = f"{row.customer.id}_{row.ad_group.id}_{date}_{keyword}"
                                    search_terms = search_term_map.get(st_key, [])
                                    if search_terms:
                                        best_term = max(search_terms, key=lambda x: x['impressions'])
                                        data_row['search_term'] = best_term['term']
                                    else:
                                        data_row['search_term'] = 'NO_SEARCH_TERMS'
                                else:
                                    data_row['search_term'] = 'NO_KEYWORDS'

                                day_data.append(data_row)
                                account_keywords += 1

                            else:
                                # Grupo com múltiplas keywords - dividir métricas de volume
                                num_keywords = len(keywords)

                                for keyword in keywords:
                                    data_row = base_data.copy()
                                    data_row['keyword_text'] = keyword

                                    # Dividir métricas de volume
                                    data_row['impressions'] = round(data_row['impressions'] / num_keywords)
                                    data_row['clicks'] = round(data_row['clicks'] / num_keywords)
                                    data_row['spend'] = round(data_row['spend'] / num_keywords, 2)
                                    data_row['conversions'] = round(data_row['conversions'] / num_keywords, 2)
                                    data_row['interactions'] = round(data_row['interactions'] / num_keywords)

                                    # Buscar search term
                                    st_key = f"{row.customer.id}_{row.ad_group.id}_{date}_{keyword}"
                                    search_terms = search_term_map.get(st_key, [])
                                    if search_terms:
                                        best_term = max(search_terms, key=lambda x: x['impressions'])
                                        data_row['search_term'] = best_term['term']
                                    else:
                                        data_row['search_term'] = 'NO_SEARCH_TERMS'

                                    day_data.append(data_row)
                                    account_keywords += 1

                            account_groups += 1

                    if account_keywords > 0:
                        formatted_id = f"{customer_id[:3]}-{customer_id[3:6]}-{customer_id[6:]}"
                        print(f"   📊 {formatted_id}: {account_keywords:,} registros")

                    total_groups += account_groups
                    total_keywords += account_keywords

                except Exception as e:
                    formatted_id = f"{customer_id[:3]}-{customer_id[3:6]}-{customer_id[6:]}"
                    print(f"   ❌ {formatted_id}: Erro na extração")

            print(f"✅ Total extraído: {total_keywords:,} registros de keywords")
            return day_data

        def save_to_gcs(self, data, date):
            """Salva arquivo no Google Cloud Storage"""
            if not data:
                print("⚠️ Nenhum dado para salvar no GCS")
                return None

            # Nome do arquivo: google_ads_keywords_02_08_2025.csv
            date_parts = date.split('-')
            filename = f"google_ads_keywords_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"
            blob_name = f"{GCS_FOLDER}/{filename}"

            try:
                # Criar DataFrame e converter para CSV
                df = pd.DataFrame(data)
                csv_data = df.to_csv(index=False).encode('utf-8')

                # Upload para GCS
                blob = self.bucket.blob(blob_name)
                blob.upload_from_string(csv_data, content_type='text/csv')

                print(f"☁️ Arquivo salvo no GCS: {filename}")
                print(f"   📊 {len(df):,} linhas salvas")

                return blob_name

            except Exception as e:
                print(f"❌ Erro ao salvar no GCS: {e}")
                return None

        def save_local_backup(self, data, date):
            """Salva arquivo local como backup"""
            if not data:
                return None

            # Criar pasta se não existir
            os.makedirs(OUTPUT_FOLDER, exist_ok=True)

            # Nome do arquivo: google_ads_keywords_02_08_2025.csv
            date_parts = date.split('-')
            filename = f"google_ads_keywords_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"
            filepath = os.path.join(OUTPUT_FOLDER, filename)

            # Salvar CSV
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False, encoding='utf-8')

            print(f"💾 Backup local salvo: {filename}")

            return filepath

    def main():
        print("🚀 EXTRATOR DE DADOS GOOGLE ADS")
        print("=" * 50)
        print("📊 Gerando arquivos CSV por dia com dados de palavras-chave")

        # Inicializar extrator
        try:
            extractor = GoogleAdsExtractor()
        except Exception as e:
            print(f"❌ Falha na inicialização: {e}")
            return

        # Validar acesso
        valid_accounts = extractor.validate_access()
        if not valid_accounts:
            print("❌ Sem acesso às contas. Verifique as credenciais.")
            return

        # Verificar datas pendentes PRIMEIRO
        dates_to_process = extractor.get_dates_to_process(START_DATE, END_DATE)

        if not dates_to_process:
            print("\n✅ Todas as datas já foram processadas!")
            print("   Nenhuma extração necessária.")
            return

        # Informações do período
        start_formatted = datetime.strptime(START_DATE, '%Y-%m-%d').strftime('%d/%m/%Y')
        end_formatted = datetime.strptime(END_DATE, '%Y-%m-%d').strftime('%d/%m/%Y')

        print(f"\n🗓️ Período configurado: {start_formatted} até {end_formatted}")
        print(f"📅 Datas para processar: {len(dates_to_process)}")
        print(f"🏢 Contas: {len(valid_accounts)}")

        # Extrair dados dia por dia
        print(f"\n{'=' * 50}")
        print("🔄 INICIANDO EXTRAÇÃO...")
        print(f"{'=' * 50}")

        files_created = []
        backups_created = []
        total_records = 0

        for i, date in enumerate(dates_to_process, 1):
            print(f"\n[{i}/{len(dates_to_process)}] Processando dia {i} de {len(dates_to_process)}...")

            try:
                day_data = extractor.extract_day_data(valid_accounts, date)
                if day_data:
                    # Salvar no GCS (principal)
                    gcs_path = extractor.save_to_gcs(day_data, date)
                    if gcs_path:
                        files_created.append(gcs_path)

                    # Salvar backup local
                    local_path = extractor.save_local_backup(day_data, date)
                    if local_path:
                        backups_created.append(local_path)

                    total_records += len(day_data)
                else:
                    date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
                    print(f"   ⚠️ {date_formatted}: Sem dados encontrados")

            except Exception as e:
                date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
                print(f"   ❌ {date_formatted}: Erro na extração - {e}")

        # Resumo final
        print(f"\n{'=' * 50}")
        print("🎉 EXTRAÇÃO CONCLUÍDA!")
        print(f"{'=' * 50}")
        print(f"☁️ Arquivos salvos no GCS: {len(files_created)}")
        print(f"💾 Backups locais criados: {len(backups_created)}")
        print(f"📊 Total de registros: {total_records:,}")
        print(f"📂 Bucket GCS: {BUCKET_NAME}/{GCS_FOLDER}/")
        print(f"📁 Pasta local: {OUTPUT_FOLDER}/")

        if files_created:
            print(f"\n📋 Últimos arquivos gerados:")
            for gcs_path in files_created[-3:]:  # Mostrar últimos 3
                filename = gcs_path.split('/')[-1]
                print(f"   ☁️ {filename}")
            if len(files_created) > 3:
                print(f"   ... e mais {len(files_created) - 3} arquivos")

        print(f"\n✅ Pronto! Dados salvos no Google Cloud Storage")
        print(f"   └─ Bucket: gs://{BUCKET_NAME}/{GCS_FOLDER}/")

    # START
    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for VistaCRM.

    Returns:
        list: List of task configurations
    """
    return [
        # {
        #     'task_id': 'run_extract_locations',
        #     'python_callable': run_extract_locations
        # },
        {
            'task_id': 'run_extract_keywords',
            'python_callable': run_extract_keywords
        }
    ]
