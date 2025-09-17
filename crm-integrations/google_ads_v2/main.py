# !/usr/bin/env python3
"""
🚀 GOOGLE ADS DATA EXTRACTOR - UNIFIED VERSION
Extrai dados de palavras-chave E localização do Google Ads com qualidade profissional
Gera arquivos CSV por dia com todas as métricas importantes
Integrado com a estrutura existente do projeto
"""

import os
from datetime import datetime, timedelta

import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.cloud import storage

from commons.app_inicializer import AppInitializer
from generic.argument_manager import ArgumentManager

# Inicializar logger através da estrutura existente
logger = AppInitializer.initialize()


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para extrair dados do Google Ads (Keywords e Locations) e processar no BigQuery")
            .add("DEVELOPER_TOKEN", "Token de desenvolvedor do Google Ads", required=True)
            .add("CLIENT_ID", "ID do cliente OAuth2", required=True)
            .add("CLIENT_SECRET", "Secret do cliente OAuth2", required=True)
            .add("REFRESH_TOKEN", "Token de refresh OAuth2", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .add("BUCKET_NAME", "Nome do bucket GCS", required=True)
            .add("START_DATE", "Data inicial (YYYY-MM-DD)", required=True)
            .add("SUBACCOUNT_IDS", "IDs das contas separados por vírgula (ex: 1234567890,9876543210)", required=True)
            .add("LOGIN_CUSTOMER_ID", "ID da conta gerenciadora (MCC)", required=True)
            .add("END_DATE", "Data final (YYYY-MM-DD)", required=False)
            .add("EXTRACT_KEYWORDS", "Extrair dados de keywords (true/false)", default="true")
            .add("EXTRACT_LOCATIONS", "Extrair dados de localização (true/false)", default="true")
            .add("GCS_FOLDER_KEYWORDS", "Pasta no bucket para keywords", default="google_ads_keywords")
            .add("GCS_FOLDER_LOCATIONS", "Pasta no bucket para locations", default="google_ads_locations")
            .add("OUTPUT_FOLDER", "Pasta local de backup", default="google_ads_exports")
            .parse())


class GoogleAdsExtractor:
    def __init__(self, args):
        """Inicializa o extrator do Google Ads"""
        self.args = args

        # Processar subaccount_ids do formato string para lista
        self.subaccount_ids = [id.strip() for id in args.SUBACCOUNT_IDS.split(',')]
        logger.info(f"🎯 Contas configuradas: {len(self.subaccount_ids)} contas")
        for i, account_id in enumerate(self.subaccount_ids, 1):
            formatted_id = f"{account_id[:3]}-{account_id[3:6]}-{account_id[6:]}" if len(
                account_id) >= 6 else account_id
            logger.info(f"   {i}. {formatted_id}")

        logger.info("🔧 Conectando ao Google Ads...")

        config = {
            "developer_token": args.DEVELOPER_TOKEN,
            "client_id": args.CLIENT_ID,
            "client_secret": args.CLIENT_SECRET,
            "refresh_token": args.REFRESH_TOKEN,
            "login_customer_id": args.LOGIN_CUSTOMER_ID,
            "use_proto_plus": True
        }

        try:
            self.client = GoogleAdsClient.load_from_dict(config)
            self.service = self.client.get_service("GoogleAdsService")
            logger.info("✅ Conexão com Google Ads estabelecida!")
        except Exception as e:
            logger.error(f"❌ Erro na conexão Google Ads: {e}")
            raise

        # Inicializar Google Cloud Storage
        try:
            logger.info("☁️ Conectando ao Google Cloud Storage...")
            # Usar as credenciais definidas na variável de ambiente
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.GOOGLE_APPLICATION_CREDENTIALS
            self.storage_client = storage.Client(project=args.PROJECT_ID)
            self.bucket = self.storage_client.bucket(args.BUCKET_NAME)
            logger.info("✅ Conexão com GCS estabelecida!")
        except Exception as e:
            logger.error(f"❌ Erro na conexão GCS: {e}")
            raise

        # Cache para mapeamento de IDs geográficos para nomes (apenas para locations)
        self.geo_mapping_cache = {}
        self.geo_mapping_loaded = False

    def validate_access(self):
        """Verifica se tem acesso às contas"""
        logger.info("\n🔍 Verificando acesso às contas...")

        # Verificar MCC
        try:
            query = "SELECT customer.id, customer.descriptive_name FROM customer LIMIT 1"
            request = self.client.get_type("SearchGoogleAdsRequest")
            request.customer_id = self.args.LOGIN_CUSTOMER_ID
            request.query = query

            response = self.service.search(request)
            for row in response:
                logger.info(f"✅ Conta principal: {row.customer.descriptive_name}")
                break
        except Exception as e:
            logger.error(f"❌ Erro no acesso à conta principal: {e}")
            return False

        # Verificar subcontas
        valid_accounts = []
        logger.info("📊 Verificando contas de anúncios...")

        for account_id in self.subaccount_ids:
            try:
                request = self.client.get_type("SearchGoogleAdsRequest")
                request.customer_id = account_id
                request.query = query

                response = self.service.search(request)
                for row in response:
                    formatted_id = f"{account_id[:3]}-{account_id[3:6]}-{account_id[6:]}"
                    logger.info(f"   ✅ {row.customer.descriptive_name} ({formatted_id})")
                    valid_accounts.append(account_id)
                    break
            except Exception as e:
                formatted_id = f"{account_id[:3]}-{account_id[3:6]}-{account_id[6:]}"
                logger.warning(f"   ❌ Conta {formatted_id}: Sem acesso")

        if not valid_accounts:
            logger.error("❌ Nenhuma conta acessível encontrada!")
            return False

        logger.info(f"✅ Total: {len(valid_accounts)} contas acessíveis")
        return valid_accounts

    def is_date_processed(self, date_str: str, data_type: str) -> bool:
        """Verifica se a data já foi processada no GCS"""
        date_parts = date_str.split('-')

        if data_type == "keywords":
            filename = f"google_ads_keywords_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"
            blob_name = f"{self.args.GCS_FOLDER_KEYWORDS}/{filename}"
        else:  # locations
            filename = f"google_ads_locations_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"
            blob_name = f"{self.args.GCS_FOLDER_LOCATIONS}/{filename}"

        return self.bucket.blob(blob_name).exists()

    def get_dates_to_process(self, start_date: str, end_date: str, data_type: str) -> list:
        """Retorna lista de datas que precisam ser processadas"""
        type_name = "keywords" if data_type == "keywords" else "localização"
        logger.info(f"\n📅 Verificando datas de {type_name} já processadas no bucket...")

        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        dates_to_process = []
        total_dates = 0
        processed_dates = 0

        current = start
        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            total_dates += 1

            if self.is_date_processed(date_str, data_type):
                date_formatted = current.strftime('%d/%m/%Y')
                logger.info(f"   ✅ {date_formatted}: Já processado")
                processed_dates += 1
            else:
                date_formatted = current.strftime('%d/%m/%Y')
                logger.info(f"   ❌ {date_formatted}: Pendente")
                dates_to_process.append(date_str)

            current += timedelta(days=1)

        logger.info(f"\n📊 Resumo da verificação ({type_name}):")
        logger.info(f"   📅 Total de datas: {total_dates}")
        logger.info(f"   ✅ Já processadas: {processed_dates}")
        logger.info(f"   ❌ Pendentes: {len(dates_to_process)}")

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

    # =============================================================================
    # 🔍 KEYWORDS EXTRACTION METHODS
    # =============================================================================

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

    def extract_keywords_data(self, customer_ids, date):
        """Extrai dados de campanhas (todos os tipos) e detalha Search com keywords/search terms"""
        date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
        logger.info(f"\n📅 Extraindo dados de keywords para {date_formatted}...")

        # --- Query 1: Campanhas (todos os canais, EXCETO Search) ---
        campaign_query = f"""
            SELECT
              customer.id,
              customer.descriptive_name,
              campaign.id,
              campaign.name,
              campaign.status,
              campaign.advertising_channel_type,
              campaign.advertising_channel_sub_type,
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
            FROM campaign
            WHERE segments.date = '{date}'
              AND campaign.advertising_channel_type != 'SEARCH'
            ORDER BY customer.id, campaign.id
        """

        # --- Query 2: Ad Groups (apenas Search) ---
        adgroup_query = f"""
            SELECT
              customer.id,
              customer.descriptive_name,
              campaign.id,
              campaign.name,
              campaign.advertising_channel_type,
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

        # --- 1) Coleta campanhas (exceto Search) ---
        for customer_id in customer_ids:
            try:
                req = self.client.get_type("SearchGoogleAdsStreamRequest")
                req.customer_id = customer_id
                req.query = campaign_query
                resp = self.service.search_stream(req)

                for batch in resp:
                    for row in batch.results:
                        record = {
                            "account_id": str(row.customer.id),
                            "account_name": row.customer.descriptive_name,
                            "campaign_id": str(row.campaign.id),
                            "campaign": row.campaign.name,
                            "status": row.campaign.status.name,
                            "channel_type": row.campaign.advertising_channel_type.name,
                            "channel_sub_type": row.campaign.advertising_channel_sub_type.name,
                            "ad_group_id": None,
                            "ad_group_name": None,
                            "keyword_text": None,
                            "search_term": None,
                            "date": date,
                            "impressions": int(row.metrics.impressions),
                            "clicks": int(row.metrics.clicks),
                            "spend": self.format_currency(row.metrics.cost_micros),
                            "conversions": round(float(row.metrics.conversions or 0), 2),
                            "ctr": self.format_percentage(row.metrics.ctr),
                            "average_cpc": self.format_currency(row.metrics.average_cpc),
                            "average_cpm": self.format_currency(row.metrics.average_cpm),
                            "conversion_rate": self.format_percentage(row.metrics.conversions_from_interactions_rate),
                            "cost_per_conversion": self.format_currency(row.metrics.cost_per_conversion),
                            "absolute_top_impression_percentage": self.format_percentage(
                                row.metrics.absolute_top_impression_percentage),
                            "interactions": int(row.metrics.interactions)
                        }
                        day_data.append(record)

            except Exception as e:
                logger.warning(f"   ❌ {customer_id}: erro na query de campanhas ({e})")

        # --- 2) Coleta Ad Groups + keywords (somente Search) ---
        keyword_map, search_term_map = self.get_keyword_mapping(customer_ids, date)

        for customer_id in customer_ids:
            try:
                req = self.client.get_type("SearchGoogleAdsStreamRequest")
                req.customer_id = customer_id
                req.query = adgroup_query
                resp = self.service.search_stream(req)

                for batch in resp:
                    for row in batch.results:
                        # Só detalhar se for Search
                        if row.campaign.advertising_channel_type.name != "SEARCH":
                            continue

                        map_key = f"{row.customer.id}_{row.ad_group.id}_{date}"
                        keywords = keyword_map.get(map_key, ["NO_KEYWORDS"])

                        for kw in keywords:
                            record = {
                                "account_id": str(row.customer.id),
                                "account_name": row.customer.descriptive_name,
                                "campaign_id": str(row.campaign.id),
                                "campaign": row.campaign.name,
                                "status": "ACTIVE",
                                "channel_type": row.campaign.advertising_channel_type.name,
                                "channel_sub_type": None,
                                "ad_group_id": str(row.ad_group.id),
                                "ad_group_name": row.ad_group.name,
                                "keyword_text": kw,
                                "search_term": None,
                                "date": date,
                                "impressions": int(row.metrics.impressions),
                                "clicks": int(row.metrics.clicks),
                                "spend": self.format_currency(row.metrics.cost_micros),
                                "conversions": round(float(row.metrics.conversions or 0), 2),
                                "ctr": self.format_percentage(row.metrics.ctr),
                                "average_cpc": self.format_currency(row.metrics.average_cpc),
                                "average_cpm": self.format_currency(row.metrics.average_cpm),
                                "conversion_rate": self.format_percentage(
                                    row.metrics.conversions_from_interactions_rate),
                                "cost_per_conversion": self.format_currency(row.metrics.cost_per_conversion),
                                "absolute_top_impression_percentage": self.format_percentage(
                                    row.metrics.absolute_top_impression_percentage),
                                "interactions": int(row.metrics.interactions)
                            }

                            # se houver search terms, anexa o principal
                            st_key = f"{row.customer.id}_{row.ad_group.id}_{date}_{kw}"
                            if st_key in search_term_map and search_term_map[st_key]:
                                best_term = max(search_term_map[st_key], key=lambda x: x["impressions"])
                                record["search_term"] = best_term["term"]

                            day_data.append(record)

            except Exception as e:
                logger.warning(f"   ❌ {customer_id}: erro na query de ad_groups/search ({e})")

        logger.info(f"✅ Total de keywords extraído em {date_formatted}: {len(day_data):,} linhas")
        return day_data

    # =============================================================================
    # 🌍 LOCATIONS EXTRACTION METHODS
    # =============================================================================

    def load_geo_mapping(self, customer_ids):
        """Carrega mapeamento de IDs geográficos para nomes"""
        if self.geo_mapping_loaded:
            return

        logger.info("🗺️ Carregando mapeamento de localização geográfica...")

        # Query para obter todos os geo targets disponíveis
        geo_query = """
                    SELECT geo_target_constant.id, \
                           geo_target_constant.name, \
                           geo_target_constant.resource_name, \
                           geo_target_constant.country_code, \
                           geo_target_constant.target_type, \
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

                logger.info(f"   ✅ {len(self.geo_mapping_cache):,} localizações carregadas")
                self.geo_mapping_loaded = True

            except Exception as e:
                logger.warning(f"   ⚠️ Erro ao carregar mapeamento geográfico: {e}")
                logger.warning("   └─ Continuando sem mapeamento (IDs serão mantidos)")

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

    def extract_locations_data(self, customer_ids, date):
        """Extrai dados de geolocalização para uma data específica"""
        date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
        logger.info(f"\n🌍 Extraindo dados de localização para {date_formatted}...")

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
                logger.info(f"   └─ Processando localização da conta: {customer_id}")

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
                            geo_region_raw = self.get_geo_name(row.segments.geo_target_region) if hasattr(row.segments,
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
                                'ad_group_name': row.ad_group.name if hasattr(row, 'ad_group') and hasattr(row.ad_group,
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
                            logger.warning(f"   ⚠️ Erro ao processar linha: {e}")
                            continue

                if account_records > 0:
                    formatted_id = f"{customer_id[:3]}-{customer_id[3:6]}-{customer_id[6:]}"
                    logger.info(f"   📊 {formatted_id}: {account_records:,} registros de localização")

            except Exception as e:
                formatted_id = f"{customer_id[:3]}-{customer_id[3:6]}-{customer_id[6:]}"
                logger.warning(f"   ❌ {formatted_id}: Erro na extração de localização - {str(e)[:100]}...")

        logger.info(f"✅ Total de locations extraído: {len(location_data):,} registros de localização")
        return location_data

    # =============================================================================
    # 💾 SAVE METHODS
    # =============================================================================

    def save_to_gcs(self, data, date, data_type):
        """Salva arquivo no Google Cloud Storage"""
        if not data:
            logger.warning(f"⚠️ Nenhum dado de {data_type} para salvar no GCS")
            return None

        # Nome do arquivo baseado no tipo
        date_parts = date.split('-')
        if data_type == "keywords":
            filename = f"google_ads_keywords_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"
            blob_name = f"{self.args.GCS_FOLDER_KEYWORDS}/{filename}"
        else:  # locations
            filename = f"google_ads_locations_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"
            blob_name = f"{self.args.GCS_FOLDER_LOCATIONS}/{filename}"

        try:
            # Criar DataFrame e converter para CSV
            df = pd.DataFrame(data)
            csv_data = df.to_csv(index=False).encode('utf-8')

            # Upload para GCS
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(csv_data, content_type='text/csv')

            logger.info(f"☁️ Arquivo de {data_type} salvo no GCS: {filename}")
            logger.info(f"   📊 {len(df):,} linhas salvas")

            return blob_name

        except Exception as e:
            logger.error(f"❌ Erro ao salvar {data_type} no GCS: {e}")
            return None

    def save_local_backup(self, data, date, data_type):
        """Salva arquivo local como backup"""
        if not data:
            return None

        # Criar pasta se não existir
        os.makedirs(self.args.OUTPUT_FOLDER, exist_ok=True)

        # Nome do arquivo baseado no tipo
        date_parts = date.split('-')
        if data_type == "keywords":
            filename = f"google_ads_keywords_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"
        else:  # locations
            filename = f"google_ads_locations_{date_parts[2]}_{date_parts[1]}_{date_parts[0]}.csv"

        filepath = os.path.join(self.args.OUTPUT_FOLDER, filename)

        # Salvar CSV
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False, encoding='utf-8')

        logger.info(f"💾 Backup local de {data_type} salvo: {filename}")

        return filepath


def main():
    logger.info("🚀 EXTRATOR DE DADOS GOOGLE ADS - UNIFICADO (KEYWORDS + LOCATIONS)")
    logger.info("=" * 70)
    logger.info("📊 Gerando arquivos CSV por dia com dados de palavras-chave e localização")

    # Obter argumentos
    args = get_arguments()

    # Definir END_DATE se não fornecido
    if not hasattr(args, 'END_DATE') or not args.END_DATE:
        args.END_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Verificar quais extrações executar
    extract_keywords = args.EXTRACT_KEYWORDS.lower() == "true"
    extract_locations = args.EXTRACT_LOCATIONS.lower() == "true"

    if not extract_keywords and not extract_locations:
        logger.error("❌ Nenhuma extração habilitada! Configure EXTRACT_KEYWORDS ou EXTRACT_LOCATIONS como 'true'")
        return 1

    # Mostrar configuração atual
    logger.info(f"\n🔧 Configuração:")
    logger.info(f"   🏢 Conta MCC: {args.LOGIN_CUSTOMER_ID}")
    logger.info(f"   📊 Subcontas: {len(args.SUBACCOUNT_IDS.split(','))} configuradas")
    logger.info(f"   📅 Período: {args.START_DATE} até {args.END_DATE}")
    logger.info(f"   ☁️ Bucket GCS: {args.BUCKET_NAME}")
    logger.info(f"   🔍 Keywords: {'✅ Ativado' if extract_keywords else '❌ Desativado'}")
    logger.info(f"   🌍 Locations: {'✅ Ativado' if extract_locations else '❌ Desativado'}")

    # Inicializar extrator
    try:
        extractor = GoogleAdsExtractor(args)
    except Exception as e:
        logger.error(f"❌ Falha na inicialização: {e}")
        return 1

    # Validar acesso
    valid_accounts = extractor.validate_access()
    if not valid_accounts:
        logger.error("❌ Sem acesso às contas. Verifique as credenciais.")
        return 1

    # Resumo dos tipos de extração
    extraction_summary = {
        'keywords': {'enabled': extract_keywords, 'files_created': [], 'total_records': 0},
        'locations': {'enabled': extract_locations, 'files_created': [], 'total_records': 0}
    }

    # Processar Keywords
    if extract_keywords:
        logger.info(f"\n{'=' * 70}")
        logger.info("🔍 PROCESSANDO KEYWORDS...")
        logger.info(f"{'=' * 70}")

        # Verificar datas pendentes para keywords
        dates_to_process_keywords = extractor.get_dates_to_process(args.START_DATE, args.END_DATE, "keywords")

        if not dates_to_process_keywords:
            logger.info("\n✅ Todas as datas de keywords já foram processadas!")
        else:
            logger.info(f"\n🔄 Extraindo keywords para {len(dates_to_process_keywords)} datas...")

            for i, date in enumerate(dates_to_process_keywords, 1):
                logger.info(f"\n[KEYWORDS {i}/{len(dates_to_process_keywords)}] Processando {date}...")

                try:
                    keywords_data = extractor.extract_keywords_data(valid_accounts, date)
                    if keywords_data:
                        # Salvar no GCS (principal)
                        gcs_path = extractor.save_to_gcs(keywords_data, date, "keywords")
                        if gcs_path:
                            extraction_summary['keywords']['files_created'].append(gcs_path)

                        # Salvar backup local
                        local_path = extractor.save_local_backup(keywords_data, date, "keywords")

                        extraction_summary['keywords']['total_records'] += len(keywords_data)
                    else:
                        date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
                        logger.warning(f"   ⚠️ {date_formatted}: Sem dados de keywords encontrados")

                except Exception as e:
                    date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
                    logger.error(f"   ❌ {date_formatted}: Erro na extração de keywords - {e}")

    # Processar Locations
    if extract_locations:
        logger.info(f"\n{'=' * 70}")
        logger.info("🌍 PROCESSANDO LOCATIONS...")
        logger.info(f"{'=' * 70}")

        # Verificar datas pendentes para locations
        dates_to_process_locations = extractor.get_dates_to_process(args.START_DATE, args.END_DATE, "locations")

        if not dates_to_process_locations:
            logger.info("\n✅ Todas as datas de locations já foram processadas!")
        else:
            logger.info(f"\n🔄 Extraindo locations para {len(dates_to_process_locations)} datas...")

            for i, date in enumerate(dates_to_process_locations, 1):
                logger.info(f"\n[LOCATIONS {i}/{len(dates_to_process_locations)}] Processando {date}...")

                try:
                    locations_data = extractor.extract_locations_data(valid_accounts, date)
                    if locations_data:
                        # Salvar no GCS (principal)
                        gcs_path = extractor.save_to_gcs(locations_data, date, "locations")
                        if gcs_path:
                            extraction_summary['locations']['files_created'].append(gcs_path)

                        # Salvar backup local
                        local_path = extractor.save_local_backup(locations_data, date, "locations")

                        extraction_summary['locations']['total_records'] += len(locations_data)
                    else:
                        date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
                        logger.warning(f"   ⚠️ {date_formatted}: Sem dados de locations encontrados")

                except Exception as e:
                    date_formatted = datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m/%Y')
                    logger.error(f"   ❌ {date_formatted}: Erro na extração de locations - {e}")

    # Resumo final consolidado
    logger.info(f"\n{'=' * 70}")
    logger.info("🎉 EXTRAÇÃO UNIFICADA CONCLUÍDA!")
    logger.info(f"{'=' * 70}")

    total_files = 0
    total_records = 0

    for data_type, summary in extraction_summary.items():
        if summary['enabled']:
            type_name = "Keywords" if data_type == "keywords" else "Locations"
            logger.info(f"📊 {type_name}:")
            logger.info(f"   ☁️ Arquivos salvos no GCS: {len(summary['files_created'])}")
            logger.info(f"   📈 Total de registros: {summary['total_records']:,}")

            if summary['files_created']:
                folder = args.GCS_FOLDER_KEYWORDS if data_type == "keywords" else args.GCS_FOLDER_LOCATIONS
                logger.info(f"   📂 Pasta GCS: {args.BUCKET_NAME}/{folder}/")

                # Mostrar últimos arquivos
                for gcs_path in summary['files_created'][-2:]:  # Últimos 2
                    filename = gcs_path.split('/')[-1]
                    logger.info(f"      └─ {filename}")
                if len(summary['files_created']) > 2:
                    logger.info(f"      ... e mais {len(summary['files_created']) - 2} arquivos")

            total_files += len(summary['files_created'])
            total_records += summary['total_records']

    logger.info(f"\n📋 Resumo geral:")
    logger.info(f"   📁 Total de arquivos: {total_files}")
    logger.info(f"   📊 Total de registros: {total_records:,}")
    logger.info(f"   📂 Pasta local: {args.OUTPUT_FOLDER}/")

    logger.info(f"\n✅ Pronto! Dados salvos no Google Cloud Storage")
    logger.info(f"   └─ Bucket: gs://{args.BUCKET_NAME}/")

    return 0


if __name__ == "__main__":
    exit(main())
