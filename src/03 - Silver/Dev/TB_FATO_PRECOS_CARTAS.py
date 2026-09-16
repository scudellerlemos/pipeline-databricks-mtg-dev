# Databricks notebook source
# =============================================================================
# CAMADA SILVER - PRECOS DE CARTAS - MAGIC: THE GATHERING
# =============================================================================
"""
Script Python para processamento da tabela TB_FATO_PRECOS_CARTAS.
Transformacao e limpeza de dados da Bronze para Silver.

CLASSIFICACAO DAMA-DMBOK: Fato - uma linha por coleta de preco de uma carta
(grao), com medidas quantitativas (VLR_USD/VLR_EUR/VLR_TIX). Fato
independente de TB_FATO_CARTAS - quem precisar combinar carta com preco faz
o join na Gold por NME_CARTA (ver docstring de TB_FATO_CARTAS).

GRAO PROPRIO: cards (Bronze cards) e precos (Bronze card_prices) vem de
fontes diferentes, com grao diferente - cards e por IMPRESSAO (ID_CARTA),
preco e por NOME (a Scryfall so responde preco por
/cards/named?exact=<name>, sem granularidade de impressao). Cada Fato fica
no seu proprio grao natural em vez de forcar ID_CARTA a carregar
DT_INGESTAO_PRECO na chave.

CHAVE UNICA: NME_CARTA + DT_INGESTAO (ver save_silver_table no fim do
notebook). A Bronze card_prices guarda so o preco mais recente por nome
(merge upsert por nome, sem historico proprio) - mas como esse "mais
recente" muda de data a cada execucao, cada run acrescenta uma nova linha
na Silver em vez de sobrescrever, e e assim que o historico diario de preco
se acumula aqui.

CONVENCAO DE NOME/CASE DE COLUNA: mesma de TB_FATO_CARTAS
(ver docstring de la) - nome de coluna 100% MAIUSCULO, valor de atributo em
Title_Case por palavra sem acento (normalizar_valor() em silver_utils.py),
exceto COD_/ID_/URL_* e texto livre longo.
"""

# =============================================================================
# BIBLIOTECAS UTILIZADAS
# =============================================================================
import logging

# =============================================================================
# CARREGAMENTO DO MODULO UTILITARIO
# =============================================================================
# Importar infraestrutura comum e funcoes do silver_utils usando %run (Databricks)

# COMMAND ----------

# MAGIC %run "../../00 - Common/Dev/base_utils"

# COMMAND ----------

# MAGIC %run ./silver_utils

# COMMAND ----------

# MAGIC %run ./silver_column_docs

# COMMAND ----------

# =============================================================================
# CONFIGURACAO INICIAL
# =============================================================================
def setup_logging():
    """Configura logging para o script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def transform_card_prices_silver(df):
    """
    Transformacao especifica para tabela Precos de Cartas, via SQL
    (spark.sql sobre temp views).
    """
    if not df:
        return None

    logger = logging.getLogger(__name__)
    logger.info("Iniciando transformacoes especificas para Precos de Cartas...")

    df.createOrReplaceTempView("_prices_bronze")

    # Renomeia Bronze -> PT-BR, upper() no codigo de colecao (join-key com
    # TB_FATO_CARTAS), cast de tipo nas colunas de preco (vem como string da
    # Bronze). ANO_INGESTAO/MES_INGESTAO vem da data da coleta, nao da de
    # lancamento da colecao. Sem coalesce para 0.0 nas colunas de preco: NULO
    # significa "sem cotacao encontrada", nao "vale zero".
    df_final = spark.sql("""
        SELECT
            name AS NME_CARTA,
            upper(`set`) AS COD_COLECAO,
            rarity AS NME_RARIDADE,
            cast(usd AS float) AS VLR_USD,
            cast(eur AS float) AS VLR_EUR,
            cast(tix AS float) AS VLR_TIX,
            scryfall_uri AS URL_SCRYFALL,
            image_url AS URL_IMAGEM,
            to_date(releaseDate) AS DT_LANCAMENTO,
            to_timestamp(ingestion_timestamp) AS DT_INGESTAO,
            coalesce(nullif(trim(source), ''), 'NA') AS NME_FONTE,
            endpoint AS DESC_URL_ORIGEM,
            source_file AS DESC_ARQUIVO_ORIGEM,
            bronze_run_id AS ID_EXECUCAO_BRONZE,
            bronze_ingestion_timestamp AS DT_INGESTAO_BRONZE,
            year(to_timestamp(ingestion_timestamp)) AS ANO_INGESTAO,
            month(to_timestamp(ingestion_timestamp)) AS MES_INGESTAO
        FROM _prices_bronze
    """)

    df_final = normalizar_valores(df_final, ["NME_CARTA", "NME_RARIDADE", "NME_FONTE"])

    logger.info(f"Transformacao Precos de Cartas concluida: {df_final.count()} registros")
    return df_final

# =============================================================================
# CONFIGURACAO
# =============================================================================

# Configuracao manual. catalog_name vem do mesmo secret que a Bronze usa
# (get_secret("catalog_name")).
config = create_manual_config(get_secret("catalog_name"), get_secret("s3_bucket"))

# Setup Unity Catalog
setup_unity_catalog(config['catalog_name'], config['schema_silver'])

# COMMAND ----------

# =============================================================================
# PROCESSAMENTO USANDO SILVER_UTILS
# =============================================================================
# Criar processor
processor = SilverTableProcessor("TB_FATO_PRECOS_CARTAS", config)

# Extracao da Bronze (nome real da tabela no catalog, minusculo)
df_bronze = processor.extract_from_bronze("card_prices")

# Aplicar transformacao especifica
df_silver = processor.transform_data(df_bronze, transform_card_prices_silver)

# Salvar na Silver com merge incremental por NME_CARTA + DT_INGESTAO (ver
# docstring da celula anterior - historico diario de preco).
# Sem order_by_col: DT_INGESTAO ja esta na propria key_column, entao dentro
# de uma particao do dedup ela e constante - usa-la como criterio de recencia
# nao desempata nada (zero variancia). Duplicatas reais de (NME_CARTA,
# DT_INGESTAO) sao indistinguiveis aqui (mesma carta, mesma coleta exata) -
# dropDuplicates padrao resolve sem custo extra de Window/hash.
processor.save_silver_table(
    df_silver,
    partition_cols=["ANO_INGESTAO", "MES_INGESTAO"],
    key_column=["NME_CARTA", "DT_INGESTAO"],
    table_comment=get_table_comment("TB_FATO_PRECOS_CARTAS"),
    column_comments=get_column_comments("TB_FATO_PRECOS_CARTAS")
)

# =============================================================================
# VALIDACAO E LOGS
# =============================================================================
if df_silver:
    print(f"Processamento concluido com sucesso!")
    print(f"Registros processados: {df_silver.count()}")
    print(f"Colunas finais: {df_silver.columns}")
else:
    print("Falha no processamento - DataFrame vazio")
