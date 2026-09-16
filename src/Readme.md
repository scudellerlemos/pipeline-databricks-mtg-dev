# 🏗️ Data Lake - Magic: The Gathering

<div align="center">

![Jace, the Mind Sculptor](https://repositorio.sbrauble.com/arquivos/in/magic/480738/68250850dd567-7s4u3-8loi0-bd6caeb231d828e67d6e2c1b6abc7239.jpg)

*"Como um mágico negro que busca entender sua própria existência, cada camada do Data Lake é um passo na jornada de transformação, onde dados brutos ganham consciência e se tornam insights de poder inestimável."* - Vivi Ornitier, Final Fantasy IX - Magic The Gathering

</div>

## 📋 Visão Geral do Data Lake

Este repositório contém o **pipeline completo de dados** do Magic: The Gathering, implementado como um Data Lake moderno no Databricks. O pipeline segue a arquitetura **Medallion** com três camadas principais: **Bronze** (dados brutos), **Silver** (dados limpos) e **Gold** (análises executivas).

### 🎯 **Objetivo Principal**

Transformar dados brutos da API do Magic: The Gathering em insights estratégicos e análises executivas, seguindo as melhores práticas de Data Engineering:

- **Extract & Load** (Bronze) - Carregamento de dados brutos
- **Transform & Load** (Silver) - Limpeza e enriquecimento
- **Analyze & Load** (Gold) - Análises executivas e métricas

## 🏛️ Arquitetura do Data Lake

```
┌─────────────────────────────────────────────────────────────┐
│                    🎮 MAGIC: THE GATHERING                  │
│                            DATA LAKE                        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   🥉 BRONZE     │    │   🥈 SILVER    │    │   🥇 GOLD       │
│                 │    │                 │    │                 │
│ • Extract       │───▶│ • Transform     │───▶│ • Analyze       │
│ • Load          │    │ • Load          │    │ • Load          │
│ • Raw Data      │    │ • Clean Data    │    │ • Insights      │
│ • Staging       │    │ • Enriched      │    │ • Metrics       │
│ • Delta Lake    │    │ • Delta Lake    │    │ • Delta Lake    │
└─────────────────┘    └─────────────────┘    └─────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    🏛️ UNITY CATALOG                         │
│              mtg_dev.{bronze|silver|gold}                    │
└─────────────────────────────────────────────────────────────┘
```

## 📁 Estrutura das Camadas

### 🥉 **Camada Bronze** - Dados Brutos
**Localização**: `src/01 - Ingestion/` → `src/02 - Bronze/`

**Processo**: **EL (Extract & Load)**
- **Extract**: Leitura de dados Parquet da staging (S3)
- **Load**: Append-only no Unity Catalog (sem MERGE/upsert), idempotente por `source_file`
- **Dados**: 6 tabelas, uma por origem da Stage (`cards`, `sets`, `card_prices`, `symbology`, `rulings`, `migrations`)

**Características**:
- ✅ Dados brutos preservados 1:1 (schema de origem, sem renomeação)
- ✅ Append-only, sem dedup por chave de negócio - histórico completo preservado
- ✅ Sem particionamento (volume atual não justifica)
- ✅ Governança via Unity Catalog (tabela e coluna comentadas - ver [`Documentação/`](<02 - Bronze/Documentação/README.md>))
- ✅ Histórico completo via Delta Lake

**Tabelas**: `cards`, `sets`, `card_prices`, `symbology`, `rulings`, `migrations` -
sem prefixo `TB_BRONZE_`, já que vivem no schema `bronze` do Unity Catalog.

### 🥈 **Camada Silver** - Dados Limpos
**Localização**: `src/03 - Silver/`

**Processo**: **TL (Transform & Load)**
- **Transform**: Limpeza, padronização e enriquecimento via SQL (`spark.sql()` sobre temp views)
- **Load**: Carregamento incremental com dados refinados
- **Dados**: 3 tabelas enriquecidas e padronizadas

**Características**:
- ✅ Dados limpos e padronizados
- ✅ Enriquecimento com categorias e métricas
- ✅ Nomenclatura consistente (NME_, COD_, DESC_)
- ✅ Particionamento otimizado
- ✅ Qualidade de dados garantida
- ✅ Transformações em SQL puro, sem UDFs Python

**Tabelas**:
- 🃏 **TB_FATO_SILVER_CARDS** - Cartas enriquecidas
- 📦 **TB_REF_SILVER_SETS** - Expansões com metadados
- 💰 **TB_FATO_SILVER_CARDPRICES** - Preços processados

### 🥇 **Camada Gold** - Análises Executivas
**Localização**: `src/04 - Gold/`

**Processo**: **AL (Analyze & Load)**
- **Analyze**: Análises pré-computadas e métricas de negócio via SQL (`spark.sql()` sobre temp views)
- **Load**: Carregamento incremental de insights estratégicos
- **Dados**: 3 tabelas de análise executiva

**Características**:
- ✅ Análises pré-computadas
- ✅ Métricas de negócio e KPIs
- ✅ Insights estratégicos
- ✅ Categorizações automáticas
- ✅ Prontidão executiva
- ✅ Transformações em SQL puro, sem UDFs Python

**Tabelas**:
- 📊 **TB_ANALISE_MERCADO_CARTAS_EXECUTIVO** - Análise executiva de mercado
- 📈 **TB_METRICAS_PERFORMANCE_INVESTIMENTOS** - KPIs de performance
- 🚨 **TB_REPORT_ALERTAS_EXECUTIVOS** - Sistema de alertas

## 🔄 Fluxo de Dados Completo

### **1. Ingestão / Stage (01 - Ingestion)**
```python
# Controle de execução: início do run
run_id = start_run(base_path, "cards", endpoint, params)

# Extração da Scryfall com retry/backoff em 429/5xx
data = http_get_with_retry(url, headers, timeout, retries)

# Salvamento em Parquet no Stage (snapshot datado, idempotente)
save_to_parquet(data, f"{base_path}/cards/{year}_{month}_{day}_cards.parquet")

# Controle de execução: fim do run (SUCCESS/FAILED/PARTIAL)
finish_run(base_path, "cards", run_id, status="SUCCESS", ...)
```

### **2. Bronze (02 - Bronze)**
```python
# EL puro: lê só os arquivos novos da Stage e faz append no Delta,
# sem regra de negócio - ver Dev/bronze_utils.py
run_bronze_ingestion(
    spark, dbutils, catalog_name, schema_name="bronze",
    bronze_table_name="cards", stage_table_name="cards",
    s3_stage_path=..., s3_bronze_path=...,
    table_comment=get_table_comment("cards"),
    column_comments=get_column_comments("cards"),
)
```

### **3. Silver (03 - Silver)**
```python
# Transformação e enriquecimento
df_silver = transform_bronze_data(df_bronze)
# Carregamento incremental na Silver
load_to_silver_unity_incremental(df_silver, "TB_FATO_SILVER_CARDS")
```

### **4. Gold (04 - Gold)**
```python
# Análises executivas
df_gold = create_executive_analysis(df_silver)
# Carregamento incremental na Gold
load_to_gold_unity_incremental(df_gold, "TB_ANALISE_MERCADO_CARTAS_EXECUTIVO")
```

## 🛠️ Tecnologias Utilizadas

### **Plataforma Principal**
- **Databricks** - Plataforma unificada de analytics
- **Unity Catalog** - Governança de dados
- **Delta Lake** - Storage layer ACID
- **Apache Spark** - Processamento distribuído

### **Linguagens e APIs**
- **SQL** - Regras de negócio das camadas Silver e Gold (`spark.sql()` sobre temp views, sem UDFs Python)
- **Python** - Orquestração (extract/load/save/config)
- **PySpark** - Leitura/escrita de dados e integração com Delta Lake

### **Infraestrutura**
- **AWS S3** - Storage de staging
- **Databricks Secrets** - Gerenciamento de credenciais
- **Databricks Clusters** - Computação escalável

## 📊 Métricas e KPIs do Pipeline

### **Performance**
- **Ingestão**: 100 páginas por execução (demonstração)
- **Processamento**: Incremental por chaves específicas
- **Tempo de Execução**: <50 minutos para pipeline completo

### **Qualidade**
- **Bronze**: Preservação de dados originais
- **Silver**: Dados limpos e válidos
- **Gold**: Análises com métricas validadas

## 🎯 Casos de Uso

### **Análises de Mercado**
- Valorização de cartas por set e raridade
- Análise de tendências temporais
- Identificação de oportunidades de investimento

### **Análises de Jogo**
- Performance de cartas por formato
- Análise de metagame e tendências
- Estatísticas de uso e popularidade

### **Análises Executivas**
- KPIs de performance de investimentos
- Alertas de oportunidades e riscos
- Relatórios estratégicos para tomada de decisão

## 🔧 Configuração e Execução

### **Pré-requisitos**
- Databricks Workspace configurado
- Unity Catalog habilitado
- Cluster Spark disponível
- Segredos configurados no scope `mtg-pipeline`

### **Segredos Necessários**
```python
catalog_name           # Nome do catálogo Unity
s3_bucket             # Bucket S3 para staging
s3_bronze_prefix      # Prefixo da camada bronze
s3_silver_prefix      # Prefixo da camada silver
s3_gold_prefix        # Prefixo da camada gold
```

### **Ordem de Execução**
1. **Ingestão**: `src/01 - Ingestion/` (extração da API)
2. **Bronze**: `src/02 - Bronze/` (carregamento de dados brutos)
3. **Silver**: `src/03 - Silver/` (transformação e limpeza)
4. **Gold**: `src/04 - Gold/` (análises executivas)


## 🚀 Próximos Passos

### **Expansão Imediata**
- Implementação de todas as tabelas Silver restantes
- Criação de Data Warehouse completo (Star Schema)
- Análises por formato de jogo (Standard, Modern, Commander)

### **Melhorias Futuras**
- Análises de sentimento de cartas
- Integração com dados de torneios
- Dashboard executivo em tempo real

### **Otimizações**
- Particionamento avançado por múltiplas dimensões
- Cache inteligente para consultas frequentes
- Otimização de queries com Z-Order
- Monitoramento avançado de performance

## 🎴 Flavor Text do Data Lake

*"Como um multiverso de dados que se expande infinitamente, este Data Lake transforma a magia bruta da informação em insights estratégicos de poder inestimável. Cada camada é um plano de existência, cada tabela uma criatura mágica, cada análise um feitiço de poder executivo."*

---

## 📞 Suporte e Contato

Para dúvidas, sugestões ou problemas:
- Verificar documentação específica de cada camada
- Consultar logs de execução no Databricks
- Revisar configurações de segredos e permissões
- Verificar status do Unity Catalog e Delta Lake

**🎮 Que a magia dos dados esteja sempre com você!** 
