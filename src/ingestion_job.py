"""
ingestion_job.py

Reconciliado com o arquivo que vocês subiram: mantive o estilo de logging,
`load_config`, `process_collection` e `main()` que já estavam esboçados —
mas implementei de verdade o que só existia como comentário
("# Aqui chamaremos...", "# Aqui implementaremos...").

Seções:
  1. IngestionControl — tabela de controle (R5) + watermark persistida (R3)
  2. BronzeWriter       — landing -> bronze: schema drift (R7), idempotência (R3/R6)
  3. process_collection / run_all / main — orquestração genérica (R1)
"""

import datetime
import json
import logging
import uuid
from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from extractor import MongoReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_WATERMARK_START = "1970-01-01T00:00:00"


# ============================================================
# 1. IngestionControl — tabela de controle (R5) + watermark (R3)
# ============================================================

CONTROL_SCHEMA_DDL = """
    _ingestion_id       STRING,
    collection          STRING,
    load_type           STRING,
    watermark_inicial   STRING,
    watermark_final     STRING,
    qtd_lida_origem     LONG,
    qtd_gravada_destino LONG,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    duracao_seg         DOUBLE,
    status              STRING,
    mensagem_erro       STRING
"""


@dataclass
class RunRecord:
    _ingestion_id: str
    collection: str
    load_type: str
    watermark_inicial: str | None
    watermark_final: str | None
    qtd_lida_origem: int
    qtd_gravada_destino: int
    start_time: datetime.datetime
    end_time: datetime.datetime
    duracao_seg: float
    status: str  # SUCCESS | FAILED | PARTIAL
    mensagem_erro: str | None


class IngestionControl:
    def __init__(self, spark: SparkSession, control_table: str):
        self.spark = spark
        self.control_table = control_table
        self._ensure_table()

    def _ensure_table(self) -> None:
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.control_table} ({CONTROL_SCHEMA_DDL}) USING DELTA")

    def get_last_watermark(self, collection: str) -> str | None:
        rows = (
            self.spark.table(self.control_table)
            .where(
                (F.col("collection") == collection)
                & (F.col("status") == "SUCCESS")
                & (F.col("load_type") == "incremental")
            )
            .orderBy(F.col("end_time").desc())
            .select("watermark_final")
            .limit(1)
            .collect()
        )
        return rows[0]["watermark_final"] if rows else None

    def log(self, record: RunRecord) -> None:
        # schema explícito (a mesma DDL usada pra criar a tabela) em vez de
        # deixar o Spark inferir os tipos a partir dos dados -- em cargas
        # full, watermark_inicial/watermark_final vêm None, e uma coluna
        # 100% nula sem schema explícito não tem como ter o tipo inferido
        # (CANNOT_DETERMINE_TYPE no Spark Connect / serverless).
        df = self.spark.createDataFrame([asdict(record)], schema=CONTROL_SCHEMA_DDL)
        df.write.format("delta").mode("append").saveAsTable(self.control_table)


# ============================================================
# 2. BronzeWriter — landing -> bronze
# ============================================================


@dataclass
class LoadResult:
    linhas_lidas_landing: int
    linhas_gravadas_bronze: int
    duplicados_no_lote: int
    pct_source_id_nulo: float


class BronzeWriter:
    def __init__(self, spark: SparkSession, catalog: str, bronze_schema: str):
        self.spark = spark
        self.catalog = catalog
        self.bronze_schema = bronze_schema
        # NOTA: nada de spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", ...)
        # aqui -- essa configuração global é bloqueada em compute Serverless
        # (CONFIG_NOT_AVAILABLE.SERVERLESS_DELTA_SCHEMA_AUTO_MERGE_ENABLED).
        # A evolução de schema é habilitada por escrita: "mergeSchema" no
        # _ensure_table (já funciona em serverless) e "MERGE WITH SCHEMA
        # EVOLUTION" via SQL no landing_to_bronze (o método Python
        # .withSchemaEvolution() do DeltaMergeBuilder TAMBÉM falha em
        # serverless pelo mesmo motivo -- só a sintaxe SQL funciona lá).

    def _target_table(self, collection: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{collection}"

    def _ensure_table(self, table: str, sample_df: DataFrame) -> None:
        if not self.spark.catalog.tableExists(table):
            (
                sample_df.limit(0)
                .write.format("delta")
                .option("mergeSchema", "true")
                .partitionBy("_ingestion_date")
                .saveAsTable(table)
            )

    def landing_to_bronze(self, landing_paths: list[str], collection: str, run_id: str, load_type: str) -> LoadResult:
        if not landing_paths:
            return LoadResult(0, 0, 0, 0.0)

        # R7 — campo fora do schema cai em _rescued_data, nunca é descartado
        raw = self.spark.read.option("rescuedDataColumn", "_rescued_data").json(landing_paths)

        now = datetime.datetime.utcnow()
        enriched = (
            raw.withColumn("_source_id", F.col("_id").cast("string"))
            .withColumn("_ingestion_id", F.lit(run_id))
            .withColumn("_ingestion_timestamp", F.lit(now.isoformat()).cast("timestamp"))
            .withColumn("_source_path", F.lit("mongodb_professor"))
            .withColumn("_load_type", F.lit(load_type))
            .withColumn("_ingestion_date", F.to_date(F.lit(now.isoformat())))
        )

        # ---- R8: checagens de qualidade ANTES de gravar ----
        total = enriched.count()
        nulos_id = enriched.where(F.col("_source_id").isNull()).count()
        pct_nulo = (nulos_id / total * 100) if total else 0.0
        duplicados = enriched.groupBy("_source_id").count().where("count > 1").count()

        target = self._target_table(collection)
        self._ensure_table(target, enriched)

        # R3/R6 — MERGE insert-only por _source_id: nunca UPDATE/DELETE em
        # linha existente (bronze continua append-only e fiel à origem);
        # só insere o que ainda não está lá (idempotente em reruns).
        # Sintaxe SQL "WITH SCHEMA EVOLUTION" em vez da API Python
        # (DeltaTable.merge(...).withSchemaEvolution()) porque a API Python
        # falha em compute Serverless (mesma limitação do spark.conf.set,
        # ver nota no __init__) -- a sintaxe SQL não tem essa restrição.
        temp_view = f"_src_{collection}_{run_id.replace('-', '_')}"
        enriched.createOrReplaceTempView(temp_view)
        self.spark.sql(
            f"""
            MERGE WITH SCHEMA EVOLUTION INTO {target} AS t
            USING {temp_view} AS s
            ON t._source_id = s._source_id
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        self.spark.catalog.dropTempView(temp_view)

        gravadas = self.spark.table(target).where(F.col("_ingestion_id") == run_id).count()

        return LoadResult(
            linhas_lidas_landing=total,
            linhas_gravadas_bronze=gravadas,
            duplicados_no_lote=duplicados,
            pct_source_id_nulo=round(pct_nulo, 4),
        )


# ============================================================
# 3. Orquestração (R1)
# ============================================================


def load_config(config_path: str) -> dict:
    """Lê as instruções de fabricação (JSON) para a nossa esteira."""
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            config = json.load(file)
            logger.info("Configuração carregada com sucesso: %s", config_path)
            return config
    except Exception as e:
        logger.error("Erro ao ler arquivo de configuração: %s", e)
        raise


def _build_incremental_filter(watermark_field: str, watermark_inicial: str, watermark_type: str | None) -> dict:
    """
    Monta o filtro $gt para carga incremental.

    O PyMongo não converte string pra BSON Date automaticamente num $gt — se
    o campo for Date real no Mongo, é preciso passar um `datetime.datetime`.
    Confirmado pro dataset de vocês: movies.lastupdated = string,
    comments.date = datetime.datetime (ver watermark_type no config).
    """
    valor: object = watermark_inicial
    if watermark_type == "date":
        valor = datetime.datetime.fromisoformat(watermark_inicial)
    return {watermark_field: {"$gt": valor}}


def process_collection(
    spark: SparkSession,
    dbutils,
    global_cfg: dict,
    coll_config: dict,
    control: IngestionControl,
) -> RunRecord:
    """Processa individualmente cada coleção de acordo com o modo estipulado."""
    collection_name = coll_config.get("name")
    mode = coll_config.get("mode")
    watermark_field = coll_config.get("watermark_field")
    watermark_type = coll_config.get("watermark_type")
    projecao = coll_config.get("projection")

    logger.info("--- Iniciando setup para coleção: %s | Modo: %s ---", collection_name, mode)

    run_id = str(uuid.uuid4())
    start_time = datetime.datetime.utcnow()
    uri = dbutils.secrets.get(scope=global_cfg["mongo_secret_scope"], key=global_cfg["mongo_secret_key"])
    landing_dir = (
        f"/Volumes/{global_cfg['catalog']}/{global_cfg['landing_schema']}/"
        f"{global_cfg['landing_volume']}/{collection_name}"
    )

    watermark_inicial = None
    filtro = None
    if mode == "incremental":
        watermark_inicial = control.get_last_watermark(collection_name) or DEFAULT_WATERMARK_START
        filtro = _build_incremental_filter(watermark_field, watermark_inicial, watermark_type)
        logger.info("Lendo watermark de '%s' baseado em %s > %s", collection_name, watermark_field, watermark_inicial)
    elif mode == "full":
        logger.info("Iniciando carga full de '%s'...", collection_name)
    else:
        logger.warning("Modo de carga desconhecido para %s: %s", collection_name, mode)

    status = "SUCCESS"
    mensagem_erro = None
    extract_result = None
    load_result = None
    watermark_final = watermark_inicial

    try:
        with MongoReader(database=global_cfg["database"], uri_secret=uri) as reader:
            extract_result = reader.extract_to_landing(
                colecao=collection_name,
                landing_dir=landing_dir,
                run_id=run_id,
                filtro=filtro,
                projecao=projecao,
                batch_size=global_cfg.get("batch_size", 2_000),
                dbutils=dbutils,
            )

        writer = BronzeWriter(spark, global_cfg["catalog"], global_cfg["bronze_schema"])
        load_result = writer.landing_to_bronze(
            landing_paths=extract_result.files_written,
            collection=collection_name,
            run_id=run_id,
            load_type=mode,
        )

        # ---- R8: reconciliação, threshold calibrado por criticidade ----
        # cada coleção define o próprio limiar no config; sem ele, cai no
        # default global
        threshold = coll_config.get(
            "reconciliation_threshold_pct",
            global_cfg.get("reconciliation_threshold_pct_default", 1.0),
        )
        divergencia_pct = (
            abs(extract_result.docs_lidos - load_result.linhas_lidas_landing) / extract_result.docs_lidos * 100
            if extract_result.docs_lidos
            else 0.0
        )
        if divergencia_pct > threshold or load_result.pct_source_id_nulo > 0 or load_result.duplicados_no_lote > 0:
            status = "PARTIAL"
            mensagem_erro = (
                f"divergencia={divergencia_pct:.2f}% (limiar={threshold}%) "
                f"nulos_id={load_result.pct_source_id_nulo}% "
                f"duplicados_no_lote={load_result.duplicados_no_lote}"
            )
            logger.warning("Coleção %s marcada como PARTIAL: %s", collection_name, mensagem_erro)

        if mode == "incremental" and watermark_field and extract_result.docs_lidos > 0:
            target = f"{global_cfg['catalog']}.{global_cfg['bronze_schema']}.{collection_name}"
            max_row = spark.table(target).where(f"_ingestion_id = '{run_id}'").agg({watermark_field: "max"}).first()
            watermark_final = str(max_row[0]) if max_row and max_row[0] is not None else watermark_inicial

    except Exception as e:
        status = "FAILED"
        mensagem_erro = str(e)[:1000]
        logger.error("Falha ao processar a coleção %s. Erro: %s", collection_name, e)

    end_time = datetime.datetime.utcnow()
    record = RunRecord(
        _ingestion_id=run_id,
        collection=collection_name,
        load_type=mode,
        watermark_inicial=watermark_inicial,
        watermark_final=watermark_final,
        qtd_lida_origem=extract_result.docs_lidos if extract_result else 0,
        qtd_gravada_destino=load_result.linhas_gravadas_bronze if load_result else 0,
        start_time=start_time,
        end_time=end_time,
        duracao_seg=(end_time - start_time).total_seconds(),
        status=status,
        mensagem_erro=mensagem_erro,
    )
    try:
        control.log(record)
    except Exception as log_exc:
        # não deixamos um problema de LOGGING derrubar as coleções
        # seguintes -- fica visível no console, mas a execução continua
        logger.error("Falha ao gravar log de controle para %s: %s", collection_name, log_exc)
    return record


def run_all(config_path: str, spark: SparkSession, dbutils) -> list[RunRecord]:
    """Ponto de entrada pensado pra rodar de dentro de um notebook (spark/dbutils
    já disponíveis no runtime)."""
    logger.info("Iniciando Esteira de Ingestão de Dados...")
    config = load_config(config_path)
    control = IngestionControl(spark, config["control_table"])

    resultados = []
    for coll in config.get("collections", []):
        record = process_collection(spark, dbutils, config, coll, control)
        logger.info(
            "status=%s lidos=%s gravados=%s dur=%.1fs",
            record.status,
            record.qtd_lida_origem,
            record.qtd_gravada_destino,
            record.duracao_seg,
        )
        resultados.append(record)
    return resultados


def main(config_path: str = "config/collections.json") -> None:
    """Ponto de entrada standalone (ex.: Databricks Job rodando este .py como
    task, fora do contexto de notebook). Constrói spark/dbutils na mão."""
    spark = SparkSession.builder.appName("MongoDB_Ingestion_Job").getOrCreate()
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    except ImportError:
        raise RuntimeError("dbutils só está disponível rodando num cluster Databricks.")

    run_all(config_path, spark, dbutils)


if __name__ == "__main__":
    main()
