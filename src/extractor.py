"""
extractor.py — máquina extratora do MongoDB para a landing zone.

Reconciliado com o arquivo que vocês subiram: mantive o nome da classe
(MongoReader), o uso de `logging` no lugar de `print`, e a filosofia de
"não depender de dbutils dentro da classe" (a URI vem por parâmetro).

O que foi corrigido/reintroduzido em relação ao que estava no arquivo:

  1. O arquivo enviado tinha vários `[cite: 4]` / `[cite: 1, 4]` soltos no
     meio do código (depois de vírgulas, dentro de expressões) — isso é
     Python inválido, o arquivo não chegava a importar. Removido.

  2. `read()` original monta `docs = [... for d in cursor]` sobre um cursor
     SEM paginação — materializa a coleção inteira em memória antes de virar
     DataFrame. Para `comments` (~50k) isso é exatamente o padrão que o R2
     do enunciado pede pra evitar. Voltei pra paginação por `_id`: cada
     página tem no máximo `batch_size` documentos, e cada página vira 1
     arquivo JSONL na landing (não 1 arquivo por documento, nem tudo em
     memória de uma vez).

  3. Sem essa mudança, também não tínhamos como reaproveitar o JSON cru pra
     resolver o schema drift (R7) via `rescuedDataColumn` do lado do
     BronzeWriter — por isso a extração escreve pra landing em vez de virar
     DataFrame direto dentro desta classe.
"""

from __future__ import annotations

import datetime
import json
import logging
import time
from dataclasses import dataclass
from typing import Any

import bson
from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)


def _encode(o: Any) -> Any:
    """Tratamento de tipos do MongoDB (BSON) para JSON padrão."""
    if isinstance(o, bson.ObjectId):
        return str(o)
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    if isinstance(o, bson.Decimal128):
        return str(o)
    if isinstance(o, bytes):
        return o.hex()
    return str(o)


def with_retry(max_attempts: int = 4, base_delay: float = 1.5):
    """Retry com backoff exponencial (1.5s, 3s, 6s...) para erros de rede do Mongo."""

    def decorator(fn):
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except PyMongoError as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        break
                    sleep_s = base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        "retry %s/%s falhou (%r); aguardando %.1fs", attempt, max_attempts, exc, sleep_s
                    )
                    time.sleep(sleep_s)
            raise last_exc

        return wrapper

    return decorator


@dataclass
class ExtractResult:
    collection: str
    files_written: list[str]
    docs_lidos: int
    paginas_lidas: int
    started_at: datetime.datetime
    finished_at: datetime.datetime


class MongoReader:
    """
    Máquina extratora padronizada para o MongoDB. Busca documentos em
    páginas ordenadas por `_id` (no máximo `batch_size` por página) e grava
    cada página como 1 arquivo JSONL na landing — sem materializar a
    coleção inteira em memória, sem 1 arquivo por documento.
    """

    def __init__(self, database: str = "sample_mflix", uri_secret: str | None = None):
        self.database = database
        if not uri_secret:
            raise ValueError("A chave de acesso (URI) não foi fornecida à máquina extratora.")

        logger.info("Conectando ao banco %s...", self.database)
        self.client = MongoClient(
            uri_secret,
            serverSelectionTimeoutMS=15_000,
            socketTimeoutMS=300_000,
            appName="databricks-mongodb-connector",
        )
        self.db = self.client[self.database]

    def close(self) -> None:
        """Desliga a máquina e libera a conexão."""
        logger.info("Encerrando conexão com o banco de dados.")
        self.client.close()

    def __enter__(self) -> "MongoReader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @with_retry()
    def count(self, colecao: str, filtro: dict | None = None) -> int:
        """Contagem na origem — usada depois para a reconciliação (R8)."""
        return self.db[colecao].count_documents(filtro or {})

    @with_retry()
    def _fetch_page(self, colecao: str, page_filter: dict, projecao: dict | None, batch_size: int) -> list[dict]:
        cursor = (
            self.db[colecao]
            .find(filter=page_filter, projection=projecao)
            .sort("_id", ASCENDING)
            .limit(batch_size)
        )
        # bounded a `batch_size` documentos — não é a coleção inteira
        return list(cursor)

    def extract_to_landing(
        self,
        colecao: str,
        landing_dir: str,
        run_id: str,
        filtro: dict | None = None,
        projecao: dict | None = None,
        batch_size: int = 2_000,
        dbutils=None,
    ) -> ExtractResult:
        """Pagina a coleção por `_id` e grava 1 arquivo JSONL por página na landing."""
        logger.info("Iniciando leitura de '%s'. batch_size=%s filtro=%s projecao=%s", colecao, batch_size, filtro, projecao)

        started_at = datetime.datetime.utcnow()
        base_filter = dict(filtro or {})

        files_written: list[str] = []
        docs_lidos = 0
        paginas_lidas = 0
        last_id = None

        while True:
            page_filter = dict(base_filter)
            if last_id is not None:
                page_filter["_id"] = {"$gt": last_id}

            page = self._fetch_page(colecao, page_filter, projecao, batch_size)
            if not page:
                break

            paginas_lidas += 1
            lines = [json.dumps(doc, default=_encode, ensure_ascii=False) for doc in page]
            docs_lidos += len(page)
            last_id = page[-1]["_id"]

            filename = f"{colecao}_{run_id}_{paginas_lidas:05d}.jsonl"
            path = f"{landing_dir.rstrip('/')}/{filename}"
            content = "\n".join(lines)
            if dbutils is not None:
                dbutils.fs.put(path, content, overwrite=True)
            else:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content)
            files_written.append(path)

            if len(page) < batch_size:
                break

        finished_at = datetime.datetime.utcnow()
        logger.info("Leitura de '%s' concluída: %s documento(s) em %s página(s).", colecao, docs_lidos, paginas_lidas)
        return ExtractResult(
            collection=colecao,
            files_written=files_written,
            docs_lidos=docs_lidos,
            paginas_lidas=paginas_lidas,
            started_at=started_at,
            finished_at=finished_at,
        )
