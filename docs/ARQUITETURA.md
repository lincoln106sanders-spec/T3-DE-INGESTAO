# Arquitetura — Diagrama

> Decisões técnicas, justificativas e detalhamento de cada camada estão no
> [`README.md`](../README.md) na raiz do repositório. Este arquivo contém
> só o diagrama da solução.

```mermaid
flowchart LR
    subgraph ORIGEM
        M[("MongoDB - VM do professor
        sample_mflix")]
    end

    subgraph DATABRICKS["Databricks - Unity Catalog"]
        direction TB
        E["extractor.py
        MongoReader
        paginacao por _id, batch_size=2000
        projection: password / jwt / plot_embedding"]
        L["Landing Volume
        meu_catalog.landing.mongo
        1 JSONL por pagina"]
        BW["ingestion_job.py
        BronzeWriter
        rescuedDataColumn
        MERGE insert-only por source_id"]
        B[("Bronze
        meu_catalog.bronze - uma tabela por colecao
        Delta, append-only")]
        C[("control_ingestion_log
        watermark e metricas R8")]
    end

    M -->|find paginado com filtro incremental| E --> L --> BW --> B
    BW -->|1 linha por execucao| C
    C -.->|le ultimo watermark_final antes de cada carga incremental| E
```
