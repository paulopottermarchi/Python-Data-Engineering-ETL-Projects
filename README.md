# Pipeline ETL – PIB Mundial por País

## Visão Geral

Este projeto implementa um **pipeline ETL (Extract, Transform, Load)** que coleta dados econômicos sobre o Produto Interno Bruto (PIB) de países ao redor do mundo.

O pipeline realiza a extração dos dados por **web scraping** a partir de uma página arquivada da Wikipédia, realiza o tratamento e transformação dos valores do PIB e armazena os dados processados em um **arquivo CSV** e em um **banco de dados SQLite** para futuras análises.

Este projeto demonstra conceitos fundamentais de **engenharia de dados**, incluindo coleta de dados, limpeza, transformação e armazenamento em banco relacional.

---

## Arquitetura do Pipeline

```text id="r8kp4k"
Página Web Arquivada
        │
        ▼
Extração (BeautifulSoup)
        │
        ▼
Transformação (Limpeza e Conversão de Dados)
        │
        ▼
Carga
 ├── Arquivo CSV
 └── Banco de Dados SQLite
```

---

## Tecnologias Utilizadas

* Python
* Pandas
* NumPy
* BeautifulSoup
* Requests
* SQLite
* Sistema de Logging

---

## Fonte dos Dados

Os dados são extraídos de uma versão arquivada da página da Wikipédia:

**Lista de países por PIB nominal**

Os seguintes atributos são coletados:

| Coluna           | Descrição                 |
| ---------------- | ------------------------- |
| Country          | Nome do país              |
| GDP_USD_millions | PIB em milhões de dólares |

---

## Transformação dos Dados

Durante a etapa de transformação o pipeline realiza:

1. Remoção de caracteres de formatação nos valores de PIB.
2. Conversão do PIB de **milhões para bilhões de dólares**.
3. Renomeação da coluna para refletir a nova unidade.

Estrutura final do dataset:

| Coluna           | Descrição                 |
| ---------------- | ------------------------- |
| Country          | Nome do país              |
| GDP_USD_billions | PIB em bilhões de dólares |

---

## Saídas Geradas

Após o processamento o pipeline gera dois tipos de saída.

### Dataset em CSV

```id="yrmwr7"
Countries_by_GDP.csv
```

### Banco de Dados SQLite

```id="q87ql3"
World_Economies.db
```

Tabela criada:

```id="4k04rk"
Countries_by_GDP
```

---

## Consulta de Exemplo

O pipeline executa uma consulta SQL para identificar países com PIB superior a **100 bilhões de dólares**.

```sql id="hpnrlp"
SELECT * FROM Countries_by_GDP
WHERE GDP_USD_billions >= 100
```

---

## Sistema de Logging

O progresso do pipeline é registrado no arquivo:

```id="5jnznk"
etl_project_log.txt
```

Exemplo de registro:

```id="c07f6e"
2026-Mar-13-21:41:10, Data extraction complete
```

---

## Estrutura do Projeto

```text id="t9x2id"
gdp-etl-pipeline
│
├── gdp_etl_pipeline.py
├── Countries_by_GDP.csv
├── World_Economies.db
├── etl_project_log.txt
└── README.md
```

---

## Objetivos de Aprendizado

Este projeto demonstra conceitos importantes de **engenharia de dados**:

* Web scraping de dados estruturados
* Construção de pipelines ETL
* Limpeza e transformação de dados
* Conversão de unidades e normalização
* Carga de dados em banco relacional
* Execução de consultas SQL
* Monitoramento de execução via logging

---

## Autor

**Paulo Potter Marchi**

Graduado em Ciência da Computação
Aspirante a Engenheiro de Dados
