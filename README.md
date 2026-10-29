# OpenAlex API – Large-Scale Bibliometric Data Collection in R

## Project Overview

This repository contains R scripts developed to collect and process large-scale bibliometric data through the [OpenAlex API](https://openalex.org/).

The project focuses on retrieving researcher profiles associated with selected institutions and extracting publication-level metadata for bibliometric analysis.

The scripts implement parallel API requests, batch processing, pagination, API rate-limit management, error handling, and incremental data storage to support large-scale data collection.

## Key Features

- **Parallel data collection:** Distributes API requests across multiple workers to improve download efficiency.
- **Batch processing:** Organizes institutions and publication identifiers into manageable groups.
- **API rate-limit management:** Monitors API usage and manages temporary request limitations.
- **Error handling:** Handles failed requests and supports retry mechanisms.
- **Checkpointing and recovery:** Records download progress to support resuming interrupted operations.
- **Data deduplication:** Avoids duplicate author records and supports unique publication identifiers.
- **Structured outputs:** Stores author data in RDS format and publication metadata in CSV format.

## Repository Contents

### 1. Author Data Collection

**Script:** `API_download_authors_parallel_rds.R`

Retrieves researcher profiles from the OpenAlex Authors API based on a predefined list of institutional identifiers.

The script:

- Reads institutional OpenAlex IDs from a CSV file.
- Groups institutions into batches.
- Retrieves associated researcher profiles using parallel API requests.
- Handles cursor-based pagination.
- Collects author identifiers, ORCID information, names, publication counts, citation counts, affiliations, research topics, and additional bibliometric indicators.
- Removes duplicate author records.
- Saves collected data in RDS format, preserving nested information.
- Maintains CSV checkpoint files for tracking completed and failed batches.

**Input:** `istituzioni_base.csv`

**Main output:** `autori_completo_authors_istituzioni_it.rds`

**Additional outputs:**
- `stato_download_authors_istituzioni_it.csv`
- `mappa_batch_istituzioni_authors_istituzioni_it.csv`

The repository may include multiple versions of the author-download script.

### 2. Publication Metadata Collection

**Script:** `API_download_works_solo_flat_csv_only.R`

Retrieves publication-level metadata from the OpenAlex Works API using a predefined list of OpenAlex Work IDs.

The script:

- Reads publication identifiers from a CSV file.
- Normalizes and deduplicates input identifiers.
- Groups requests into batches and processes them in parallel.
- Retrieves selected bibliometric metadata.
- Handles API limits, failed requests, and missing identifiers.
- Saves results in a structured CSV dataset.
- Maintains progress information for incremental processing.

**Input:** `id_works.csv`

**Main output:** `works_flat_works_base.csv`

The output contains the following variables:

| Variable | Description |
|---|---|
| `id_work` | OpenAlex publication identifier |
| `doi` | Digital Object Identifier |
| `title` | Publication title |
| `display_name` | Display name of the publication |
| `publication_date` | Publication date |
| `publication_year` | Publication year |
| `type` | Publication type |
| `fwci` | Field-Weighted Citation Impact |
| `cited_by_count` | Number of citations |
| `referenced_works_count` | Number of referenced works |
| `is_retracted` | Retraction indicator |

The script produces a flat dataset rather than a separate table of authorship relationships.

## Technologies and Libraries

**Programming language:** R

**Main libraries:**

- `httr` – HTTP requests and API communication
- `jsonlite` – JSON parsing
- `data.table` – Efficient data manipulation and CSV processing
- `parallel` – Parallel computation
- `askpass` – Secure interactive password input where required

**Data formats:** CSV, RDS, JSON

**Data source:** [OpenAlex](https://openalex.org/)

## Data Collection Workflow

The scripts support the following data collection process:

**Institution IDs → Author profiles**

A predefined list of institutions is used to retrieve researcher information from the OpenAlex Authors API.

**Work IDs → Publication metadata**

A predefined list of publication identifiers is used to retrieve bibliometric information from the OpenAlex Works API.

These processes require their respective input CSV files. The repository does not automatically generate publication IDs from the downloaded author profiles.

## Configuration and Usage

Before running the scripts:

1. Install the required R libraries.
2. Prepare the input CSV files containing valid OpenAlex identifiers.
3. Update input and output directory paths for the local environment.
4. Configure valid OpenAlex API credentials securely.
5. Adjust the number of parallel workers and batch sizes according to available computing resources and API limitations.
6. Review any environment-specific network settings.

Example installation:

```r
install.packages(c(
  "httr",
  "jsonlite",
  "data.table",
  "askpass"
))
```

The `parallel` package is included with standard R installations.

**Security note:** API credentials, passwords, and private network configurations should not be committed to a public repository. Environment variables or other secure configuration methods are recommended.

The scripts are provided as research-oriented implementations and may require configuration before execution in a different computing environment.

## Potential Applications

The collected data can support:

- Bibliometric and scientometric research
- Research productivity analysis
- Citation impact analysis
- Institutional research assessment
- Academic publication trend analysis
- Construction of large-scale research datasets

## Project Scope

This repository focuses on the **data acquisition and preparation stages** of bibliometric research.

It demonstrates practical experience in API integration, parallel programming, data engineering, large-scale data collection, and the management of structured and nested research data.

## Data Attribution

Bibliometric information is retrieved from [OpenAlex](https://openalex.org/), an open catalog of the global research system.

This repository is an independent project and is not officially affiliated with OpenAlex.
