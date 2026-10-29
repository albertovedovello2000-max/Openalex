# =============================================================================
# DOWNLOAD AUTHORS OPENALEX DA ISTITUZIONI
# PARALLELIZZATO CON LA STESSA LOGICA DI "API download works totali id.R"
# OUTPUT DATI IN RDS, STATO IN CSV
# =============================================================================

library(httr)
library(jsonlite)
library(data.table)
library(parallel)
library(askpass)

# =============================================================================
# 1. CONFIGURAZIONE GENERALE
# =============================================================================

input_dir_istituzioni <- "/home/group/main/openalex/Openalex API/API Institutions"
file_istituzioni <- "istituzioni_base.csv"

path_istituzioni <- file.path(
  input_dir_istituzioni,
  file_istituzioni
)

codice_lavoro <- "authors_istituzioni_it"

output_dir <- "/home/group/main/openalex/Openalex API/API Authors/authors istituzioni it"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

path_output_rds <- file.path(
  output_dir,
  paste0("autori_completo_", codice_lavoro, ".rds")
)

# Lo stato resta in CSV perché deve essere leggero, ispezionabile e facile da correggere.
path_stato_csv <- file.path(
  output_dir,
  paste0("stato_download_", codice_lavoro, ".csv")
)

# Mappa stabile batch-istituzioni.
# Serve a garantire che, in caso di ripartenza, il batch 17 continui
# a indicare lo stesso insieme di istituzioni del primo run.
path_mappa_batch_csv <- file.path(
  output_dir,
  paste0("mappa_batch_istituzioni_", codice_lavoro, ".csv")
)

url_base <- "https://api.openalex.org/authors"
url_rate_limit <- "https://api.openalex.org/rate-limit"

# -----------------------------------------------------------------------------
# API KEY
# -----------------------------------------------------------------------------
#
# Inserisci le 16 key reali al posto dei placeholder.

api_keys <- c()

api_keys <- trimws(api_keys)
api_keys <- api_keys[
  !is.na(api_keys) &
    api_keys != "" &
    !grepl("^INSERISCI_API_KEY_", api_keys)
]

if (length(api_keys) == 0) {
  stop(
    "Nessuna API key valida trovata. ",
    "Sostituisci i placeholder nel vettore api_keys con le tue key reali."
  )
}

# -----------------------------------------------------------------------------
# Parametri tecnici
# -----------------------------------------------------------------------------

# Quante istituzioni mettere dentro un singolo filtro OpenAlex.
# Valori ragionevoli: 10, 25, 50.
# Per il tuo caso attuale: circa 2.100 istituzioni totali.
# Con 10 istituzioni per batch ottieni circa 210 batch, quindi il cluster
# ha abbastanza granularità per distribuire meglio il lavoro tra 32 worker.
institution_batch_size <- 10L

# Numero di risultati per pagina OpenAlex. 200 è il massimo pratico usato già nello script originario.
per_page <- 200L

# Numero di worker paralleli.
n_workers <- 16

# Numero di batch processati in ogni blocco parallelo.
chunk_parallel_size <- 10L

# RDS non compresso: più veloce e più adatto a dataset grandi, ma occupa più spazio.
# Metti TRUE se preferisci risparmiare disco a costo di più CPU/tempo.
compress_output_rds <- FALSE

# Campi da scaricare da OpenAlex Authors.
campi <- paste(
  c(
    "id",
    "orcid",
    "display_name",
    "display_name_alternatives",
    "works_count",
    "cited_by_count",
    "counts_by_year",
    "summary_stats",
    "updated_date",
    "created_date",
    "affiliations",
    "last_known_institutions",
    "topics",
    "topic_share",
    "x_concepts"
  ),
  collapse = ","
)


cat("Lavoro corrente:", codice_lavoro, "\n")
cat("File istituzioni:", path_istituzioni, "\n")
cat("File output RDS:", path_output_rds, "\n")
cat("File stato CSV:", path_stato_csv, "\n")
cat("File mappa batch-istituzioni:", path_mappa_batch_csv, "\n")
cat("Numero API key:", length(api_keys), "\n")
cat("Worker paralleli:", n_workers, "\n")
cat("Istituzioni per batch:", institution_batch_size, "\n")
cat("Batch per blocco parallelo:", chunk_parallel_size, "\n")

# =============================================================================
# 2. PROXY NELLA SESSIONE PRINCIPALE
# =============================================================================

# Se nel tuo ambiente il proxy è già impostato, puoi mettere FALSE.
usa_proxy_bankit <- TRUE

if (isTRUE(usa_proxy_bankit)) {
  .myusername <- Sys.getenv("USER")
  .mypwd <- askpass("Insert user password: ")

  Sys.setenv(
    http_proxy = paste0(
      "http://utenze%5c",
      .myusername,
      ":",
      .mypwd,
      "@itaca-prod.utenze.bankit.it:8080"
    )
  )

  Sys.setenv(
    https_proxy = paste0(
      "http://utenze%5c",
      .myusername,
      ":",
      .mypwd,
      "@itaca-prod.utenze.bankit.it:8080"
    )
  )
}

# =============================================================================
# 3. CARICAMENTO OUTPUT E STATO PREGRESSI
# =============================================================================

if (file.exists(path_output_rds)) {

  autori_completo <- readRDS(path_output_rds)
  autori_completo <- as.data.table(autori_completo)

  if ("id" %in% names(autori_completo)) {
    n_prima_dedup <- nrow(autori_completo)
    autori_completo <- autori_completo[!duplicated(id)]

    if (nrow(autori_completo) < n_prima_dedup) {
      saveRDS(
        autori_completo,
        path_output_rds,
        compress = compress_output_rds
      )
    }
  }

} else {

  autori_completo <- data.table()
}

if (file.exists(path_stato_csv)) {

  stato_download <- data.table::fread(path_stato_csv)
  stato_download <- as.data.table(stato_download)

  if (!"batch" %in% names(stato_download)) {
    stop("Il file di stato esiste ma non contiene la colonna 'batch': ", path_stato_csv)
  }

  if (!"status" %in% names(stato_download)) {
    stop("Il file di stato esiste ma non contiene la colonna 'status': ", path_stato_csv)
  }

  stato_download[, batch := as.integer(batch)]

  # Protezione: se lo stato esiste ma l'RDS non esiste, non considero affidabili
  # i batch già marcati come completati.
  if (!file.exists(path_output_rds)) {
    warning(
      "Esiste un file di stato ma non esiste l'output RDS. ",
      "Ignoro i batch completati e riscarico."
    )
    stato_download <- stato_download[status != "completato"]
  }

} else {

  stato_download <- data.table(
    batch = integer(),
    status = character(),
    errore = character(),
    n_istituzioni_batch = numeric(),
    n_pagine = numeric(),
    n_righe_lorde = numeric(),
    n_autori_unici_batch = numeric(),
    aggiornato_il = character()
  )
}

batch_completati <- stato_download[
  status == "completato",
  unique(as.integer(batch))
]

batch_falliti <- setdiff(
  stato_download[status == "fallito", unique(as.integer(batch))],
  batch_completati
)

cat("Autori già presenti:", nrow(autori_completo), "\n")
cat("Batch già completati:", length(batch_completati), "\n")
cat("Batch falliti non ancora completati:", length(batch_falliti), "\n")

# =============================================================================
# 4. CARICAMENTO ISTITUZIONI E CREAZIONE BATCH
# =============================================================================

if (!file.exists(path_istituzioni)) {
  stop("File istituzioni non trovato: ", path_istituzioni)
}

istituzioni_it <- data.table::fread(path_istituzioni)

if (ncol(istituzioni_it) == 0 || nrow(istituzioni_it) == 0) {
  stop("Il file istituzioni è vuoto: ", path_istituzioni)
}

if ("id_istituzioni" %in% names(istituzioni_it)) {
  institution_ids <- istituzioni_it[["id_istituzioni"]]
} else {
  institution_ids <- istituzioni_it[[1]]
}

institution_ids <- basename(institution_ids)
institution_ids <- toupper(institution_ids)
institution_ids <- unique(institution_ids)
institution_ids <- institution_ids[!is.na(institution_ids) & institution_ids != ""]

if (length(institution_ids) == 0) {
  stop("Nessun ID istituzione valido trovato nel file: ", path_istituzioni)
}

# -----------------------------------------------------------------------------
# Creazione o riuso della mappa batch-istituzioni
# -----------------------------------------------------------------------------
# Non randomizzo: mantengo l'ordine originale del file istituzioni.
# La mappa viene però salvata al primo run e riusata nelle ripartenze.
# Questo evita che cambiare per errore institution_batch_size dopo un'interruzione
# faccia slittare il significato dei numeri di batch già registrati nello stato.

if (file.exists(path_mappa_batch_csv)) {

  mappa_batch_istituzioni <- data.table::fread(path_mappa_batch_csv)
  mappa_batch_istituzioni <- as.data.table(mappa_batch_istituzioni)

  if (!all(c("batch", "institution_id") %in% names(mappa_batch_istituzioni))) {
    stop(
      "La mappa batch-istituzioni esiste ma non contiene le colonne richieste: ",
      "batch e institution_id. File: ",
      path_mappa_batch_csv
    )
  }

  mappa_batch_istituzioni[, batch := as.integer(batch)]
  mappa_batch_istituzioni[, institution_id := as.character(institution_id)]
  if ("posizione_nel_batch" %in% names(mappa_batch_istituzioni)) {
    mappa_batch_istituzioni <- mappa_batch_istituzioni[order(batch, posizione_nel_batch)]
  } else {
    mappa_batch_istituzioni <- mappa_batch_istituzioni[order(batch)]
  }

  institution_batches <- split(
    mappa_batch_istituzioni$institution_id,
    mappa_batch_istituzioni$batch
  )

  names(institution_batches) <- as.character(seq_along(institution_batches))

  cat(
    "Mappa batch-istituzioni esistente caricata: ",
    path_mappa_batch_csv,
    "\n",
    sep = ""
  )

} else {

  institution_batches <- split(
    institution_ids,
    ceiling(seq_along(institution_ids) / institution_batch_size)
  )

  mappa_batch_istituzioni <- data.table(
    batch = rep(seq_along(institution_batches), lengths(institution_batches)),
    posizione_nel_batch = unlist(
      lapply(institution_batches, seq_along),
      use.names = FALSE
    ),
    institution_id = unlist(institution_batches, use.names = FALSE)
  )

  data.table::fwrite(
    mappa_batch_istituzioni,
    path_mappa_batch_csv
  )

  cat(
    "Mappa batch-istituzioni creata: ",
    path_mappa_batch_csv,
    "\n",
    sep = ""
  )
}

batch_da_fare <- setdiff(
  seq_along(institution_batches),
  batch_completati
)

blocchi <- if (length(batch_da_fare) > 0) {
  split(
    batch_da_fare,
    ceiling(seq_along(batch_da_fare) / chunk_parallel_size)
  )
} else {
  list()
}

cat("Istituzioni totali:", length(institution_ids), "\n")
cat("Batch totali:", length(institution_batches), "\n")
cat("Batch da scaricare:", length(batch_da_fare), "\n")

# =============================================================================
# 5. FUNZIONI DI SUPPORTO PER RDS, STATO E DOWNLOAD
# =============================================================================

scrivi_stato_csv <- function(stato_download, path_stato_csv) {

  if (nrow(stato_download) > 0) {
    stato_download <- stato_download[order(batch)]
  }

  data.table::fwrite(stato_download, path_stato_csv)
}

salva_output_rds <- function(autori_completo,
                             path_output_rds,
                             compress_output_rds = FALSE) {

  tmp_path <- paste0(path_output_rds, ".tmp")

  saveRDS(
    autori_completo,
    tmp_path,
    compress = compress_output_rds
  )

  if (file.exists(path_output_rds)) {
    file.remove(path_output_rds)
  }

  file.rename(tmp_path, path_output_rds)
}

aggiorna_stato <- function(stato_download, nuove_righe) {

  if (is.null(nuove_righe) || nrow(nuove_righe) == 0) {
    return(stato_download)
  }

  nuove_righe[, batch := as.integer(batch)]

  # Una nuova riga di stato sostituisce lo stato precedente dello stesso batch.
  stato_download <- stato_download[!batch %in% nuove_righe$batch]

  stato_download <- data.table::rbindlist(
    list(stato_download, nuove_righe),
    fill = TRUE
  )

  stato_download[]
}

bind_data_tables_safe <- function(lista) {

  lista <- lista[!vapply(lista, is.null, logical(1))]
  lista <- lista[vapply(lista, function(x) nrow(as.data.frame(x)) > 0, logical(1))]

  if (length(lista) == 0) {
    return(data.table())
  }

  lista <- lapply(lista, as.data.table)

  data.table::rbindlist(
    lista,
    fill = TRUE,
    use.names = TRUE
  )
}

aggiungi_nuovi_autori <- function(df_blocco,
                                  autori_completo) {

  if (is.null(df_blocco) || nrow(df_blocco) == 0) {
    return(autori_completo)
  }

  df_blocco <- as.data.table(df_blocco)

  if ("id" %in% names(df_blocco)) {
    df_blocco <- df_blocco[!duplicated(id)]
  }

  if (
    nrow(autori_completo) > 0 &&
    "id" %in% names(autori_completo) &&
    "id" %in% names(df_blocco)
  ) {
    nuovi <- df_blocco[!id %in% autori_completo$id]
  } else {
    nuovi <- df_blocco
  }

  if (nrow(nuovi) == 0) {
    return(autori_completo)
  }

  autori_completo <- data.table::rbindlist(
    list(autori_completo, nuovi),
    fill = TRUE,
    use.names = TRUE
  )

  if ("id" %in% names(autori_completo)) {
    autori_completo <- autori_completo[!duplicated(id)]
  }

  autori_completo[]
}

make_filter_institutions <- function(ids_correnti) {

  paste0(
    "affiliations.institution.id:",
    paste(ids_correnti, collapse = "|")
  )
}

formatta_reset_italia <- function(resets_at) {

  if (
    is.null(resets_at) ||
    length(resets_at) == 0 ||
    is.na(resets_at) ||
    resets_at == ""
  ) {
    return(NA_character_)
  }

  reset_utc <- as.POSIXct(
    resets_at,
    format = "%Y-%m-%dT%H:%M:%SZ",
    tz = "UTC"
  )

  if (is.na(reset_utc)) {
    return(as.character(resets_at))
  }

  format(
    reset_utc,
    tz = "Europe/Rome",
    usetz = TRUE
  )
}

check_rate_limit_worker <- function(api_key,
                                    url_rate_limit) {

  risposta <- tryCatch(
    httr::GET(
      url_rate_limit,
      query = list(api_key = api_key),
      httr::timeout(60),
      httr::config(connecttimeout = 30)
    ),
    error = function(e) {
      return(NULL)
    }
  )

  if (is.null(risposta)) {
    return(
      list(
        ok = FALSE,
        errore = "Nessuna risposta da /rate-limit"
      )
    )
  }

  status <- httr::status_code(risposta)
  testo <- httr::content(risposta, "text", encoding = "UTF-8")

  parsed <- tryCatch(
    jsonlite::fromJSON(testo, flatten = TRUE),
    error = function(e) NULL
  )

  if (is.null(parsed)) {
    return(
      list(
        ok = FALSE,
        status = status,
        errore = substr(testo, 1, 500)
      )
    )
  }

  rl <- if (!is.null(parsed$rate_limit)) parsed$rate_limit else parsed

  resets_at <- if (!is.null(rl$resets_at)) rl$resets_at else NA_character_
  resets_in_seconds <- if (!is.null(rl$resets_in_seconds)) rl$resets_in_seconds else NA_real_

  credits_remaining <- if (!is.null(rl$credits_remaining)) {
    rl$credits_remaining
  } else if (!is.null(rl$remaining)) {
    rl$remaining
  } else {
    NA_real_
  }

  credits_used <- if (!is.null(rl$credits_used)) {
    rl$credits_used
  } else if (!is.null(rl$used)) {
    rl$used
  } else {
    NA_real_
  }

  credits_limit <- if (!is.null(rl$credits_limit)) {
    rl$credits_limit
  } else if (!is.null(rl$limit)) {
    rl$limit
  } else {
    NA_real_
  }

  daily_remaining_usd <- if (!is.null(rl$daily_remaining_usd)) {
    rl$daily_remaining_usd
  } else {
    NA_real_
  }

  daily_used_usd <- if (!is.null(rl$daily_used_usd)) {
    rl$daily_used_usd
  } else {
    NA_real_
  }

  daily_budget_usd <- if (!is.null(rl$daily_budget_usd)) {
    rl$daily_budget_usd
  } else {
    NA_real_
  }

  list(
    ok = TRUE,
    status = status,
    credits_limit = credits_limit,
    credits_used = credits_used,
    credits_remaining = credits_remaining,
    daily_budget_usd = daily_budget_usd,
    daily_used_usd = daily_used_usd,
    daily_remaining_usd = daily_remaining_usd,
    resets_at_utc = resets_at,
    resets_at_italia = formatta_reset_italia(resets_at),
    resets_in_seconds = resets_in_seconds
  )
}

sintesi_rate_limits <- function(rate_infos) {

  righe <- lapply(seq_along(rate_infos), function(i) {

    x <- rate_infos[[i]]

    key_index <- if (!is.null(x$key_index)) x$key_index else i

    if (isFALSE(x$ok)) {
      return(
        paste0(
          "key_index ", key_index,
          ": controllo /rate-limit fallito; errore = ",
          x$errore
        )
      )
    }

    paste0(
      "key_index ", key_index,
      ": credits_remaining = ", x$credits_remaining,
      "; daily_remaining_usd = ", x$daily_remaining_usd,
      "; resets_at_italia = ", x$resets_at_italia,
      "; resets_in_seconds = ", x$resets_in_seconds,
      "; retry_after_header = ",
      ifelse(is.null(x$retry_after_header), NA, x$retry_after_header)
    )
  })

  paste(righe, collapse = "\n")
}

get_with_key_rotation_worker <- function(query,
                                         url_base,
                                         url_rate_limit,
                                         api_keys,
                                         key_start = 1,
                                         max_tentativi_connessione = 2) {

  n_keys <- length(api_keys)
  key_order <- ((seq_len(n_keys) + key_start - 2) %% n_keys) + 1

  ultimo_messaggio_429 <- NA_character_
  ultimo_errore <- NA_character_
  rate_infos_429 <- list()

  for (k in key_order) {

    query$api_key <- api_keys[k]

    for (tentativo in seq_len(max_tentativi_connessione)) {

      risposta <- tryCatch(
        httr::GET(
          url_base,
          query = query,
          httr::timeout(120),
          httr::config(connecttimeout = 60)
        ),
        error = function(e) {
          ultimo_errore <<- conditionMessage(e)
          NULL
        }
      )

      if (is.null(risposta)) {
        next
      }

      status <- httr::status_code(risposta)
      testo <- httr::content(risposta, "text", encoding = "UTF-8")

      if (status == 429) {

        parsed_error <- tryCatch(
          jsonlite::fromJSON(testo),
          error = function(e) NULL
        )

        retry_after <- httr::headers(risposta)[["retry-after"]]

        ultimo_messaggio_429 <- if (!is.null(parsed_error$message)) {
          parsed_error$message
        } else {
          substr(testo, 1, 500)
        }

        info_key <- check_rate_limit_worker(
          api_key = api_keys[k],
          url_rate_limit = url_rate_limit
        )

        info_key$key_index <- k
        info_key$retry_after_header <- retry_after
        info_key$messaggio_429 <- ultimo_messaggio_429

        rate_infos_429[[length(rate_infos_429) + 1]] <- info_key

        break
      }

      if (status >= 400) {
        stop(
          "Errore API status ", status,
          ". Risposta: ", substr(testo, 1, 1000)
        )
      }

      return(risposta)
    }
  }

  if (length(rate_infos_429) > 0) {

    stop(
      "Tutte le API key disponibili per questo worker hanno ricevuto 429.\n",
      "Ultimo messaggio 429: ", ultimo_messaggio_429, "\n\n",
      "Stato rate limit delle key provate:\n",
      sintesi_rate_limits(rate_infos_429)
    )

  } else {

    stop(
      "Nessuna risposta HTTP ricevuta. Probabile problema di rete/proxy. ",
      "Ultimo errore connessione: ",
      ultimo_errore
    )
  }
}

scarica_batch_istituzioni_worker <- function(b,
                                             institution_batches,
                                             campi,
                                             per_page,
                                             url_base,
                                             url_rate_limit,
                                             api_keys) {

  ids_correnti <- institution_batches[[b]]
  filtro <- make_filter_institutions(ids_correnti)

  cursore <- "*"
  pagina <- 1L
  risultati_batch <- list()
  n_righe_lorde <- 0L

  key_start <- ((b - 1L) %% length(api_keys)) + 1L

  repeat {

    query <- list(
      filter = filtro,
      select = campi,
      `per-page` = per_page,
      cursor = cursore
    )

    risposta <- get_with_key_rotation_worker(
      query = query,
      url_base = url_base,
      url_rate_limit = url_rate_limit,
      api_keys = api_keys,
      key_start = key_start
    )

    testo <- httr::content(risposta, "text", encoding = "UTF-8")

    parsed <- jsonlite::fromJSON(
      testo,
      flatten = TRUE
    )

    risultati <- parsed$results

    if (!is.null(risultati) && is.data.frame(risultati) && nrow(risultati) > 0) {
      risultati_batch[[length(risultati_batch) + 1L]] <- risultati
      n_righe_lorde <- n_righe_lorde + nrow(risultati)
    }

    prossimo_cursore <- parsed$meta$next_cursor

    if (
      is.null(prossimo_cursore) ||
      length(prossimo_cursore) == 0 ||
      is.na(prossimo_cursore) ||
      prossimo_cursore == ""
    ) {
      break
    }

    cursore <- prossimo_cursore
    pagina <- pagina + 1L
  }

  df_batch <- bind_data_tables_safe(risultati_batch)

  if (nrow(df_batch) > 0 && "id" %in% names(df_batch)) {
    df_batch <- df_batch[!duplicated(id)]
  }

  list(
    batch = as.integer(b),
    ok = TRUE,
    data = df_batch,
    n_istituzioni_batch = as.numeric(length(ids_correnti)),
    n_pagine = as.numeric(pagina),
    n_righe_lorde = as.numeric(n_righe_lorde),
    n_autori_unici_batch = as.numeric(nrow(df_batch)),
    errore = NA_character_
  )
}

scarica_batch_safe <- function(b,
                               institution_batches,
                               campi,
                               per_page,
                               url_base,
                               url_rate_limit,
                               api_keys) {

  tryCatch(
    scarica_batch_istituzioni_worker(
      b = b,
      institution_batches = institution_batches,
      campi = campi,
      per_page = per_page,
      url_base = url_base,
      url_rate_limit = url_rate_limit,
      api_keys = api_keys
    ),
    error = function(e) {
      list(
        batch = as.integer(b),
        ok = FALSE,
        data = data.table(),
        n_istituzioni_batch = if (!is.null(institution_batches[[b]])) {
          as.numeric(length(institution_batches[[b]]))
        } else {
          NA_real_
        },
        n_pagine = NA_real_,
        n_righe_lorde = NA_real_,
        n_autori_unici_batch = NA_real_,
        errore = conditionMessage(e)
      )
    }
  )
}

# =============================================================================
# 6. CLUSTER PARALLELO CON PROXY
# =============================================================================

cl <- parallel::makeCluster(n_workers)

on.exit({
  if (exists("cl") && !is.null(cl)) {
    try(parallel::stopCluster(cl), silent = TRUE)
  }
}, add = TRUE)

proxy_http <- Sys.getenv("http_proxy")
proxy_https <- Sys.getenv("https_proxy")

parallel::clusterExport(
  cl,
  varlist = c("proxy_http", "proxy_https"),
  envir = environment()
)

parallel::clusterEvalQ(cl, {
  Sys.setenv(
    http_proxy = proxy_http,
    https_proxy = proxy_https
  )

  library(httr)
  library(jsonlite)
  library(data.table)
})

parallel::clusterExport(
  cl,
  varlist = c(
    "institution_batches",
    "campi",
    "per_page",
    "url_base",
    "url_rate_limit",
    "api_keys",
    "make_filter_institutions",
    "formatta_reset_italia",
    "check_rate_limit_worker",
    "sintesi_rate_limits",
    "get_with_key_rotation_worker",
    "bind_data_tables_safe",
    "scarica_batch_istituzioni_worker",
    "scarica_batch_safe"
  ),
  envir = environment()
)

# =============================================================================
# 7. LOOP PRINCIPALE A BLOCCHI PARALLELI
# =============================================================================

inizio_totale <- Sys.time()

if (length(blocchi) == 0) {
  cat("\nNessun batch da scaricare.\n")
}

for (g in seq_along(blocchi)) {

  inizio_blocco <- Sys.time()
  batch_blocco <- blocchi[[g]]

  cat(
    "\n=== ",
    codice_lavoro,
    " | Blocco ", g, "/", length(blocchi),
    " | Batch: ", min(batch_blocco), "-", max(batch_blocco),
    " | N batch: ", length(batch_blocco),
    " ===\n",
    sep = ""
  )

  risultati <- parallel::parLapplyLB(
    cl,
    batch_blocco,
    scarica_batch_safe,
    institution_batches = institution_batches,
    campi = campi,
    per_page = per_page,
    url_base = url_base,
    url_rate_limit = url_rate_limit,
    api_keys = api_keys
  )

  ok <- vapply(risultati, function(x) isTRUE(x$ok), logical(1))

  risultati_ok <- risultati[ok]
  risultati_fail <- risultati[!ok]

  # ---------------------------------------------------------------------------
  # Salvo comunque i batch riusciti, anche se nello stesso blocco ce ne sono
  # alcuni falliti.
  # ---------------------------------------------------------------------------

  if (length(risultati_ok) > 0) {

    pagine_blocco <- sum(
      vapply(risultati_ok, function(x) as.numeric(x$n_pagine), numeric(1)),
      na.rm = TRUE
    )

    righe_lorde_blocco <- sum(
      vapply(risultati_ok, function(x) as.numeric(x$n_righe_lorde), numeric(1)),
      na.rm = TRUE
    )

    istituzioni_blocco <- sum(
      vapply(risultati_ok, function(x) as.numeric(x$n_istituzioni_batch), numeric(1)),
      na.rm = TRUE
    )

    batch_ok <- vapply(
      risultati_ok,
      function(x) as.integer(x$batch),
      integer(1)
    )

    dati_blocco <- lapply(risultati_ok, function(x) x$data)
    df_blocco <- bind_data_tables_safe(dati_blocco)

    if (nrow(df_blocco) > 0 && "id" %in% names(df_blocco)) {
      df_blocco <- df_blocco[!duplicated(id)]
    }

    autori_prima <- nrow(autori_completo)
    autori_unici_blocco <- nrow(df_blocco)

    autori_completo <- aggiungi_nuovi_autori(
      df_blocco = df_blocco,
      autori_completo = autori_completo
    )

    autori_totali <- nrow(autori_completo)
    autori_nuovi <- autori_totali - autori_prima

    # Salvataggio incrementale dell'output in RDS dopo ogni blocco.
    salva_output_rds(
      autori_completo = autori_completo,
      path_output_rds = path_output_rds,
      compress_output_rds = compress_output_rds
    )

    righe_stato_ok <- data.table(
      batch = batch_ok,
      status = "completato",
      errore = NA_character_,
      n_istituzioni_batch = vapply(risultati_ok, function(x) as.numeric(x$n_istituzioni_batch), numeric(1)),
      n_pagine = vapply(risultati_ok, function(x) as.numeric(x$n_pagine), numeric(1)),
      n_righe_lorde = vapply(risultati_ok, function(x) as.numeric(x$n_righe_lorde), numeric(1)),
      n_autori_unici_batch = vapply(risultati_ok, function(x) as.numeric(x$n_autori_unici_batch), numeric(1)),
      aggiornato_il = as.character(Sys.time())
    )

    stato_download <- aggiorna_stato(
      stato_download = stato_download,
      nuove_righe = righe_stato_ok
    )

    scrivi_stato_csv(
      stato_download = stato_download,
      path_stato_csv = path_stato_csv
    )

  } else {

    pagine_blocco <- 0
    righe_lorde_blocco <- 0
    istituzioni_blocco <- 0
    autori_unici_blocco <- 0
    autori_nuovi <- 0
    autori_totali <- nrow(autori_completo)
  }

  # ---------------------------------------------------------------------------
  # Registro eventuali batch falliti.
  # ---------------------------------------------------------------------------

  if (length(risultati_fail) > 0) {

    falliti <- vapply(
      risultati_fail,
      function(x) as.integer(x$batch),
      integer(1)
    )

    errori <- vapply(
      risultati_fail,
      function(x) as.character(x$errore),
      character(1)
    )

    righe_stato_fail <- data.table(
      batch = falliti,
      status = "fallito",
      errore = errori,
      n_istituzioni_batch = vapply(risultati_fail, function(x) as.numeric(x$n_istituzioni_batch), numeric(1)),
      n_pagine = NA_real_,
      n_righe_lorde = NA_real_,
      n_autori_unici_batch = NA_real_,
      aggiornato_il = as.character(Sys.time())
    )

    stato_download <- aggiorna_stato(
      stato_download = stato_download,
      nuove_righe = righe_stato_fail
    )

    scrivi_stato_csv(
      stato_download = stato_download,
      path_stato_csv = path_stato_csv
    )

    cat("\nERRORE in uno o più batch:\n")
    print(data.table(batch = falliti, errore = errori))

    rm(risultati, risultati_ok, risultati_fail)
    if (exists("dati_blocco")) rm(dati_blocco)
    if (exists("df_blocco")) rm(df_blocco)
    gc()

    stop(
      "Download interrotto: almeno un batch parallelo è fallito. ",
      "I batch riusciti nello stesso blocco sono stati salvati in RDS."
    )
  }

  fine_blocco <- Sys.time()
  tempo_blocco <- round(difftime(fine_blocco, inizio_blocco, units = "mins"), 2)

  rm(risultati, risultati_ok, risultati_fail)
  if (exists("dati_blocco")) rm(dati_blocco)
  if (exists("df_blocco")) rm(df_blocco)
  gc()

  batch_completati <- stato_download[
    status == "completato",
    unique(as.integer(batch))
  ]

  batch_falliti <- setdiff(
    stato_download[status == "fallito", unique(as.integer(batch))],
    batch_completati
  )

  cat("  Istituzioni coperte nel blocco:", istituzioni_blocco, "\n")
  cat("  Pagine scaricate nel blocco:", pagine_blocco, "\n")
  cat("  Righe lorde scaricate nel blocco:", righe_lorde_blocco, "\n")
  cat("  Autori unici nel blocco:", autori_unici_blocco, "\n")
  cat("  Autori nuovi aggiunti all'RDS:", autori_nuovi, "\n")
  cat("  Autori unici totali in memoria:", autori_totali, "\n")
  cat("  Batch completati:", length(batch_completati), "\n")
  cat("  Batch falliti non completati:", length(batch_falliti), "\n")
  cat("  Tempo blocco:", tempo_blocco, "minuti\n")
}

fine_totale <- Sys.time()

parallel::stopCluster(cl)
cl <- NULL

# =============================================================================
# 8. SALVATAGGIO / RIEPILOGO FINALE
# =============================================================================

# L'RDS è già stato salvato incrementalmente dopo ogni blocco riuscito.
# Qui riscrivo soltanto output e stato finali per sicurezza.

salva_output_rds(
  autori_completo = autori_completo,
  path_output_rds = path_output_rds,
  compress_output_rds = compress_output_rds
)

scrivi_stato_csv(
  stato_download = stato_download,
  path_stato_csv = path_stato_csv
)

cat("\n=== DOWNLOAD COMPLETATO:", codice_lavoro, "===\n")
cat("Batch completati:", length(stato_download[status == "completato", unique(batch)]), "\n")
cat("Batch falliti non completati:", length(setdiff(
  stato_download[status == "fallito", unique(batch)],
  stato_download[status == "completato", unique(batch)]
)), "\n")
cat("Autori unici totali:", nrow(autori_completo), "\n")
cat("File output RDS:", path_output_rds, "\n")
cat("File stato CSV:", path_stato_csv, "\n")
cat("File mappa batch-istituzioni:", path_mappa_batch_csv, "\n")
cat(
  "Tempo totale:",
  round(difftime(fine_totale, inizio_totale, units = "mins"), 2),
  "minuti\n"
)
