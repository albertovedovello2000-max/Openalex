# =============================================================================
# DOWNLOAD WORKS OPENALEX DA LISTA DI ID - SOLO CAMPI FLAT
# PARALLELIZZATO A BLOCCHI, OUTPUT UNICO CSV, STATO LEGGERO IN CSV
# =============================================================================

library(httr)
library(jsonlite)
library(data.table)
library(parallel)
library(askpass)

# =============================================================================
# 1. CONFIGURAZIONE GENERALE
# =============================================================================

# -----------------------------------------------------------------------------
# MODIFICA QUI: cartella e nome del file con gli ID works OpenAlex
# -----------------------------------------------------------------------------

input_dir_works <- "/home/group/main/openalex/Openalex API/API Works/id_works"
file_works_ids <- "id_works.csv"

path_works_ids <- file.path(
  input_dir_works,
  file_works_ids
)

codice_lavoro <- "works_base"

output_dir <- "/home/group/main/openalex/Openalex API/API Works/"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

path_stato_chunk_csv <- file.path(
  output_dir,
  paste0("stato_chunk_", codice_lavoro, ".csv")
)

path_batch_falliti_csv <- file.path(
  output_dir,
  paste0("batch_falliti_", codice_lavoro, ".csv")
)

path_missing_ids_csv <- file.path(
  output_dir,
  paste0("missing_ids_", codice_lavoro, ".csv")
)

path_works_flat_csv <- file.path(
  output_dir,
  paste0("works_flat_", codice_lavoro, ".csv")
)


path_input_manifest_csv <- file.path(
  output_dir,
  paste0("manifest_input_", codice_lavoro, ".csv")
)

url_base <- "https://api.openalex.org/works"
url_rate_limit <- "https://api.openalex.org/rate-limit"

# -----------------------------------------------------------------------------
# API KEY
# -----------------------------------------------------------------------------

# Opzione consigliata: salva le key in una variabile d'ambiente separandole con ;
# Esempio shell: export OPENALEX_API_KEYS="key1;key2;key3"
#api_keys <- unlist(strsplit(Sys.getenv("OPENALEX_API_KEYS"), ";", fixed = TRUE))

api_keys <- c()


api_keys <- trimws(api_keys)
api_keys <- api_keys[!is.na(api_keys) & api_keys != ""]

if (length(api_keys) == 0) {
  stop("Nessuna API key trovata. Inseriscile nel vettore api_keys.")
}



# -----------------------------------------------------------------------------
# Parametri tecnici
# -----------------------------------------------------------------------------

# OpenAlex documenta un massimo di 100 valori OR per filtro.
# Per questo non portare work_batch_size sopra 100, anche se per-page=200
# può sembrare funzionare in alcune situazioni.
work_batch_size <- 100L

# Numero indicativo di works per ogni chunk salvato nel CSV finale.
# Con work_batch_size = 100, 200000 works = 2000 richieste API per chunk.
works_per_file <- 200000L

chunk_parallel_size <- as.integer(ceiling(works_per_file / work_batch_size))

# Per richieste by-ID con massimo 100 ID nel filtro, per-page=100 basta.
# Usare 200 qui non riduce il numero di chiamate se work_batch_size resta 100.
per_page <- 100L

# Con 40 core fisici/logici disponibili, 32 worker è una partenza aggressiva ma ragionevole.
n_workers <- 32L

# Se TRUE, elimina duplicati negli ID input prima di creare i batch.
# Lo stato farà riferimento alle posizioni del vettore dopo questa normalizzazione.
dedup_input_ids <- TRUE

# Se TRUE, un solo batch fallito interrompe il download dopo aver aggiornato lo stato.
# Scelta consigliata: rende il problema esplicito e impedisce di marcare completato un chunk parziale.
stop_on_failed_batch <- TRUE

# -----------------------------------------------------------------------------
# MODIFICA QUI: campi da scaricare
# -----------------------------------------------------------------------------
# Campi semplici, non nested, da salvare nel CSV works_flat_*.
# Non inserire "id": viene sempre scaricato e salvato come id_work.
non_nested_vars <- c(
  "doi",
  "title",
  "display_name",
  "publication_date",
  "publication_year",
  "type",
  "fwci",
  "cited_by_count",
  "referenced_works_count",
  "is_retracted"
)

# Nessun campo nested in questo script: output solo flat.
nested_vars <- character(0)

parse_campi_config <- function(x) {
  # Accetta sia c("doi", "title") sia una stringa unica tipo
  # "doi,title,display_name". Questo evita che il vettore di variabili
  # venga interpretato come un solo nome di colonna.
  if (is.null(x) || length(x) == 0) {
    return(character(0))
  }
  x <- as.character(x)
  x <- unlist(strsplit(x, "[,;|]", perl = TRUE), use.names = FALSE)
  x <- trimws(x)
  x <- x[!is.na(x) & x != ""]
  unique(x)
}

non_nested_vars <- parse_campi_config(non_nested_vars)
non_nested_vars <- non_nested_vars[non_nested_vars != "id"]

nested_vars <- unique(trimws(nested_vars))
nested_vars <- nested_vars[!is.na(nested_vars) & nested_vars != ""]

campi_select <- paste(unique(c("id", non_nested_vars)), collapse = ",")

cat("Lavoro corrente:", codice_lavoro, "\n")
cat("File input works:", path_works_ids, "\n")
cat("Output dir:", output_dir, "\n")
cat("Stato chunk CSV:", path_stato_chunk_csv, "\n")
cat("Batch falliti CSV:", path_batch_falliti_csv, "\n")
cat("Missing IDs CSV:", path_missing_ids_csv, "\n")
cat("Works flat CSV:", path_works_flat_csv, "\n")
cat("Campi OpenAlex select:", campi_select, "\n")
cat("Numero API key:", length(api_keys), "\n")
cat("Work batch size:", work_batch_size, "\n")
cat("Works per chunk CSV:", works_per_file, "\n")
cat("Batch per chunk:", chunk_parallel_size, "\n")
cat("Worker paralleli:", n_workers, "\n")

# =============================================================================
# 2. PROXY NELLA SESSIONE PRINCIPALE
# =============================================================================

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

# =============================================================================
# 3. FUNZIONI GENERALI
# =============================================================================

normalizza_work_id <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x <- basename(x)
  x <- toupper(x)
  x <- gsub("^OPENALEX:", "", x)
  x <- x[!is.na(x) & x != ""]
  x
}

make_filter_works <- function(ids_correnti) {
  ids_correnti <- normalizza_work_id(ids_correnti)
  paste0("openalex:", paste(ids_correnti, collapse = "|"))
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
  reset_clean <- sub("\\.\\d+Z$", "Z", as.character(resets_at))
  reset_utc <- as.POSIXct(
    reset_clean,
    format = "%Y-%m-%dT%H:%M:%SZ",
    tz = "UTC"
  )
  if (is.na(reset_utc)) {
    return(as.character(resets_at))
  }
  format(reset_utc, tz = "Europe/Rome", usetz = TRUE)
}

parse_retry_after_seconds <- function(retry_after) {
  if (is.null(retry_after) || length(retry_after) == 0 || is.na(retry_after) || retry_after == "") {
    return(NA_real_)
  }
  retry_after <- as.character(retry_after[[1]])
  numeric_value <- suppressWarnings(as.numeric(retry_after))
  if (!is.na(numeric_value)) {
    return(max(0, numeric_value))
  }
  retry_time <- suppressWarnings(as.POSIXct(
    retry_after,
    format = "%a, %d %b %Y %H:%M:%S",
    tz = "GMT"
  ))
  if (!is.na(retry_time)) {
    return(max(0, as.numeric(difftime(retry_time, Sys.time(), units = "secs"))))
  }
  NA_real_
}

safe_fwrite_append <- function(dt, path) {
  if (is.null(dt) || nrow(dt) == 0) {
    return(invisible(FALSE))
  }
  append_mode <- file.exists(path)
  data.table::fwrite(
    dt,
    path,
    append = append_mode,
    col.names = !append_mode
  )
  invisible(TRUE)
}

valore_scalare_csv <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(NA_character_)
  }
  if (is.list(x)) {
    x <- unlist(x, recursive = TRUE, use.names = FALSE)
  }
  if (length(x) == 0) {
    return(NA_character_)
  }
  x <- as.character(x)
  x <- x[!is.na(x)]
  if (length(x) == 0) {
    return(NA_character_)
  }
  if (length(x) == 1) {
    return(x)
  }
  paste(x, collapse = "|")
}

collassa_valori_csv <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(NA_character_)
  }
  if (is.list(x)) {
    x <- unlist(x, recursive = TRUE, use.names = FALSE)
  }
  if (length(x) == 0) {
    return(NA_character_)
  }
  x <- as.character(x)
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) {
    return(NA_character_)
  }
  paste(unique(x), collapse = "|")
}

oggetto_vuoto_o_na <- function(x) {
  is.null(x) || length(x) == 0 || (!is.list(x) && length(x) == 1 && is.na(x))
}

campo_o_na <- function(x, campo) {
  if (is.null(x) || !is.list(x) || is.null(x[[campo]])) {
    return(NA)
  }
  x[[campo]]
}

estrai_works_flat <- function(risultati, non_nested_vars) {
  if (is.null(risultati) || length(risultati) == 0) {
    out <- data.table(id_work = character())
    for (v in non_nested_vars) out[, (v) := character()]
    return(out)
  }

  righe <- lapply(risultati, function(w) {
    r <- list(id_work = valore_scalare_csv(campo_o_na(w, "id")))
    for (v in non_nested_vars) {
      r[[v]] <- valore_scalare_csv(campo_o_na(w, v))
    }
    r
  })

  out <- data.table::rbindlist(righe, fill = TRUE)
  colonne_attese <- c("id_work", non_nested_vars)
  for (cc in setdiff(colonne_attese, names(out))) {
    out[, (cc) := NA_character_]
  }
  data.table::setcolorder(out, colonne_attese)
  out[]
}

estrai_authorships <- function(risultati) {
  if (is.null(risultati) || length(risultati) == 0) {
    return(data.table(
      id_work = character(),
      author.id = character(),
      author.display.name = character(),
      author.orcid = character(),
      institutions.id = character(),
      institutions.type = character(),
      institutions.country_code = character(),
      is_corresponding = logical(),
      raw_author_name = character(),
      raw_affiliation_strings = character()
    ))
  }

  righe <- list()
  ii <- 0L

  for (w in risultati) {
    id_work <- valore_scalare_csv(campo_o_na(w, "id"))
    authorships <- campo_o_na(w, "authorships")

    if (oggetto_vuoto_o_na(authorships)) {
      next
    }

    for (a in authorships) {
      author <- campo_o_na(a, "author")
      institutions <- campo_o_na(a, "institutions")

      if (oggetto_vuoto_o_na(institutions)) {
        institutions_ids <- NA_character_
        institutions_type <- NA_character_
        institutions_country_code <- NA_character_
      } else {
        institutions_ids <- collassa_valori_csv(lapply(institutions, function(z) campo_o_na(z, "id")))
        institutions_type <- collassa_valori_csv(lapply(institutions, function(z) campo_o_na(z, "type")))
        institutions_country_code <- collassa_valori_csv(lapply(institutions, function(z) campo_o_na(z, "country_code")))
      }

      ii <- ii + 1L
      righe[[ii]] <- list(
        id_work = id_work,
        author.id = valore_scalare_csv(campo_o_na(author, "id")),
        author.display.name = valore_scalare_csv(campo_o_na(author, "display_name")),
        author.orcid = valore_scalare_csv(campo_o_na(author, "orcid")),
        institutions.id = institutions_ids,
        institutions.type = institutions_type,
        institutions.country_code = institutions_country_code,
        is_corresponding = as.logical(campo_o_na(a, "is_corresponding")),
        raw_author_name = valore_scalare_csv(campo_o_na(a, "raw_author_name")),
        raw_affiliation_strings = collassa_valori_csv(campo_o_na(a, "raw_affiliation_strings"))
      )
    }
  }

  if (length(righe) == 0) {
    return(data.table(
      id_work = character(),
      author.id = character(),
      author.display.name = character(),
      author.orcid = character(),
      institutions.id = character(),
      institutions.type = character(),
      institutions.country_code = character(),
      is_corresponding = logical(),
      raw_author_name = character(),
      raw_affiliation_strings = character()
    ))
  }

  out <- data.table::rbindlist(righe, fill = TRUE)
  data.table::setcolorder(out, c(
    "id_work",
    "author.id",
    "author.display.name",
    "author.orcid",
    "institutions.id",
    "institutions.type",
    "institutions.country_code",
    "is_corresponding",
    "raw_author_name",
    "raw_affiliation_strings"
  ))
  out[]
}


scrivi_stato_chunk_csv <- function(stato_chunk, path_stato_chunk_csv) {
  if (nrow(stato_chunk) > 0) {
    stato_chunk <- stato_chunk[order(chunk_id)]
  }
  data.table::fwrite(stato_chunk, path_stato_chunk_csv)
}

aggiorna_stato_chunk <- function(stato_chunk, nuove_righe) {
  if (is.null(nuove_righe) || nrow(nuove_righe) == 0) {
    return(stato_chunk)
  }
  nuove_righe[, chunk_id := as.integer(chunk_id)]
  stato_chunk <- stato_chunk[!chunk_id %in% nuove_righe$chunk_id]
  stato_chunk <- data.table::rbindlist(list(stato_chunk, nuove_righe), fill = TRUE)
  stato_chunk[]
}

# =============================================================================
# 4. RATE LIMIT, API KEY ROTATION E KEY TEMPORANEAMENTE ESAURITE
# =============================================================================

check_rate_limit_worker <- function(api_key, url_rate_limit) {
  risposta <- tryCatch(
    httr::GET(
      url_rate_limit,
      query = list(api_key = api_key),
      httr::timeout(60),
      httr::config(connecttimeout = 30)
    ),
    error = function(e) NULL
  )

  if (is.null(risposta)) {
    return(list(ok = FALSE, errore = "Nessuna risposta da /rate-limit"))
  }

  status <- httr::status_code(risposta)
  testo <- httr::content(risposta, "text", encoding = "UTF-8")

  parsed <- tryCatch(
    jsonlite::fromJSON(testo, flatten = TRUE),
    error = function(e) NULL
  )

  if (is.null(parsed)) {
    return(list(ok = FALSE, status = status, errore = substr(testo, 1, 500)))
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

  daily_remaining_usd <- if (!is.null(rl$daily_remaining_usd)) rl$daily_remaining_usd else NA_real_
  daily_used_usd <- if (!is.null(rl$daily_used_usd)) rl$daily_used_usd else NA_real_
  daily_budget_usd <- if (!is.null(rl$daily_budget_usd)) rl$daily_budget_usd else NA_real_

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

get_key_state_env <- function(api_keys) {
  if (!exists(".openalex_key_state", envir = .GlobalEnv, inherits = FALSE)) {
    assign(".openalex_key_state", new.env(parent = emptyenv()), envir = .GlobalEnv)
  }
  st <- get(".openalex_key_state", envir = .GlobalEnv, inherits = FALSE)
  n_keys <- length(api_keys)
  if (is.null(st$available_at) || length(st$available_at) != n_keys) {
    st$available_at <- rep(as.POSIXct(NA), n_keys)
    st$reason <- rep(NA_character_, n_keys)
    st$last_key_index <- 0L
  }
  st
}

refresh_key_state <- function(st) {
  now <- Sys.time()
  idx <- which(!is.na(st$available_at) & st$available_at <= now)
  if (length(idx) > 0) {
    st$available_at[idx] <- as.POSIXct(NA)
    st$reason[idx] <- NA_character_
  }
  invisible(TRUE)
}

select_available_key <- function(st, api_keys, key_start = 1L) {
  refresh_key_state(st)
  n_keys <- length(api_keys)
  available <- which(is.na(st$available_at))
  if (length(available) == 0) {
    return(NA_integer_)
  }

  if (is.null(st$last_key_index) || is.na(st$last_key_index) || st$last_key_index == 0L) {
    st$last_key_index <- as.integer(((key_start - 2L) %% n_keys) + 1L)
  }

  key_order <- ((seq_len(n_keys) + st$last_key_index - 1L) %% n_keys) + 1L
  key_order <- key_order[key_order %in% available]

  if (length(key_order) == 0) {
    return(NA_integer_)
  }

  k <- as.integer(key_order[1])
  st$last_key_index <- k
  k
}

mark_key_unavailable <- function(st, key_index, wait_seconds, reason = NA_character_) {
  wait_seconds <- suppressWarnings(as.numeric(wait_seconds))
  if (is.na(wait_seconds) || wait_seconds < 1) {
    wait_seconds <- 60
  }
  # margine di sicurezza minimo per evitare di riprovare nel secondo esatto del reset
  wait_seconds <- wait_seconds + 5
  st$available_at[key_index] <- Sys.time() + wait_seconds
  st$reason[key_index] <- as.character(reason)
  invisible(TRUE)
}

sleep_until_first_key_available <- function(st) {
  refresh_key_state(st)
  if (any(is.na(st$available_at))) {
    return(invisible(FALSE))
  }
  next_time <- min(st$available_at, na.rm = TRUE)
  wait_seconds <- max(1, as.numeric(difftime(next_time, Sys.time(), units = "secs")))
  cat(
    "Tutte le API key risultano temporaneamente indisponibili su questo worker. Attendo ",
    round(wait_seconds, 0),
    " secondi.\n",
    sep = ""
  )
  Sys.sleep(wait_seconds)
  refresh_key_state(st)
  invisible(TRUE)
}

calcola_wait_429 <- function(rate_info, retry_after_header) {
  retry_after_seconds <- parse_retry_after_seconds(retry_after_header)

  daily_remaining <- if (!is.null(rate_info$daily_remaining_usd)) {
    suppressWarnings(as.numeric(rate_info$daily_remaining_usd))
  } else {
    NA_real_
  }

  credits_remaining <- if (!is.null(rate_info$credits_remaining)) {
    suppressWarnings(as.numeric(rate_info$credits_remaining))
  } else {
    NA_real_
  }

  resets_in_seconds <- if (!is.null(rate_info$resets_in_seconds)) {
    suppressWarnings(as.numeric(rate_info$resets_in_seconds))
  } else {
    NA_real_
  }

  # Se il credito giornaliero o i credits risultano esauriti, conviene aspettare il reset.
  if ((!is.na(daily_remaining) && daily_remaining <= 0) ||
      (!is.na(credits_remaining) && credits_remaining <= 0)) {
    if (!is.na(resets_in_seconds) && resets_in_seconds > 0) {
      return(resets_in_seconds)
    }
  }

  # Se è un rate limit temporaneo, Retry-After è più pertinente.
  if (!is.na(retry_after_seconds) && retry_after_seconds > 0) {
    return(retry_after_seconds)
  }

  # Fallback: se /rate-limit fornisce reset, usalo; altrimenti pausa breve.
  if (!is.na(resets_in_seconds) && resets_in_seconds > 0) {
    return(min(resets_in_seconds, 300))
  }

  60
}

get_with_key_pool_worker <- function(query,
                                     url_base,
                                     url_rate_limit,
                                     api_keys,
                                     key_start = 1L,
                                     max_tentativi_connessione = 3L,
                                     max_tentativi_server = 5L) {
  st <- get_key_state_env(api_keys)
  tentativi_connessione <- 0L
  tentativi_server <- 0L
  ultimo_errore <- NA_character_

  repeat {
    k <- select_available_key(st, api_keys, key_start = key_start)

    if (is.na(k)) {
      sleep_until_first_key_available(st)
      next
    }

    query$api_key <- api_keys[k]

    risposta <- tryCatch(
      httr::GET(
        url_base,
        query = query,
        httr::timeout(180),
        httr::config(connecttimeout = 60)
      ),
      error = function(e) {
        ultimo_errore <<- conditionMessage(e)
        NULL
      }
    )

    if (is.null(risposta)) {
      tentativi_connessione <- tentativi_connessione + 1L
      if (tentativi_connessione >= max_tentativi_connessione) {
        stop(
          "Nessuna risposta HTTP ricevuta dopo ", max_tentativi_connessione,
          " tentativi. Ultimo errore connessione: ", ultimo_errore
        )
      }
      Sys.sleep(min(60, 5 * tentativi_connessione))
      next
    }

    status <- httr::status_code(risposta)
    testo <- httr::content(risposta, "text", encoding = "UTF-8")

    if (status == 429) {
      retry_after <- httr::headers(risposta)[["retry-after"]]
      parsed_error <- tryCatch(jsonlite::fromJSON(testo), error = function(e) NULL)
      messaggio_429 <- if (!is.null(parsed_error$message)) parsed_error$message else substr(testo, 1, 500)

      rate_info <- check_rate_limit_worker(api_key = api_keys[k], url_rate_limit = url_rate_limit)
      wait_seconds <- calcola_wait_429(rate_info, retry_after)

      reason <- paste0(
        "429; msg=", messaggio_429,
        "; retry_after=", ifelse(is.null(retry_after), NA, retry_after),
        "; resets_in_seconds=", ifelse(is.null(rate_info$resets_in_seconds), NA, rate_info$resets_in_seconds),
        "; daily_remaining_usd=", ifelse(is.null(rate_info$daily_remaining_usd), NA, rate_info$daily_remaining_usd)
      )

      mark_key_unavailable(
        st = st,
        key_index = k,
        wait_seconds = wait_seconds,
        reason = reason
      )

      cat(
        "  429 su key_index ", k,
        "; key sospesa per circa ", round(wait_seconds, 0),
        " secondi.\n",
        sep = ""
      )
      next
    }

    if (status %in% c(500, 502, 503, 504)) {
      tentativi_server <- tentativi_server + 1L
      if (tentativi_server >= max_tentativi_server) {
        stop(
          "Errore server OpenAlex status ", status,
          " dopo ", max_tentativi_server,
          " tentativi. Risposta: ", substr(testo, 1, 1000)
        )
      }
      Sys.sleep(min(120, 10 * tentativi_server))
      next
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

# =============================================================================
# 5. DOWNLOAD DI UN REQUEST-BATCH DI WORKS
# =============================================================================

scarica_work_batch_worker <- function(global_batch_id,
                                      work_ids,
                                      work_batch_size,
                                      campi,
                                      non_nested_vars,
                                      nested_vars,
                                      per_page,
                                      url_base,
                                      url_rate_limit,
                                      api_keys) {
  first_index <- ((as.integer(global_batch_id) - 1L) * work_batch_size) + 1L
  last_index <- min(as.integer(global_batch_id) * work_batch_size, length(work_ids))

  ids_correnti <- work_ids[first_index:last_index]
  ids_correnti <- normalizza_work_id(ids_correnti)

  if (length(ids_correnti) == 0) {
    return(list(
      global_batch_id = as.integer(global_batch_id),
      ok = TRUE,
      flat_data = data.table(),
      authorships_data = data.table(),
      missing_ids = character(),
      n_ids = 0L,
      n_returned = 0L,
      n_missing = 0L,
      errore = NA_character_
    ))
  }

  filtro <- make_filter_works(ids_correnti)
  key_start <- ((as.integer(global_batch_id) - 1L) %% length(api_keys)) + 1L

  query <- list(
    filter = filtro,
    select = campi,
    `per-page` = per_page
  )

  risposta <- get_with_key_pool_worker(
    query = query,
    url_base = url_base,
    url_rate_limit = url_rate_limit,
    api_keys = api_keys,
    key_start = key_start
  )

  testo <- httr::content(risposta, "text", encoding = "UTF-8")

  parsed <- jsonlite::fromJSON(testo, simplifyVector = FALSE)
  risultati <- parsed$results

  if (is.null(risultati) || length(risultati) == 0) {
    risultati <- list()
  }

  flat_batch <- estrai_works_flat(risultati, non_nested_vars = non_nested_vars)

  if ("authorships" %in% nested_vars) {
    authorships_batch <- estrai_authorships(risultati)
  } else {
    authorships_batch <- data.table()
  }

  if (nrow(flat_batch) > 0 && "id_work" %in% names(flat_batch)) {
    returned_ids <- normalizza_work_id(flat_batch[["id_work"]])
    flat_batch <- flat_batch[!duplicated(returned_ids)]
    returned_ids <- normalizza_work_id(flat_batch[["id_work"]])
  } else {
    returned_ids <- character()
  }

  missing_ids <- setdiff(ids_correnti, returned_ids)

  list(
    global_batch_id = as.integer(global_batch_id),
    ok = TRUE,
    flat_data = flat_batch,
    authorships_data = authorships_batch,
    missing_ids = missing_ids,
    n_ids = as.integer(length(ids_correnti)),
    n_returned = as.integer(nrow(flat_batch)),
    n_missing = as.integer(length(missing_ids)),
    errore = NA_character_
  )
}

scarica_work_batch_safe <- function(global_batch_id,
                                    work_ids,
                                    work_batch_size,
                                    campi,
                                    non_nested_vars,
                                    nested_vars,
                                    per_page,
                                    url_base,
                                    url_rate_limit,
                                    api_keys) {
  tryCatch(
    scarica_work_batch_worker(
      global_batch_id = global_batch_id,
      work_ids = work_ids,
      work_batch_size = work_batch_size,
      campi = campi_select,
      non_nested_vars = non_nested_vars,
      nested_vars = nested_vars,
      per_page = per_page,
      url_base = url_base,
      url_rate_limit = url_rate_limit,
      api_keys = api_keys
    ),
    error = function(e) {
      list(
        global_batch_id = as.integer(global_batch_id),
        ok = FALSE,
        flat_data = data.table(),
        authorships_data = data.table(),
        missing_ids = character(),
        n_ids = NA_integer_,
        n_returned = NA_integer_,
        n_missing = NA_integer_,
        errore = conditionMessage(e)
      )
    }
  )
}

# =============================================================================
# 6. CARICAMENTO ID WORKS E CREAZIONE STATO LEGGERO
# =============================================================================

if (!file.exists(path_works_ids)) {
  stop("File works IDs non trovato: ", path_works_ids)
}

works_base <- data.table::fread(path_works_ids)

if (ncol(works_base) == 0 || nrow(works_base) == 0) {
  stop("Il file works IDs è vuoto: ", path_works_ids)
}

if ("id_works" %in% names(works_base)) {
  work_ids <- works_base[["id_works"]]
} else if ("work_id" %in% names(works_base)) {
  work_ids <- works_base[["work_id"]]
} else if ("id" %in% names(works_base)) {
  work_ids <- works_base[["id"]]
} else {
  work_ids <- works_base[[1]]
}

n_input_lordo <- length(work_ids)
work_ids <- normalizza_work_id(work_ids)

if (dedup_input_ids) {
  work_ids <- unique(work_ids)
}

n_ids <- length(work_ids)

if (n_ids == 0) {
  stop("Nessun ID work valido trovato nel file: ", path_works_ids)
}

rm(works_base)
gc()

n_request_batches <- as.integer(ceiling(n_ids / work_batch_size))
n_chunks <- as.integer(ceiling(n_request_batches / chunk_parallel_size))

chunk_ids <- seq_len(n_chunks)
chunk_first_batch <- ((chunk_ids - 1L) * chunk_parallel_size) + 1L
chunk_last_batch <- pmin(chunk_ids * chunk_parallel_size, n_request_batches)
chunk_first_index <- ((chunk_first_batch - 1L) * work_batch_size) + 1L
chunk_last_index <- pmin(chunk_last_batch * work_batch_size, n_ids)

stato_chunk_base <- data.table(
  chunk_id = as.integer(chunk_ids),
  first_batch = as.integer(chunk_first_batch),
  last_batch = as.integer(chunk_last_batch),
  first_index = as.integer(chunk_first_index),
  last_index = as.integer(chunk_last_index),
  n_batches = as.integer(chunk_last_batch - chunk_first_batch + 1L),
  n_ids = as.integer(chunk_last_index - chunk_first_index + 1L),
  status = "da_fare",
  path_csv = path_works_flat_csv,
  n_returned = NA_integer_,
  n_missing = NA_integer_,
  n_batch_falliti = NA_integer_,
  errore = NA_character_,
  aggiornato_il = NA_character_
)

input_info <- file.info(path_works_ids)
manifest_input <- data.table(
  codice_lavoro = codice_lavoro,
  input_path = normalizePath(path_works_ids, mustWork = FALSE),
  input_size_bytes = as.numeric(input_info$size),
  input_mtime = as.character(input_info$mtime),
  n_input_lordo = as.integer(n_input_lordo),
  n_ids_validi = as.integer(n_ids),
  dedup_input_ids = as.logical(dedup_input_ids),
  work_batch_size = as.integer(work_batch_size),
  works_per_chunk_csv = as.integer(works_per_file),
  chunk_parallel_size = as.integer(chunk_parallel_size),
  non_nested_vars = paste(non_nested_vars, collapse = "|"),
  nested_vars = NA,
  n_request_batches = as.integer(n_request_batches),
  n_chunks = as.integer(n_chunks),
  creato_il = as.character(Sys.time())
)

if (!file.exists(path_input_manifest_csv)) {
  data.table::fwrite(manifest_input, path_input_manifest_csv)
} else {
  manifest_vecchio <- data.table::fread(path_input_manifest_csv)
  campi_check <- c(
    "input_path",
    "input_size_bytes",
    "n_ids_validi",
    "work_batch_size",
    "chunk_parallel_size",
    "non_nested_vars",
    "nested_vars",
    "n_request_batches",
    "n_chunks"
  )
  campi_check <- intersect(campi_check, intersect(names(manifest_vecchio), names(manifest_input)))
  diverso <- FALSE
  for (cc in campi_check) {
    if (!identical(as.character(manifest_vecchio[[cc]][1]), as.character(manifest_input[[cc]][1]))) {
      diverso <- TRUE
    }
  }
  if (diverso) {
    stop(
      "Il manifest input esiste ma non coincide con la configurazione corrente. ",
      "Per cambiare input, work_batch_size o chunk_parallel_size usa un nuovo codice_lavoro ",
      "oppure rimuovi consapevolmente stato, manifest e CSV di stato/output."
    )
  }
}

if (file.exists(path_stato_chunk_csv)) {
  stato_chunk <- data.table::fread(path_stato_chunk_csv)
  stato_chunk <- as.data.table(stato_chunk)
  if (!"chunk_id" %in% names(stato_chunk) || !"status" %in% names(stato_chunk)) {
    stop("Il file di stato chunk esiste ma non contiene chunk_id/status: ", path_stato_chunk_csv)
  }
  stato_chunk[, chunk_id := as.integer(chunk_id)]

  # Integro eventuali chunk mancanti dalla configurazione base.
  missing_chunks <- stato_chunk_base[!chunk_id %in% stato_chunk$chunk_id]
  if (nrow(missing_chunks) > 0) {
    stato_chunk <- data.table::rbindlist(list(stato_chunk, missing_chunks), fill = TRUE)
  }
} else {
  stato_chunk <- copy(stato_chunk_base)
  scrivi_stato_chunk_csv(stato_chunk, path_stato_chunk_csv)
}

chunk_completati <- stato_chunk[status == "completato", unique(as.integer(chunk_id))]
chunk_da_fare <- stato_chunk[status != "completato", unique(as.integer(chunk_id))]

cat("ID input lordi:", n_input_lordo, "\n")
cat("ID validi usati:", n_ids, "\n")
cat("Request batch totali:", n_request_batches, "\n")
cat("Chunk totali:", n_chunks, "\n")
cat("Chunk completati:", length(chunk_completati), "\n")
cat("Chunk da fare:", length(chunk_da_fare), "\n")

# =============================================================================
# 7. CLUSTER PARALLELO CON PROXY
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
    "work_ids",
    "work_batch_size",
    "campi_select",
    "non_nested_vars",
    "nested_vars",
    "per_page",
    "url_base",
    "url_rate_limit",
    "api_keys",
    "normalizza_work_id",
    "make_filter_works",
    "valore_scalare_csv",
    "collassa_valori_csv",
    "oggetto_vuoto_o_na",
    "campo_o_na",
    "estrai_works_flat",
    "estrai_authorships",
    "formatta_reset_italia",
    "parse_retry_after_seconds",
    "check_rate_limit_worker",
    "get_key_state_env",
    "refresh_key_state",
    "select_available_key",
    "mark_key_unavailable",
    "sleep_until_first_key_available",
    "calcola_wait_429",
    "get_with_key_pool_worker",
    "scarica_work_batch_worker",
    "scarica_work_batch_safe"
  ),
  envir = environment()
)

# =============================================================================
# 8. LOOP PRINCIPALE PER CHUNK
# =============================================================================

inizio_totale <- Sys.time()

if (length(chunk_da_fare) == 0) {
  cat("\nNessun chunk da scaricare.\n")
}

for (chunk_corrente in chunk_da_fare) {
  inizio_chunk <- Sys.time()
  riga_chunk <- stato_chunk_base[chunk_id == chunk_corrente]

  batch_chunk <- seq.int(
    from = as.integer(riga_chunk$first_batch),
    to = as.integer(riga_chunk$last_batch)
  )

  cat(
    "\n=== ",
    codice_lavoro,
    " | Chunk ", chunk_corrente, "/", n_chunks,
    " | Batch ", min(batch_chunk), "-", max(batch_chunk),
    " | N batch: ", length(batch_chunk),
    " | Index ", riga_chunk$first_index, "-", riga_chunk$last_index,
    " | N ID: ", riga_chunk$n_ids,
    " ===\n",
    sep = ""
  )

  risultati <- parallel::parLapplyLB(
    cl,
    batch_chunk,
    scarica_work_batch_safe,
    work_ids = work_ids,
    work_batch_size = work_batch_size,
    campi = campi_select,
    non_nested_vars = non_nested_vars,
    nested_vars = nested_vars,
    per_page = per_page,
    url_base = url_base,
    url_rate_limit = url_rate_limit,
    api_keys = api_keys
  )

  ok <- vapply(risultati, function(x) isTRUE(x$ok), logical(1))

  risultati_ok <- risultati[ok]
  risultati_fail <- risultati[!ok]

  n_batch_falliti <- length(risultati_fail)

  if (n_batch_falliti > 0) {
    falliti <- data.table(
      chunk_id = as.integer(chunk_corrente),
      global_batch_id = vapply(risultati_fail, function(x) as.integer(x$global_batch_id), integer(1)),
      first_index = ((vapply(risultati_fail, function(x) as.integer(x$global_batch_id), integer(1)) - 1L) * work_batch_size) + 1L,
      last_index = pmin(vapply(risultati_fail, function(x) as.integer(x$global_batch_id), integer(1)) * work_batch_size, n_ids),
      errore = vapply(risultati_fail, function(x) as.character(x$errore), character(1)),
      aggiornato_il = as.character(Sys.time())
    )

    safe_fwrite_append(falliti, path_batch_falliti_csv)

    riga_fail <- data.table(
      chunk_id = as.integer(chunk_corrente),
      first_batch = as.integer(riga_chunk$first_batch),
      last_batch = as.integer(riga_chunk$last_batch),
      first_index = as.integer(riga_chunk$first_index),
      last_index = as.integer(riga_chunk$last_index),
      n_batches = as.integer(riga_chunk$n_batches),
      n_ids = as.integer(riga_chunk$n_ids),
      status = "fallito",
      path_csv = path_works_flat_csv,
      n_returned = NA_integer_,
      n_missing = NA_integer_,
      n_batch_falliti = as.integer(n_batch_falliti),
      errore = paste(unique(falliti$errore), collapse = " || "),
      aggiornato_il = as.character(Sys.time())
    )

    stato_chunk <- aggiorna_stato_chunk(stato_chunk, riga_fail)
    scrivi_stato_chunk_csv(stato_chunk, path_stato_chunk_csv)

    cat("\nERRORE in uno o più request-batch del chunk corrente:\n")
    print(falliti)

    rm(risultati, risultati_ok, risultati_fail)
    gc()

    if (stop_on_failed_batch) {
      stop(
        "Download interrotto: almeno un request-batch è fallito. ",
        "I batch falliti sono stati registrati in: ", path_batch_falliti_csv,
        ". Il chunk verrà ritentato al prossimo run."
      )
    }
  }

  dati_flat_chunk <- lapply(risultati_ok, function(x) x$flat_data)

  if (length(dati_flat_chunk) == 0) {
    df_flat_chunk <- data.table()
  } else {
    df_flat_chunk <- data.table::rbindlist(dati_flat_chunk, fill = TRUE)
  }

  if (nrow(df_flat_chunk) > 0 && "id_work" %in% names(df_flat_chunk)) {
    id_norm_chunk <- normalizza_work_id(df_flat_chunk[["id_work"]])
    df_flat_chunk <- df_flat_chunk[!duplicated(id_norm_chunk)]
  }


  missing_list <- lapply(risultati_ok, function(x) x$missing_ids)
  missing_ids <- unique(unlist(missing_list, use.names = FALSE))

  if (length(missing_ids) > 0) {
    missing_dt <- data.table(
      chunk_id = as.integer(chunk_corrente),
      work_id = missing_ids,
      aggiornato_il = as.character(Sys.time())
    )
    safe_fwrite_append(missing_dt, path_missing_ids_csv)
  }

  n_returned_chunk <- as.integer(nrow(df_flat_chunk))
  n_missing_chunk <- as.integer(length(missing_ids))

  # Checkpoint incrementale: il CSV viene aggiornato solo dopo che tutti i batch
  # del chunk corrente sono andati a buon fine.
  safe_fwrite_append(df_flat_chunk, path_works_flat_csv)

  fine_chunk <- Sys.time()
  tempo_chunk <- round(difftime(fine_chunk, inizio_chunk, units = "mins"), 2)

  riga_ok <- data.table(
    chunk_id = as.integer(chunk_corrente),
    first_batch = as.integer(riga_chunk$first_batch),
    last_batch = as.integer(riga_chunk$last_batch),
    first_index = as.integer(riga_chunk$first_index),
    last_index = as.integer(riga_chunk$last_index),
    n_batches = as.integer(riga_chunk$n_batches),
    n_ids = as.integer(riga_chunk$n_ids),
    status = "completato",
    path_csv = path_works_flat_csv,
    n_returned = as.integer(n_returned_chunk),
    n_missing = as.integer(n_missing_chunk),
    n_batch_falliti = as.integer(0L),
    errore = NA_character_,
    aggiornato_il = as.character(Sys.time())
  )

  stato_chunk <- aggiorna_stato_chunk(stato_chunk, riga_ok)
  scrivi_stato_chunk_csv(stato_chunk, path_stato_chunk_csv)

  chunk_completati <- stato_chunk[status == "completato", unique(as.integer(chunk_id))]
  chunk_falliti <- stato_chunk[status == "fallito", unique(as.integer(chunk_id))]

  cat("  Works richiesti nel chunk:", riga_chunk$n_ids, "\n")
  cat("  Works restituiti nel chunk:", n_returned_chunk, "\n")
  cat("  Works mancanti nel chunk:", n_missing_chunk, "\n")
  cat("  Batch falliti nel chunk:", n_batch_falliti, "\n")
  cat("  CSV works flat:", path_works_flat_csv, "\n")
  cat("  Chunk completati:", length(chunk_completati), "/", n_chunks, "\n")
  cat("  Chunk falliti:", length(chunk_falliti), "\n")
  cat("  Tempo chunk:", tempo_chunk, "minuti\n")

  rm(
    risultati,
    risultati_ok,
    risultati_fail,
    dati_flat_chunk,
    df_flat_chunk,
    missing_list,
    missing_ids
  )
  gc()
}

fine_totale <- Sys.time()

parallel::stopCluster(cl)
cl <- NULL

# =============================================================================
# 9. RIEPILOGO FINALE
# =============================================================================

scrivi_stato_chunk_csv(stato_chunk, path_stato_chunk_csv)

cat("\n=== DOWNLOAD COMPLETATO O PORTATO ALLO STATO CORRENTE:", codice_lavoro, "===\n")
cat("Chunk totali:", n_chunks, "\n")
cat("Chunk completati:", length(stato_chunk[status == 'completato', unique(chunk_id)]), "\n")
cat("Chunk falliti:", length(stato_chunk[status == 'fallito', unique(chunk_id)]), "\n")
cat("Stato chunk CSV:", path_stato_chunk_csv, "\n")
cat("Batch falliti CSV:", path_batch_falliti_csv, "\n")
cat("Missing IDs CSV:", path_missing_ids_csv, "\n")
cat("Works flat CSV:", path_works_flat_csv, "\n")
cat(
  "Tempo totale:",
  round(difftime(fine_totale, inizio_totale, units = "mins"), 2),
  "minuti\n"
)
