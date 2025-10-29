# MA 2A Mapping Script — Technical Documentation

**Purpose:** Automate retrieval of the Massachusetts “Licensed or Approved Companies” list from Mass.gov, archive the raw file, standardize and filter the data for Property & Casualty carriers, then match those companies against RMV carrier names. The result is published to a SQL Server mapping table used by the MA 2A Form workflow.

---
## 1) High‑Level Flow

1. **Connect to SQL Server** (Windows or SQL auth via `pyodbc`).
2. **Scrape Mass.gov index page** to find the latest *“Massachusetts Licensed Or Approved Companies.xls”* link.
3. **Download** the Excel (XLS/XLSX) file and **archive** it to a network folder with a date‑stamped filename.
4. **Parse** the worksheet, detect the header row, normalize columns, and **clean** the data (types, ZIP, NAIC, lengths).
5. **Filter** to **Property & Casualty** companies only.
6. **Load RMV carrier names** from `EnterpriseServices.[dbo].[RMV_CARRIER_NAME]`.
7. **Match in two passes**:
   - **Pass 1:** Exact string match on raw names.
   - **Pass 2:** Apply **hardcoded overrides**, then **normalize** company names and match on normalized strings.
8. **Combine** the matches (favor Pass 1 when duplicates occur), add `update_dt`.
9. **Recreate** (drop & create) and **insert** into `[dbo].[MA_2A_Form_Mapping]`.
10. **Log** progress and **close** the connection.

---
## 2) Runtime Dependencies

Install with pip (notebook/venv/agent):

```bash
pip install pandas requests pyodbc beautifulsoup4 lxml openpyxl xlrd==1.2.0 python-dateutil
```

> **Note:** `xlrd==1.2.0` is required for legacy `.xls` support. `.xlsx` is handled by `openpyxl`.

---
## 3) Configuration (ENV + Defaults)

The script reads environment variables with sensible defaults:

| Variable | Default | Meaning |
|---|---|---|
| `ARCHIVE_FOLDER` | `\\njredbf2001\ProductManagement\Product\Auto\MASSACHUSETTS\Operational Processes\2A Form\MA Gov Company Address List` | Where the raw XLS/XLSX is saved daily with a date stamp |
| `SQL_SERVER` | `AE1SQLWPV20` | Target SQL Server for writes |
| `SQL_DATABASE` | `Iwan` | Target database |
| `SQL_SCHEMA` | `dbo` | Schema for `MA_2A_Form_Mapping` |
| `ODBC_DRIVER` | `ODBC Driver 17 for SQL Server` | ODBC driver name |
| `TRUSTED_CONN` | `1` | Use Windows Auth if `1`, otherwise provide `SQL_USER`/`SQL_PASSWORD` |
| `SQL_USER` / `SQL_PASSWORD` | *(none)* | Used only when `TRUSTED_CONN` is `0` |

Additional constants:

- `TARGET_PAGE`: Mass.gov index page to scrape
- `XLS_NAME_PATTERN`: case‑insensitive regex targeting the official company list filename
- `SQL_MAPPING_TABLE`: output table name (`MA_2A_Form_Mapping`)
- `RMV_SOURCE_DB`: `CO1SQLWPV10_EnterpriseServices`
- `RMV_SOURCE_TABLE`: `EnterpriseServices.[dbo].[RMV_CARRIER_NAME]`

**Example .env (PowerShell):**
```powershell
$env:ARCHIVE_FOLDER="\\\\njredbf2001\\ProductManagement\\Product\\Auto\\MASSACHUSETTS\\Operational Processes\\2A Form\\MA Gov Company Address List"
$env:SQL_SERVER="AE1SQLWPV20"
$env:SQL_DATABASE="Iwan"
$env:SQL_SCHEMA="dbo"
$env:TRUSTED_CONN="1"
$env:ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

---
## 4) Key Functions & Responsibilities

### 4.1 Logging
`logging.basicConfig(...)` configures INFO‑level logs to stdout. Use the logger name `MA_RMV_Mapping` to filter in job runners.

### 4.2 Name Normalization
`normalize_name(s)` mirrors the SQL `dbo.NormalizeInsName` logic:
- Uppercases, converts `&` → `AND`, strips punctuation, collapses whitespace.
- Removes leading `THE`.
- Iteratively trims trailing **stopwords** (e.g., `INC`, `LLC`, `INSURANCE`, `ASSURANCE`, `CORP`, etc.).
- Returns a compact, comparable string (or `None`).

This enables fuzzy‑ish exact matching without a full fuzzy library.

### 4.3 SQL Connection
`get_sql_connection()` builds an ODBC connection string based on Windows or SQL auth. Autocommit is enabled.

### 4.4 Mass.gov Scrape & Download
- `find_xls_url()` fetches `TARGET_PAGE`, parses with BeautifulSoup (`lxml` parser), and locates the first link whose **text or href** matches `XLS_NAME_PATTERN`. It canonicalizes `//` and `/` links to absolute HTTPS.
- `download_file(url)` streams the file bytes.
- `is_xlsx(b)` checks ZIP header for OOXML (`b"PK\x03\x04"`).

### 4.5 Excel Parsing & Cleaning
- `read_update_date_from_b4(bytes)` attempts to read cell **B4** of the first worksheet as a `date` (works for both `.xlsx` and legacy `.xls`). Fallbacks to `dateutil.parser` on free text. Non‑fatal on failure.
- `detect_header_row(df_raw)` scans first 40 rows looking for a row resembling headers (≥4 of: Company Type, NAIC #, Company, Address, City, State, Zip, Phone).
- `load_table_dataframe(bytes)` reads the sheet twice: once no‑header to detect `hdr_idx`, then again with `header=hdr_idx`. Column names are normalized to snake_case and pruned to a known set.
- `clean_and_trim(df)` standardizes strings; extracts `state` (2‑letter), formats `zip` (5 or 9 w/ hyphen), **extracts digits** from `naic`, enforces **max lengths** to avoid SQL truncation, and replaces null‑likes with `None`.

### 4.6 RMV Data
`get_rmv_data(conn)` pulls distinct `CARRIER_NAME` values from `EnterpriseServices.[dbo].[RMV_CARRIER_NAME]`.

### 4.7 Hardcoded Overrides (Before Normalization)
`apply_hardcoded_matches(df_rmv)` sets an `rmv_match_target` column, then:
- Pattern rule: `...(Pilgrim)` → `Pilgrim Insurance Company`.
- A finite map of exact replacements (e.g., PURE expansion, Farmers/Metropolitan rename, Electric → Plymouth Rock Assurance Corporation, etc.).

These stabilize normalization for known edge cases.

### 4.8 Two‑Pass Matching
- **Pass 1 (Raw Exact):** `CARRIER_NAME` (RMV) vs `company` (Mass.gov) exact merge.
- **Pass 2 (Normalized):** Remaining RMV names → apply overrides → normalize both sides → exact merge on `normalized_name`.

Results are concatenated with Pass 1 first so that `drop_duplicates(keep='first')` **prefers Pass 1** when the same RMV name matched in both.

### 4.9 Output Table Rebuild & Insert
- `recreate_mapping_table(conn)` drops and recreates `[dbo].[MA_2A_Form_Mapping]` with columns: `rmv_name, mass_gov_name, address, city, state, zip, phone, update_dt`.
- `insert_mapping_dataframe(conn, df)` uses `fast_executemany` parameterized inserts for performance and safety.

---
## 5) Output Schema

```sql
CREATE TABLE [dbo].[MA_2A_Form_Mapping](
  rmv_name      VARCHAR(255) NULL,
  mass_gov_name VARCHAR(255) NULL,
  address       VARCHAR(255) NULL,
  city          VARCHAR(120) NULL,
  state         VARCHAR(10)  NULL,
  zip           VARCHAR(20)  NULL,
  phone         VARCHAR(40)  NULL,
  update_dt     DATE         NULL
);
```

- **`rmv_name`**: Original name from RMV source table.
- **`mass_gov_name`**: Matched company name from Mass.gov list.
- **Address fields / phone**: From Mass.gov, cleaned and length‑bounded.
- **`update_dt`**: Script run date, not necessarily the Mass.gov refresh date. (B4 date is logged, not stored.)

---
## 6) Operational Guidance

### 6.1 Scheduling
- Run once daily after Mass.gov is updated (typical business hours).
- Windows Task Scheduler or SQL Agent (via Python step) recommended.

**Windows Task Scheduler tip:**
- Program: `python`
- Args: `path\to\script.py`
- Start in: working directory containing your virtual environment (ensure the ODBC driver is installed on the host).

### 6.2 Permissions Required
- Read access to `CO1SQLWPV10_EnterpriseServices.EnterpriseServices.[dbo].[RMV_CARRIER_NAME]` via linked‑server or direct ODBC route as configured.
- Write DDL/DML on `AE1SQLWPV20.Iwan.dbo` (drop/create/insert on `MA_2A_Form_Mapping`).
- File share write permissions to `ARCHIVE_FOLDER`.

### 6.3 Archiving Convention
- Filename: `MA_Licensed_Companies_YYYYMMDD.xlsx|.xls` depending on source format.
- The archive preserves the **raw** download for traceability and audits.

### 6.4 Logging & Observability
- INFO logs describe each phase and record counts.
- Exceptions are logged with stack trace and a non‑zero exit code.

---
## 7) Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `Could not find 'Massachusetts Licensed Or Approved Companies.xls' link` | Mass.gov changed the link text/path | Update `XLS_NAME_PATTERN` or broaden search logic to alternative titles; verify `TARGET_PAGE` |
| `xlrd` read error for `.xls` | Wrong `xlrd` version | Pin `xlrd==1.2.0` |
| `openpyxl` or `lxml` missing | Dependencies not installed | Reinstall deps; ensure the job uses the correct Python env |
| ZIP header mismatch on `.xlsx` | File is actually `.xls` or HTML/intercept | Let `is_xlsx()` decide; inspect archived file to confirm |
| SQL auth failure | Windows vs SQL auth mismatch | Set `TRUSTED_CONN` properly; if `0`, provide `SQL_USER`/`SQL_PASSWORD` |
| Permission denied on archive path | Share ACLs | Request write access to the network folder |
| Table not found / DROP fails | Schema/db mismatch | Confirm `SQL_SCHEMA`/`SQL_DATABASE`; check job’s default DB |
| No P&C rows after filter | Column rename/format changed | Inspect headers in the raw; adjust `filter_mask` string |

---
## 8) Extensibility Notes

- **Storing Mass.gov update date:** Persist `read_update_date_from_b4()` result to a metadata table for provenance.
- **Fuzzy matching:** If normalized exact match is insufficient, add a third pass using token set ratio / trigram similarity with thresholds.
- **Incremental loads:** Instead of drop‑and‑create, consider MERGE to preserve history.
- **Audit table:** Record run timestamp, source file name, row counts, and match rates.
- **Alerting:** Send an email/Teams message if zero matches or Mass.gov link not found.

---
## 9) Quick Start

1. Create/activate a Python environment and install **Dependencies** (Section 2).
2. Set **Environment Variables** (Section 3) or accept defaults.
3. Ensure:
   - ODBC Driver 17 is installed on the host.
   - SQL permissions & share permissions are granted.
4. Run:
   ```bash
   python ma_rmv_mapping.py
   ```
5. Verify:
   - Archive folder contains today’s raw file.
   - Logs show Pass 1/Pass 2 counts and final insert count.
   - Table `[dbo].[MA_2A_Form_Mapping]` is populated.

---
## 10) Function Reference (Alphabetical)

- **`apply_hardcoded_matches(df_rmv)`** — add `rmv_match_target` with known corrections before normalization.
- **`clean_and_trim(df)`** — standardize strings, extract state/ZIP/NAIC, enforce max lengths, null handling.
- **`detect_header_row(df_raw)`** — heuristically find header row (≥4 expected column names within top 40 rows).
- **`download_file(url)`** — HTTP GET with 120s timeout, returns bytes.
- **`find_xls_url()`** — scrape Mass.gov page for the current company list link.
- **`get_rmv_data(conn)`** — read unique `CARRIER_NAME` from RMV table.
- **`get_sql_connection()`** — build and open a pyodbc connection (autocommit).
- **`insert_mapping_dataframe(conn, df)`** — parameterized bulk insert with `fast_executemany`.
- **`is_xlsx(bytes)`** — check if content is OOXML zip.
- **`load_table_dataframe(bytes)`** — load worksheet with detected header, normalize columns, select relevant fields.
- **`normalize_name(s)`** — strip punctuation/stopwords, canonicalize for exact‑on‑normalized matches.
- **`read_update_date_from_b4(bytes)`** — best‑effort parse of B4 cell into a `date`.
- **`recreate_mapping_table(conn)`** — drop & create final output table.

---
## 11) Safety & Compliance Considerations

- **Read‑only web access** to a public government site; no scraping of personal data.
- **Least privilege** on SQL (grant only what the job needs: DROP/CREATE/INSERT for the output table; SELECT on RMV source table).
- **Network share** should be treated as an audit trail; do not overwrite historical raw files.

---
## 12) Appendix — Key Constants

```python
TARGET_PAGE = "https://www.mass.gov/lists/massachusetts-licensed-insurance-companies"
XLS_NAME_PATTERN = r"Massachusetts\s+Licensed\s+Or\s+Approved\s+Companies\.xls"
SQL_MAPPING_TABLE = "MA_2A_Form_Mapping"
RMV_SOURCE_DB = "CO1SQLWPV10_EnterpriseServices"
RMV_SOURCE_TABLE = "EnterpriseServices.[dbo].[RMV_CARRIER_NAME]"
```

*Document owner:* Product Analytics — Telematics

