# CLAUDE.md - AI Assistant Guide for DataPulse

**Last Updated:** 2025-11-17
**Project:** DataPulse - Financial Data Search Engine
**License:** MIT License (Copyright 2025 Jason Zheng)
**Purpose:** RPI Course Project for research purposes

---

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture Summary](#architecture-summary)
- [Directory Structure](#directory-structure)
- [Technology Stack](#technology-stack)
- [Development Workflows](#development-workflows)
- [Code Conventions](#code-conventions)
- [Key Components](#key-components)
- [Data Flow](#data-flow)
- [Common Tasks](#common-tasks)
- [Important Constraints](#important-constraints)
- [Testing Strategy](#testing-strategy)
- [Troubleshooting](#troubleshooting)

---

## Project Overview

DataPulse is a financial data search engine designed to efficiently query and visualize large-scale stock market data (138GB+). The system uses direct CSV processing without a database, leveraging modern data processing libraries for performance.

### Core Capabilities
- Query stock data by symbol, time interval, date range, and metrics
- Visualize data via interactive charts (Recharts) or sortable tables
- Export filtered results to CSV
- Support multiple time intervals: 1 minute, 5 minutes, 15 minutes, 1 hour, 1 day, 1 week
- Handle large datasets efficiently using PyArrow and Pandas

### Target Users
- Researchers working with financial data
- Students in data analysis and finance courses
- Developers building financial analysis tools

---

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────┐
│                    DataPulse Architecture                    │
└─────────────────────────────────────────────────────────────┘

Frontend (React @ :3000)
    ↓ HTTP Proxy
Flask API (:5000) → Manifest.json → CSV Files (FirstData/)
    ↑
PyArrow/Pandas (Fast CSV reading)

ETL Pipeline (Optional):
Raw CSV → etl_ohlvc.py → Normalized Parquet → Rollups
```

### Key Design Decisions
1. **No Database:** Direct CSV processing with manifest-based indexing
2. **Proxy-based Communication:** React dev server proxies API calls to Flask
3. **Manifest Pattern:** `manifest.json` indexes all data files for fast lookup
4. **Monolithic Components:** Frontend uses single-component design for simplicity
5. **Pre-adjusted Data:** All stock data is split/dividend-adjusted

---

## Directory Structure

```
DataPulse/
├── stock-search-app/              # React frontend (Create React App)
│   ├── public/                    # Static assets
│   │   ├── index.html
│   │   ├── manifest.json
│   │   ├── favicon.ico
│   │   └── robots.txt
│   ├── src/
│   │   ├── StockSearchEngine.jsx  # MAIN COMPONENT (359 lines)
│   │   ├── App.js                 # Root wrapper
│   │   ├── App.css                # Global styles
│   │   ├── App.test.js            # Placeholder test
│   │   ├── index.js               # Entry point
│   │   ├── index.css              # Tailwind directives
│   │   ├── setupTests.js          # Jest configuration
│   │   └── reportWebVitals.js     # Performance metrics
│   ├── package.json               # Dependencies + proxy config
│   ├── tailwind.config.js         # Tailwind CSS setup
│   ├── postcss.config.js          # PostCSS configuration
│   └── .env                       # Dev server config
│
├── backend/
│   ├── etl/
│   │   ├── 0_ingest.py           # Creates manifest.json from data files
│   │   ├── query_api.py          # Flask API (124 lines)
│   │   ├── requirements.txt      # Python dependencies
│   │   ├── etl_tmp/              # Generated manifest location (not in git)
│   │   │   └── manifest.json     # File index
│   │   ├── FirstData/            # Raw data directory (not in git)
│   │   │   ├── 1hr/
│   │   │   └── 5min/
│   │   └── venv/                 # Virtual environment (not in git)
│   ├── plan.txt                  # Architecture documentation
│   └── .gitignore
│
├── Root-level Python Scripts (legacy/utilities):
│   ├── main.py                   # CSV file finder (32 lines)
│   ├── parser.py                 # PyArrow CSV parser (90 lines)
│   ├── etl_ohlvc.py              # Polars-based normalization
│   └── Prop-mockup/              # Design mockups
│
├── .gitignore                    # Root-level ignore rules
├── README.md                     # User documentation
├── LICENSE                       # MIT License
└── CLAUDE.md                     # This file
```

### Important Paths Reference
- Frontend entry: `stock-search-app/src/StockSearchEngine.jsx`
- Backend API: `backend/etl/query_api.py`
- Data ingest: `backend/etl/0_ingest.py`
- Manifest: `backend/etl/etl_tmp/manifest.json` (generated)
- Data files: `backend/etl/FirstData/{interval}/Historical_stock_full_A_{interval}_adj_splitdiv/{SYMBOL}_full_{interval}_adjsplitdiv.txt`

---

## Technology Stack

### Frontend
| Technology | Version | Purpose |
|------------|---------|---------|
| React | 18.3.1 | UI framework |
| Recharts | 3.2.1 | Interactive charts |
| Tailwind CSS | 3.4.17 | Utility-first styling |
| Lucide React | 0.544.0 | Icon library |
| Create React App | Latest | Build tooling |
| Jest + RTL | Latest | Testing framework |

### Backend
| Technology | Version | Purpose |
|------------|---------|---------|
| Flask | 2.3.3 | API framework |
| Flask-CORS | 4.0.0 | Cross-origin requests |
| Pandas | 2.0.3 | Data manipulation |
| PyArrow | 12.0.1 | Fast CSV parsing (10x faster) |
| Dask | 2023.9.3 | Parallel processing |
| Polars | Latest | ETL transformations |
| Pytest | 7.4.3 | Testing framework |

### Development Tools
- **Python:** 3.10 (recommended for compatibility)
- **Node.js:** v14+ required
- **npm:** v6+ required
- **Version Control:** Git

---

## Development Workflows

### Initial Setup

#### Backend Setup
```bash
# Navigate to backend directory
cd backend/etl

# Create virtual environment (Python 3.10 recommended)
python3.10 -m venv venv

# Activate virtual environment
# Linux/Mac:
source venv/bin/activate
# Windows:
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Create data directories (if not exists)
mkdir -p FirstData/1hr/Historical_stock_full_A_1hour_adj_splitdiv
mkdir -p FirstData/5min/Historical_stock_full_A_5min_adj_splitdiv

# Create manifest directory
mkdir -p etl_tmp

# Run ingest script to create manifest.json
python 0_ingest.py

# Start Flask API
python query_api.py
# Server runs at http://127.0.0.1:5000
```

#### Frontend Setup
```bash
# Navigate to frontend directory
cd stock-search-app

# Install dependencies
npm install

# Start development server
npm start
# Application opens at http://localhost:3000
```

### Running the Application

**Development Mode:**
1. Start Flask backend in terminal 1: `cd backend/etl && python query_api.py`
2. Start React frontend in terminal 2: `cd stock-search-app && npm start`
3. Access application at `http://localhost:3000`

**Production Build:**
```bash
cd stock-search-app
npm run build
# Serve the build/ directory with a web server
```

### Testing

**Frontend:**
```bash
cd stock-search-app
npm test                    # Run Jest tests in watch mode
npm test -- --coverage      # Generate coverage report
```

**Backend:**
```bash
cd backend/etl
source venv/bin/activate
pytest                      # Run all tests
pytest --cov=.              # With coverage
```

### Data Management

**Update Data Files:**
1. Place new CSV files in `backend/etl/FirstData/{interval}/Historical_stock_full_A_{interval}_adj_splitdiv/`
2. Re-run ingest: `python backend/etl/0_ingest.py`
3. Restart Flask API

**File Naming Convention:**
- Pattern: `{SYMBOL}_full_{interval}_adjsplitdiv.txt`
- Examples:
  - `AAPL_full_1hour_adjsplitdiv.txt`
  - `GOOGL_full_5min_adjsplitdiv.txt`

---

## Code Conventions

### Naming Conventions

**Frontend (React/JavaScript):**
- Components: `PascalCase` (e.g., `StockSearchEngine`, `App`)
- Variables: `camelCase` (e.g., `selectedCompanies`, `dateFrom`)
- Constants: `UPPER_SNAKE_CASE` (e.g., `INTERVALS`, `METRICS`)
- Files: `PascalCase.jsx` for components, `camelCase.js` for utilities

**Backend (Python):**
- Variables: `snake_case` (e.g., `stock_data`, `date_from`)
- Functions: `snake_case` (e.g., `find_file`, `parse_csv`)
- Classes: `PascalCase` (e.g., `DataProcessor`)
- Files: `snake_case.py` (e.g., `query_api.py`, `0_ingest.py`)

### Code Style

**Frontend:**
- Use functional components with hooks (no class components)
- Use `useState` for state management
- Inline event handlers are acceptable
- Tailwind utility classes for styling (no CSS modules)
- Conditional rendering with `&&` operator
- Array operations: `map`, `filter`, `includes`

**Backend:**
- Type hints encouraged: `def parse(file_path: str, start_time: Optional[str]) -> pa.Table`
- Use `request.args.get()` and `request.args.getlist()` for parameters
- Return JSON with `jsonify()` or direct dictionaries
- Manual CORS headers in `after_request` decorator
- Use `pathlib.Path` for path operations

### File Organization

**Frontend:**
- Monolithic component design (single large component per feature)
- State managed at component level with `useState`
- No global state management (Redux, Context API)
- Hardcoded configuration within components

**Backend:**
- One route handler per endpoint
- Separate concerns: ingest, query, ETL
- Manifest-based file discovery pattern
- No ORM or database models

---

## Key Components

### Frontend: StockSearchEngine.jsx

**Location:** `stock-search-app/src/StockSearchEngine.jsx` (359 lines)

**Responsibilities:**
- Company search with tag filtering
- Filter controls (interval, date range, metrics)
- API communication with Flask backend
- Data visualization (chart and table views)
- CSV export functionality

**State Variables:**
```javascript
searchQuery         // string: company search input
selectedCompanies   // array: chosen stock symbols
interval            // string: "1hr", "5min", "1d", "1w"
dateFrom            // string: start date (YYYY-MM-DD)
dateTo              // string: end date (YYYY-MM-DD)
selectedMetrics     // array: ["open", "high", "low", "close", "volume"]
viewMode            // string: "chart" or "table"
showResults         // boolean: display results section
stockData           // array: API response data
loading             // boolean: API request in progress
error               // string: error messages
```

**Key Functions:**
- `handleSearch()`: Calls Flask API with selected filters
- `handleDownloadCSV()`: Exports data to CSV file
- Company filtering logic with tags and search

**API Integration:**
```javascript
// Endpoint: /api/query_stock
// Proxy configured in package.json: "http://127.0.0.1:5000"
// Query parameters: symbols[], interval, date_from, date_to, metrics[]
```

### Backend: query_api.py

**Location:** `backend/etl/query_api.py` (124 lines)

**Endpoints:**

1. **GET /api/test**
   - Purpose: Proxy verification
   - Returns: `{"status": "proxy works"}`

2. **GET /api/query_stock**
   - Purpose: Main data query endpoint
   - Parameters:
     - `symbols[]` (array): Stock tickers (e.g., ["AAPL", "GOOGL"])
     - `interval` (string): "1hr", "5min", "1d", "1w"
     - `date_from` (string): Start date (YYYY-MM-DD)
     - `date_to` (string): End date (YYYY-MM-DD)
     - `metrics[]` (array): ["open", "high", "low", "close", "volume"]
   - Returns: JSON array of stock data (max 500 rows per symbol)
   - Error: 404 if no data found

3. **GET /**
   - Purpose: Test HTML form for manual API testing
   - Returns: Interactive form interface

**Data Processing Logic:**
```python
1. Parse request parameters
2. Load manifest.json (file index)
3. Filter manifest for symbol + interval match
4. For each symbol:
   a. Read CSV file with Pandas
   b. Filter rows by date range (date_from to date_to)
   c. Select requested metrics
   d. Limit to 500 rows
5. Combine results from all symbols
6. Return JSON response
```

### Backend: 0_ingest.py

**Location:** `backend/etl/0_ingest.py`

**Purpose:** Create manifest.json index of all data files

**Process:**
1. Scan `FirstData/` directory recursively
2. Extract ZIP archives to `etl_tmp/{interval}/{archive_name}/`
3. Index all CSV/TXT files
4. Create `manifest.json` with:
   - File paths
   - File sizes
   - Interval type (1hr, 5min, etc.)
   - Archive source
   - Filename
   - Creation timestamp

**CLI Arguments:**
```bash
python 0_ingest.py [--data-root /path/to/data]
```

**Fallback Paths:**
1. `--data-root` argument
2. `FIRSTDATA_PATH` environment variable
3. `{repo_root}/backend/etl/FirstData/`
4. `{cwd}/FirstData/`

### Root-level Scripts (Legacy/Utilities)

#### main.py
**Purpose:** Find CSV file path and apply date filtering
**Usage:** `path = find_file("AAPL", "1hour")`
**Note:** Legacy script, replaced by manifest-based approach in query_api.py

#### parser.py
**Purpose:** PyArrow-based CSV parser with temporal filtering
**Key Function:** `parse(file_path, start_time, end_time) -> pa.Table`
**Performance:** 10x faster than standard CSV reading
**Schema:**
```python
timestamp: pa.timestamp("s")
open, high, low, close: pa.float64()
volume: pa.float64()
```

#### etl_ohlvc.py
**Purpose:** Normalize raw CSV to Parquet format with validation
**Technology:** Polars (alternative to Pandas)
**Features:**
- OHLC validation (low ≤ open/close, high ≥ open/close)
- Duplicate removal with aggregation
- Zstandard compression
- Output schema: `[symbol, ts, date, open, high, low, close, volume]`

---

## Data Flow

### Request → Response Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    User Interaction                          │
│  1. Select: symbols, interval, date range, metrics           │
│  2. Click "Search Data"                                      │
└────────────────────┬─────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────┐
│              React Frontend (StockSearchEngine)              │
│  1. Construct query string:                                  │
│     /api/query_stock?symbols[]=AAPL&symbols[]=GOOGL&         │
│     interval=1hr&date_from=2005-01-01&date_to=2005-01-10&   │
│     metrics[]=open&metrics[]=close                           │
│  2. Fetch via proxy (http://127.0.0.1:5000)                 │
└────────────────────┬─────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────┐
│              Flask Backend (query_api.py)                    │
│  1. Parse request.args                                       │
│  2. Load manifest.json (cached in memory)                    │
│  3. Filter manifest: symbol + interval match                 │
│  4. For each symbol:                                         │
│     a. Read CSV with Pandas                                  │
│     b. Filter: df[date_from <= df['time'] <= date_to]       │
│     c. Select columns: df[['time', 'symbol'] + metrics]      │
│     d. Limit: df.head(500)                                   │
│  5. Combine all symbols into single array                    │
└────────────────────┬─────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────┐
│                   JSON Response                              │
│  [                                                           │
│    {time: "2005-01-03 10:00:00", symbol: "AAPL",            │
│     open: 11.89, close: 11.92},                             │
│    {time: "2005-01-03 11:00:00", symbol: "AAPL",            │
│     open: 11.92, close: 11.88},                             │
│    ...                                                       │
│  ]                                                           │
└────────────────────┬─────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────┐
│              React Frontend (Display)                        │
│  1. Store in state: setStockData(response)                   │
│  2. Render:                                                  │
│     - Chart View: Recharts LineChart with multiple lines     │
│     - Table View: Sortable HTML table                        │
│  3. Enable CSV download                                      │
└──────────────────────────────────────────────────────────────┘
```

### Data Lifecycle

```
┌──────────────────────────────────────────────────────────────┐
│            Raw Data (User-provided, not in git)              │
│  FirstData/                                                  │
│  ├── 1hr/Historical_stock_full_A_1hour_adj_splitdiv.zip     │
│  └── 5min/Historical_stock_full_A_5min_adj_splitdiv.zip     │
│                                                              │
│  Files: {SYMBOL}_full_{interval}_adjsplitdiv.txt            │
│  Columns: timestamp, open, high, low, close, volume         │
│  Pre-adjusted for splits and dividends                       │
└────────────────────┬─────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────┐
│                   0_ingest.py                                │
│  1. Scan FirstData/ recursively                              │
│  2. Extract ZIP archives → etl_tmp/{interval}/               │
│  3. Index all files → manifest.json                          │
└────────────────────┬─────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────┐
│              manifest.json (Generated Index)                 │
│  {                                                           │
│    "created_at": "2025-11-17T...",                          │
│    "files": [                                                │
│      {                                                       │
│        "path": "etl_tmp/1hr/.../AAPL_full_1hour_...",       │
│        "size_bytes": 1234567,                                │
│        "interval": "1hr",                                    │
│        "filename": "AAPL_full_1hour_adjsplitdiv.txt"        │
│      }                                                       │
│    ]                                                         │
│  }                                                           │
└────────────────────┬─────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────┐
│              query_api.py (Runtime)                          │
│  1. Load manifest on startup                                 │
│  2. Map symbols → file paths via manifest                    │
│  3. Read CSV with Pandas (on-demand)                         │
│  4. Filter, select, limit                                    │
│  5. Return JSON                                              │
└──────────────────────────────────────────────────────────────┘
```

### Optional ETL Pipeline (Normalization)

```
Raw CSV → etl_ohlvc.py → Normalized Parquet → Future Rollups

Process:
1. Read CSV with Polars (no headers)
2. Assign columns: ts, open, high, low, close, volume
3. Parse timestamp → UTC datetime
4. Validate OHLC relationships
5. Remove duplicates (aggregate by timestamp)
6. Add symbol and date columns
7. Write Parquet with Zstandard compression
```

---

## Common Tasks

### Adding a New Stock Symbol

1. **Add data file:**
   ```bash
   # Place file in appropriate directory
   cp NEWSYMBOL_full_1hour_adjsplitdiv.txt \
      backend/etl/FirstData/1hr/Historical_stock_full_A_1hour_adj_splitdiv/
   ```

2. **Regenerate manifest:**
   ```bash
   cd backend/etl
   python 0_ingest.py
   ```

3. **Restart Flask API:**
   ```bash
   # Kill and restart
   python query_api.py
   ```

4. **Update frontend (optional):**
   - Add to hardcoded company list in `StockSearchEngine.jsx`
   - Or use dynamic company list from API

### Modifying the Frontend UI

**File:** `stock-search-app/src/StockSearchEngine.jsx`

**Example: Add new metric**
```javascript
// 1. Update METRICS constant
const METRICS = [
  { id: 'open', label: 'Open' },
  { id: 'high', label: 'High' },
  { id: 'low', label: 'Low' },
  { id: 'close', label: 'Close' },
  { id: 'volume', label: 'Volume' },
  { id: 'adjclose', label: 'Adj Close' },  // NEW
];

// 2. Ensure backend returns this metric
// 3. Update chart/table rendering logic if needed
```

**Example: Change date range format**
```javascript
// Modify dateFrom/dateTo state handling
// Update API query string construction
// Adjust backend date parsing in query_api.py
```

### Adding a New API Endpoint

**File:** `backend/etl/query_api.py`

```python
@app.route('/api/new_endpoint', methods=['GET'])
def new_endpoint():
    # 1. Parse parameters
    param = request.args.get('param')

    # 2. Process data
    result = process_data(param)

    # 3. Return JSON
    return jsonify(result)

# Note: CORS headers added automatically by after_request decorator
```

### Debugging API Issues

**Enable Flask debug mode:**
```python
# In query_api.py
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
```

**Check proxy configuration:**
```json
// In stock-search-app/package.json
{
  "proxy": "http://127.0.0.1:5000"  // Must match Flask port
}
```

**Test API directly:**
```bash
# Test endpoint
curl "http://127.0.0.1:5000/api/test"

# Test query
curl "http://127.0.0.1:5000/api/query_stock?symbols[]=AAPL&interval=1hr&date_from=2005-01-01&date_to=2005-01-10&metrics[]=close"
```

### Running ETL Normalization

```bash
# Normalize raw data to Parquet
python etl_ohlvc.py

# Verify output
ls -lh *.parquet
```

---

## Important Constraints

### Data Constraints

1. **File Size Limit:** 500 rows per symbol per query (hardcoded in `query_api.py`)
   - Modify in `query_api.py`: Change `.head(500)` to desired limit
   - Consider performance implications for large limits

2. **Date Range:** Limited by available data files
   - Data must exist in FirstData/ directory
   - No automatic data fetching or updates

3. **Supported Intervals:** 1hr, 5min, 1d, 1w
   - Defined by directory structure in FirstData/
   - Must match file naming convention

4. **CSV Format:** Fixed schema expected
   - Columns: timestamp, open, high, low, close, volume
   - Timestamp format: "YYYY-MM-DD HH:MM:SS"
   - No header row in some scripts (check specific script)

### Technical Constraints

1. **Python Version:** 3.10 recommended
   - Dependencies optimized for Python 3.10
   - Other versions may cause compatibility issues

2. **No Database:** Direct CSV processing only
   - No persistent storage besides files
   - No indexing or query optimization
   - Limited to file system performance

3. **Proxy Dependency:** Frontend requires backend running
   - Proxy configured in package.json
   - Frontend cannot run standalone in development

4. **Memory Limits:** Large CSV files loaded into memory
   - Pandas reads entire file per query
   - Consider system RAM for large datasets

5. **Single-threaded:** Flask development server is single-threaded
   - Use production WSGI server (Gunicorn, uWSGI) for production
   - Consider async processing for heavy workloads

### Security Constraints

1. **No Authentication:** API is open
   - No user authentication or authorization
   - No API keys or rate limiting
   - Suitable for local/research use only

2. **CORS Enabled:** All origins allowed
   - Flask-CORS configured for development
   - Restrict origins in production

3. **File Path Exposure:** Manifest exposes file paths
   - Consider security implications in production

---

## Testing Strategy

### Frontend Testing

**Test File:** `stock-search-app/src/App.test.js`

**Current Status:** Placeholder test only

**Recommended Tests:**
- Component rendering
- User interactions (search, filter, select)
- API integration (mock fetch calls)
- CSV export functionality
- Chart rendering
- Table sorting

**Example Test:**
```javascript
import { render, screen, fireEvent } from '@testing-library/react';
import StockSearchEngine from './StockSearchEngine';

test('renders search input', () => {
  render(<StockSearchEngine />);
  const searchInput = screen.getByPlaceholderText(/search companies/i);
  expect(searchInput).toBeInTheDocument();
});

test('handles company selection', () => {
  render(<StockSearchEngine />);
  // Test company selection logic
});
```

### Backend Testing

**Framework:** Pytest

**Recommended Tests:**
- API endpoint responses
- Manifest loading
- CSV parsing
- Date filtering logic
- Error handling (file not found, invalid dates)
- CORS headers

**Example Test:**
```python
import pytest
from query_api import app

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_api_test_endpoint(client):
    response = client.get('/api/test')
    assert response.status_code == 200
    assert response.json == {"status": "proxy works"}

def test_query_stock_endpoint(client):
    response = client.get('/api/query_stock?symbols[]=AAPL&interval=1hr&date_from=2005-01-01&date_to=2005-01-10&metrics[]=close')
    # Assert response structure and data
```

### Manual Testing

**Frontend:**
1. Search for companies (test search functionality)
2. Select multiple companies
3. Change intervals, date ranges
4. Toggle metrics (open, high, low, close, volume)
5. Switch between chart and table views
6. Download CSV export
7. Test responsive design (mobile, tablet, desktop)

**Backend:**
1. Test `/api/test` endpoint
2. Test `/api/query_stock` with various parameters
3. Test error cases (invalid symbols, dates)
4. Test manifest regeneration
5. Test with large date ranges

---

## Troubleshooting

### Frontend Issues

**Problem: "Proxy error" or "Cannot connect to backend"**
- **Solution:** Ensure Flask API is running at `http://127.0.0.1:5000`
- **Check:** `package.json` proxy configuration matches Flask port
- **Verify:** `curl http://127.0.0.1:5000/api/test` returns `{"status": "proxy works"}`

**Problem: "No data found" message**
- **Solution:** Check manifest.json includes requested symbols/intervals
- **Regenerate:** `cd backend/etl && python 0_ingest.py`
- **Verify:** Data files exist in FirstData/ directory

**Problem: Chart not rendering**
- **Solution:** Check browser console for JavaScript errors
- **Verify:** stockData state is populated with valid data
- **Check:** Recharts library is installed: `npm list recharts`

**Problem: Tailwind styles not applying**
- **Solution:** Restart development server: `npm start`
- **Verify:** `tailwind.config.js` and `postcss.config.js` are correct
- **Check:** `index.css` includes Tailwind directives

### Backend Issues

**Problem: "ModuleNotFoundError" when running scripts**
- **Solution:** Activate virtual environment: `source venv/bin/activate`
- **Install:** `pip install -r requirements.txt`
- **Verify:** Python version: `python --version` (should be 3.10)

**Problem: "manifest.json not found"**
- **Solution:** Run ingest script: `python 0_ingest.py`
- **Create directory:** `mkdir -p etl_tmp`
- **Check:** FirstData/ directory exists with data files

**Problem: "No data returned from API"**
- **Solution:** Check Flask logs for errors
- **Verify:** Manifest includes requested symbol/interval combination
- **Test:** Query smaller date range
- **Check:** CSV file format matches expected schema

**Problem: "CORS errors in browser"**
- **Solution:** Ensure Flask-CORS is installed: `pip install flask-cors`
- **Verify:** `after_request` decorator is adding CORS headers
- **Check:** Browser is using proxy (dev server on :3000)

### Data Issues

**Problem: "File not found" errors**
- **Solution:** Check file naming convention: `{SYMBOL}_full_{interval}_adjsplitdiv.txt`
- **Verify:** Files are in correct directory structure
- **Check:** ZIP files extracted properly
- **Regenerate:** Manifest with `python 0_ingest.py`

**Problem: "Invalid timestamp format"**
- **Solution:** Verify CSV timestamp format: "YYYY-MM-DD HH:MM:SS"
- **Check:** parser.py timestamp parsing pattern
- **Fix:** Adjust timestamp format in raw data or parser

**Problem: "Data appears incorrect or missing"**
- **Solution:** Verify data files are split/dividend-adjusted
- **Check:** OHLC validation (low ≤ open/close, high ≥ open/close)
- **Run:** ETL normalization script: `python etl_ohlvc.py`

### Performance Issues

**Problem: "Slow query responses"**
- **Solution:** Reduce date range or number of symbols
- **Optimize:** Use PyArrow parser instead of Pandas
- **Consider:** Pre-aggregated rollups (5m, 30m, 1hr)
- **Implement:** Caching for frequently queried data

**Problem: "High memory usage"**
- **Solution:** Limit query results (currently 500 rows per symbol)
- **Optimize:** Use chunked reading with Dask
- **Consider:** Switch to Parquet files (smaller, faster)

---

## Development Best Practices

### When Modifying Frontend

1. **Component Structure:**
   - Keep StockSearchEngine.jsx as main component
   - Consider decomposing if it exceeds 500 lines
   - Extract reusable UI components (buttons, inputs, cards)

2. **State Management:**
   - Use useState for component-level state
   - Consider Context API for global state (if needed)
   - Avoid prop drilling beyond 2 levels

3. **Styling:**
   - Use Tailwind utility classes
   - Follow existing color scheme (blue/indigo primary)
   - Ensure responsive design (test mobile breakpoints)

4. **API Integration:**
   - Handle loading and error states
   - Display user-friendly error messages
   - Validate user input before API calls

### When Modifying Backend

1. **API Design:**
   - Use RESTful conventions
   - Return consistent JSON structures
   - Include proper HTTP status codes (200, 404, 500)

2. **Data Processing:**
   - Prefer PyArrow over Pandas for CSV reading (10x faster)
   - Use Polars for data transformations
   - Implement pagination for large result sets

3. **Error Handling:**
   - Catch and log exceptions
   - Return informative error messages
   - Validate input parameters

4. **Performance:**
   - Cache manifest.json in memory (already implemented)
   - Consider result caching for frequent queries
   - Use Dask for parallel processing of multiple files

### When Adding Features

1. **Plan First:**
   - Review existing code patterns
   - Consider impact on frontend and backend
   - Document new endpoints/components

2. **Test Thoroughly:**
   - Write unit tests for new functions
   - Test integration with existing features
   - Verify error cases and edge conditions

3. **Update Documentation:**
   - Update this CLAUDE.md file
   - Update README.md for user-facing changes
   - Add inline comments for complex logic

---

## Quick Reference

### File Paths Cheatsheet

```
Frontend:
  Main Component: stock-search-app/src/StockSearchEngine.jsx
  Entry Point:    stock-search-app/src/index.js
  Config:         stock-search-app/package.json
  Styles:         stock-search-app/src/index.css

Backend:
  API:            backend/etl/query_api.py
  Ingest:         backend/etl/0_ingest.py
  Manifest:       backend/etl/etl_tmp/manifest.json (generated)
  Data:           backend/etl/FirstData/{interval}/...
  Requirements:   backend/etl/requirements.txt

Utilities:
  CSV Parser:     parser.py
  File Finder:    main.py
  ETL:            etl_ohlvc.py

Documentation:
  User Guide:     README.md
  AI Guide:       CLAUDE.md (this file)
  Architecture:   backend/plan.txt
```

### Command Cheatsheet

```bash
# Backend
cd backend/etl
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python 0_ingest.py
python query_api.py

# Frontend
cd stock-search-app
npm install
npm start
npm test
npm run build

# Testing
curl http://127.0.0.1:5000/api/test
pytest --cov=.

# Git (current branch)
git status
git add .
git commit -m "message"
git push -u origin claude/claude-md-mi3qiecvdak8mby1-014yKtcen7V2BYdK1Jr2dmT6
```

### Environment Variables

```bash
# Backend
FIRSTDATA_PATH=/path/to/data  # Override default FirstData location

# Frontend
DANGEROUSLY_DISABLE_HOST_CHECK=true  # Dev server config
WDS_SOCKET_HOST=localhost            # WebSocket host
```

---

## Glossary

- **OHLCV:** Open, High, Low, Close, Volume (stock data metrics)
- **Manifest:** Index file mapping symbols to file paths (`manifest.json`)
- **Interval:** Time granularity (1hr, 5min, 1d, 1w)
- **PyArrow:** Columnar data format library (10x faster CSV reading)
- **Polars:** DataFrame library (alternative to Pandas)
- **Dask:** Parallel computing library for Python
- **Recharts:** React charting library
- **Proxy:** React dev server forwards API calls to Flask backend
- **Split/Dividend-adjusted:** Stock prices adjusted for corporate actions
- **ETL:** Extract, Transform, Load (data pipeline)
- **Parquet:** Columnar storage format (compressed, efficient)

---

## Additional Resources

### Internal Documentation
- `README.md` - User-facing project documentation
- `backend/plan.txt` - Architecture and setup guide
- `LICENSE` - MIT License details

### External Resources
- React Docs: https://react.dev/
- Flask Docs: https://flask.palletsprojects.com/
- Recharts Docs: https://recharts.org/
- Tailwind CSS: https://tailwindcss.com/
- PyArrow Docs: https://arrow.apache.org/docs/python/
- Polars Docs: https://pola-rs.github.io/polars/
- Pandas Docs: https://pandas.pydata.org/

---

## Questions for Humans

When working on this codebase, consider asking the project maintainer:

1. **Data Source:** Where do FirstData files come from? How are they updated?
2. **Production Deployment:** Is production deployment planned? What infrastructure?
3. **Scaling:** Expected dataset size growth? Need for database in future?
4. **Authentication:** Will user authentication be required?
5. **Additional Intervals:** Need support for 1m, 15m, 1d, 1w intervals?
6. **Component Decomposition:** Should StockSearchEngine.jsx be split into smaller components?
7. **Testing Requirements:** What test coverage is expected?
8. **Performance Targets:** What are acceptable query response times?

---

## Changelog

- **2025-11-17:** Initial creation of CLAUDE.md
  - Documented codebase structure, architecture, workflows
  - Added comprehensive development guides and troubleshooting
  - Included code conventions and best practices

---

## Notes for AI Assistants

### When Asked to Implement Features:

1. **Check existing patterns first** - Review similar components/endpoints
2. **Maintain monolithic structure** - Don't over-engineer with excessive abstraction
3. **Use Tailwind for styling** - No custom CSS unless absolutely necessary
4. **Test API changes** - Verify with curl before testing in frontend
5. **Update manifest** - Regenerate after adding data files
6. **Restart servers** - Changes may require backend restart

### When Debugging:

1. **Check Flask logs** - Errors often logged in Flask console
2. **Verify manifest** - Ensure symbol/interval exists in manifest.json
3. **Test API directly** - Use curl to isolate frontend vs backend issues
4. **Check browser console** - Frontend errors appear in DevTools console
5. **Verify proxy** - Ensure package.json proxy matches Flask port

### When Reviewing Code:

1. **Look for hardcoded values** - Company list, intervals, metrics
2. **Check error handling** - Ensure user-friendly error messages
3. **Verify CORS** - API responses must include CORS headers
4. **Test edge cases** - Invalid dates, missing files, large queries
5. **Consider performance** - Large CSV files, many symbols, wide date ranges

---

**End of CLAUDE.md**
