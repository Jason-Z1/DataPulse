# DataPulse

An RPI Course Project that will be tested and used for future research purposes.

## Project Overview

DataPulse is a financial data search engine designed to efficiently query and visualize large-scale stock market data (138GB+) with customizable metrics, time intervals, and export capabilities.

---

## Frontend 

### Technology Stack

**Framework & Libraries:**
- React.js for component-based UI development
- Recharts for interactive stock price visualizations
- Tailwind CSS for modern, responsive styling
- Lucide React for UI icons

### Features

- **Advanced Search**: Search stocks by company name, symbol, or industry tags
- **Flexible Filtering**: 
  - Multiple time intervals (1 minute to 1 week)
  - Custom date range selection
  - Selective metrics display (Open, High, Low, Close, Volume)
- **Dual Visualization Modes**:
  - Interactive line charts with Recharts
  - Sortable data tables
  - Combined view option
- **Data Export**: Download filtered results as CSV files
- **Responsive Design**: Works seamlessly on desktop and mobile devices

### Demo Data

Currently displays mock stock data for demonstration purposes. Backend integration will connect to real CSV datasets.

---

## Backend (In Development)

### Planned Technology Stack

**API & Server:**
- FastAPI for RESTful API endpoints
- Uvicorn for ASGI server deployment

**Data Processing:**
- Pandas for CSV data manipulation and filtering
- Dask for parallel chunked processing of large files (138GB+)
- PyArrow for optimized CSV parsing (10x faster reads)

**Integration:**
- CORS middleware for secure React-Python communication

### Architecture

```
React Frontend (Port 3000) ←→ FastAPI Backend (Port 5000) ←→ CSV Files (138GB)
```

**No database required** - Direct CSV processing with efficient chunking and filtering.

---

## Installation & Setup

### Prerequisites

- Node.js (v14 or higher)
- npm (v6 or higher)

### Frontend Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd DataPulse
```

2. Install dependencies:
```bash
npm install
```

### Dependencies

The project uses the following npm packages:

```json
{
  "dependencies": {
    "react": "^18.x",
    "recharts": "^2.x",
    "lucide-react": "^0.x",
    "tailwindcss": "^3.x"
  }
}
```

### Running the Application

#### Development Mode

Start the React development server:

```bash
npm start
```

The application will open automatically at `http://localhost:3000`

#### Production Build

Create an optimized production build:

```bash
npm run build
```

#### Running Tests

Execute the test suite:

```bash
npm test
```

---

## Project Structure

```
DataPulse/
├── public/
│   └── index.html
├── src/
│   ├── StockSearchEngine.jsx    # Main application component
│   ├── App.js                    # Root component
│   ├── App.css                   # Global styles
│   └── index.js                  # Entry point
├── .gitignore
├── package.json
├── tailwind.config.js            # Tailwind CSS configuration
└── README.md
```

---

## Available Scripts

### `npm start`
Runs the app in development mode at [http://localhost:3000](http://localhost:3000).  
The page will reload when you make changes.

### `npm test`
Launches the test runner in interactive watch mode.

### `npm run build`
Builds the app for production to the `build` folder.  
Optimizes the build for best performance.

### `npm run eject`
**Note: This is a one-way operation!**  
Ejects from Create React App for full configuration control.

---

## Usage

1. **Search for a Company**: Type company name, symbol, or tags (e.g., "Technology", "Electric Vehicles")
2. **Select Filters**:
   - Choose time interval (1m, 5m, 15m, 1h, 1d, 1w)
   - Set date range (From/To dates)
   - Select view mode (Chart, Table, or Both)
3. **Choose Metrics**: Toggle Open, High, Low, Close, Volume data points
4. **Search Data**: Click "Search Data" button to display results
5. **Export**: Download filtered data as CSV using "Download CSV" button

---