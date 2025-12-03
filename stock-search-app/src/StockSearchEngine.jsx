import React, { useState, useEffect, useMemo } from 'react';
import { Search, Download, TrendingUp, X } from 'lucide-react';
import { LineChart } from '@mui/x-charts/LineChart';

const INTERVALS = ["1min", "5min", "30min", "1hour", "1d", "1w"];
const METRICS = ["open", "high", "low", "close", "volume"];

const symbolColors = {
  AAPL: '#8884d8',
  AA:   '#82ca9d',
  A:    '#ffc658',
  MSFT: '#ff7300',
  AMZN: '#a4de6c',
};

const StockSearchEngine = () => {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCompanies, setSelectedCompanies] = useState([]);
  const [interval, setInterval] = useState("1hour");
  const [dateFrom, setDateFrom] = useState('2005-01-01');
  const [dateTo, setDateTo] = useState('2005-01-31');
  const [selectedMetrics, setSelectedMetrics] = useState([...METRICS]);
  const [viewMode, setViewMode] = useState('chart');
  const [showResults, setShowResults] = useState(false);
  const [stockData, setStockData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [companies, setCompanies] = useState([]);
  const [loadingSymbols, setLoadingSymbols] = useState(true);

  useEffect(() => {
    const fetchSymbols = async () => {
      try {
        setLoadingSymbols(true);
        const response = await fetch('/api/manifest');
        if (!response.ok) throw new Error('Bad manifest response');
        const manifest = await response.json();
        const formatted = (manifest.all_tags || []).map(tag => ({
          symbol: tag,
          name: tag,
          tags: [tag],
        }));
        setCompanies(formatted);
      } catch (err) {
        console.error("Failed to load symbols from manifest", err);
        setError('Failed to load ticker symbols. Please ensure Flask is running.');
      } finally {
        setLoadingSymbols(false);
      }
    };
    fetchSymbols();
  }, []);

  const handleSearch = async () => {
    if (selectedCompanies.length === 0) {
      setError('Please select at least one company symbol.');
      return;
    }

    setShowResults(false);
    setLoading(true);
    setError('');

    const params = [
      ...selectedCompanies.map(sym => `symbols[]=${encodeURIComponent(sym)}`),
      `interval=${encodeURIComponent(interval)}`,
      `date_from=${encodeURIComponent(dateFrom)}`,
      `date_to=${encodeURIComponent(dateTo)}`,
      ...selectedMetrics.map(m => `metrics[]=${encodeURIComponent(m)}`),
    ].join('&');

    try {
      const res = await fetch(`/api/query_stock?${params}`);
      if (!res.ok) {
        setError('No data found for this search.');
        setStockData([]);
        setShowResults(true);
        return;
      }
      const data = await res.json();
      setStockData(Array.isArray(data) ? data : []);
      setShowResults(true);
    } catch (err) {
      console.error(err);
      setError('Network error or backend not running.');
      setStockData([]);
      setShowResults(true);
    } finally {
      setLoading(false);
    }
  };

  const handleMetricToggle = (metric) => {
    setSelectedMetrics(prev =>
      prev.includes(metric)
        ? prev.filter(m => m !== metric)
        : [...prev, metric]
    );
  };

  const downloadCSV = () => {
    if (!stockData.length) return;
    const headers = ['Time', 'Symbol', ...selectedMetrics.map(
      m => m.charAt(0).toUpperCase() + m.slice(1)
    )];
    const csvContent = [
      headers.join(','),
      ...stockData.map(row =>
        [
          row.time,
          row.symbol,
          ...selectedMetrics.map(metric => row[metric]),
        ].join(',')
      ),
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${selectedCompanies.join('_') || 'stock'}_data.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const filteredCompanies = companies.filter(company =>
    company.symbol.toLowerCase().includes(searchQuery.toLowerCase())
  );

  // Downsample data for performance with large datasets
  const denseData = useMemo(() => {
    const MAX_POINTS = 3000;
    if (stockData.length <= MAX_POINTS) return stockData;
    
    const step = Math.ceil(stockData.length / MAX_POINTS);
    return stockData.filter((_, idx) => idx % step === 0);
  }, [stockData]);

  // Build pivoted dataset for MUI X Charts with real zoom/pan
  const chartConfig = useMemo(() => {
    if (!denseData.length || !selectedMetrics.includes('close')) {
      return { dataset: [], series: [], xAxis: [] };
    }

    // Group data by timestamp and pivot
    const timeMap = {};
    denseData.forEach(row => {
      if (!timeMap[row.time]) {
        timeMap[row.time] = { time: new Date(row.time) };
      }
      timeMap[row.time][`close_${row.symbol}`] = row.close;
    });

    // Convert to array and sort by time
    const dataset = Object.values(timeMap).sort((a, b) => a.time - b.time);

    // Build series for each selected symbol
    const series = selectedCompanies
      .filter(symbol => {
        // Check if this symbol has any data
        return dataset.some(row => row[`close_${symbol}`] != null);
      })
      .map(symbol => ({
        id: `${symbol}-close`,
        label: `${symbol} Close`,
        dataKey: `close_${symbol}`,
        showMark: false,
        color: symbolColors[symbol] || '#8884d8',
        connectNulls: true,
      }));

    // Configure x-axis with zoom and pan
    const xAxis = [{
      dataKey: 'time',
      scaleType: 'time',
      valueFormatter: (v) => {
        if (!v) return '';
        const date = v instanceof Date ? v : new Date(v);
        return date.toISOString().slice(0, 10);
      },
      zoom: {
        minSpan: 50,  // minimum number of points visible
        panning: true, // enable drag to pan
        filterMode: 'discard',
      },
      // Enable pinch-to-zoom on touchpad/touchscreen
      disablePinch: false,
    }];

    return { dataset, series, xAxis };
  }, [denseData, selectedCompanies, selectedMetrics]);

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-indigo-50">
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <div className="flex items-center gap-3">
            <TrendingUp className="w-8 h-8 text-blue-600" />
            <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">
              FinanceSearch Pro
            </h1>
          </div>
          <p className="text-gray-600 mt-2">
            Advanced stock data search with customizable metrics and intervals
          </p>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Search Section */}
        <div className="bg-white rounded-2xl shadow-xl p-8 mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-6 flex items-center gap-2">
            <Search className="w-6 h-6" />
            Search Financial Data
          </h2>

          {/* Symbol search */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Company Symbols{" "}
              {loadingSymbols && (
                <span className="text-blue-600 text-xs">(Loading...)</span>
              )}
              {!loadingSymbols && (
                <span className="text-gray-500 text-xs">
                  ({companies.length} available)
                </span>
              )}
            </label>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-5 h-5" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search ticker symbol (e.g., AAPL, TSLA, MSFT)"
                className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                disabled={loadingSymbols}
              />
              {searchQuery && filteredCompanies.length > 0 && (
                <div className="mt-2 bg-white border border-gray-200 rounded-lg shadow-lg max-h-60 overflow-y-auto absolute w-full z-10">
                  {filteredCompanies.slice(0, 50).map(company => (
                    <div
                      key={company.symbol}
                      onClick={() => {
                        setSelectedCompanies(prev =>
                          prev.includes(company.symbol)
                            ? prev
                            : [...prev, company.symbol]
                        );
                        setSearchQuery('');
                      }}
                      className="p-3 hover:bg-blue-50 cursor-pointer border-b border-gray-100 last:border-b-0"
                    >
                      <div className="font-semibold text-gray-800">
                        {company.symbol}
                      </div>
                    </div>
                  ))}
                  {filteredCompanies.length > 50 && (
                    <div className="p-3 text-sm text-gray-500 text-center">
                      Showing first 50 of {filteredCompanies.length} results
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* Selected Companies */}
            {selectedCompanies.length > 0 && (
              <div className="flex gap-2 flex-wrap mt-3">
                {selectedCompanies.map(sym => (
                  <span
                    key={sym}
                    className="bg-blue-100 text-blue-800 rounded-full px-3 py-1 flex items-center gap-2 text-sm font-medium"
                  >
                    {sym}
                    <button
                      onClick={() =>
                        setSelectedCompanies(prev => prev.filter(c => c !== sym))
                      }
                      className="hover:bg-blue-200 rounded-full p-0.5 transition-colors"
                    >
                      <X className="w-3 h-3" />
                    </button>
                  </span>
                ))}
              </div>
            )}
          </div>

          {/* Interval Selection */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Interval
            </label>
            <select
              value={interval}
              onChange={e => setInterval(e.target.value)}
              className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            >
              {INTERVALS.map(intv => (
                <option key={intv} value={intv}>
                  {intv}
                </option>
              ))}
            </select>
          </div>

          {/* Date Range */}
          <div className="mb-6 grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Date From
              </label>
              <input
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Date To
              </label>
              <input
                type="date"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>

          {/* Metrics */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-3">
              Metrics
            </label>
            <div className="flex flex-wrap gap-3">
              {METRICS.map(metric => (
                <label key={metric} className="flex items-center">
                  <input
                    type="checkbox"
                    checked={selectedMetrics.includes(metric)}
                    onChange={() => handleMetricToggle(metric)}
                    className="mr-2 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <span className="capitalize font-medium text-gray-700">
                    {metric}
                  </span>
                </label>
              ))}
            </div>
          </div>

          <button
            onClick={handleSearch}
            disabled={selectedCompanies.length === 0 || loadingSymbols}
            className="bg-gradient-to-r from-blue-600 to-indigo-600 text-white px-8 py-3 rounded-lg font-semibold hover:from-blue-700 hover:to-indigo-700 transition-all shadow-lg hover:shadow-xl disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Search Data ({selectedCompanies.length}{" "}
            {selectedCompanies.length === 1 ? 'symbol' : 'symbols'})
          </button>
        </div>

        {loading && (
          <p className="text-blue-600 text-center text-lg">Loading...</p>
        )}
        {error && (
          <p className="text-red-600 text-center text-lg">{error}</p>
        )}

        {/* Results Section */}
        {showResults && stockData.length > 0 && (
          <div className="bg-white rounded-2xl shadow-xl p-8">
            <div className="flex justify-between items-center mb-6">
              <h2 className="text-2xl font-bold text-gray-800">
                Results for {selectedCompanies.join(', ')} ({interval} interval)
              </h2>
              <div className="flex gap-3">
                <div className="relative">
                  <select
                    value={viewMode}
                    onChange={(e) => setViewMode(e.target.value)}
                    className="appearance-none bg-blue-600 text-white px-4 py-2 pr-10 rounded-lg hover:bg-blue-700 transition-colors cursor-pointer font-medium"
                  >
                    <option value="chart">Chart View</option>
                    <option value="table">Table View</option>
                  </select>
                  <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-white">
                    <svg
                      className="w-4 h-4"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth="2"
                        d="M19 9l-7 7-7-7"
                      />
                    </svg>
                  </div>
                </div>
                <button
                  onClick={downloadCSV}
                  className="flex items-center gap-2 bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition-colors"
                >
                  <Download className="w-4 h-4" />
                  Download CSV
                </button>
              </div>
            </div>

            {/* Chart View with MUI X Charts - Real Zoom & Pan */}
            {viewMode === 'chart' && (
              <div className="mb-8">
                <h3 className="text-lg font-semibold mb-4">
                  Price Chart 
                  {denseData.length < stockData.length && (
                    <span className="text-sm text-gray-500 ml-2">
                      (Showing {denseData.length} of {stockData.length} points for performance)
                    </span>
                  )}
                </h3>
                <div className="h-96">
                  {chartConfig.dataset.length > 0 && chartConfig.series.length > 0 ? (
                    <LineChart
                      dataset={chartConfig.dataset}
                      xAxis={chartConfig.xAxis}
                      series={chartConfig.series}
                      height={384}
                      margin={{ top: 20, right: 20, bottom: 50, left: 70 }}
                      slotProps={{
                        legend: {
                          direction: 'row',
                          position: { vertical: 'top', horizontal: 'middle' },
                          padding: 0,
                        },
                      }}
                    />
                  ) : (
                    <div className="flex items-center justify-center h-full text-gray-500">
                      No chart data available. Please ensure "close" metric is selected.
                    </div>
                  )}
                </div>
                <p className="text-sm text-gray-600 mt-2">
                  💡 Tip: <strong>Mouse:</strong> Scroll wheel to zoom, drag to pan • <strong>Touchpad:</strong> Two-finger pinch to zoom, two-finger drag to pan
                </p>
              </div>
            )}

            {/* Table View */}
            {viewMode === 'table' && (
              <div>
                <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
                  Data Table
                </h3>
                <div className="overflow-x-auto">
                  <table className="w-full border-collapse border border-gray-300">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="border border-gray-300 px-4 py-2 text-left font-semibold">
                          Time
                        </th>
                        <th className="border border-gray-300 px-4 py-2 text-left font-semibold">
                          Symbol
                        </th>
                        {selectedMetrics.map(metric => (
                          <th
                            key={metric}
                            className="border border-gray-300 px-4 py-2 text-left font-semibold"
                          >
                            {metric.charAt(0).toUpperCase() + metric.slice(1)}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {stockData.map((row, index) => (
                        <tr key={index} className="hover:bg-gray-50">
                          <td className="border border-gray-300 px-4 py-2">
                            {row.time}
                          </td>
                          <td className="border border-gray-300 px-4 py-2 font-semibold">
                            {row.symbol}
                          </td>
                          {selectedMetrics.map(metric => (
                            <td
                              key={metric}
                              className="border border-gray-300 px-4 py-2"
                            >
                              {metric === 'volume'
                                ? row[metric]?.toLocaleString()
                                : (row[metric] != null
                                    ? (+row[metric]).toFixed(4)
                                    : '')}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default StockSearchEngine;