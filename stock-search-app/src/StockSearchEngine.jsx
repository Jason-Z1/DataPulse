import React, { useState } from 'react';
import { Search, Download, TrendingUp, Table, X } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';

const INTERVALS = ["1hour", "5min", "1d", "1w"];
const METRICS = ["open", "high", "low", "close", "volume"];

const StockSearchEngine = () => {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCompanies, setSelectedCompanies] = useState([]); // Array of symbols
  const [interval, setInterval] = useState("1hour");
  const [dateFrom, setDateFrom] = useState('2005-01-01');
  const [dateTo, setDateTo] = useState('2005-01-10');
  const [selectedMetrics, setSelectedMetrics] = useState([...METRICS]);
  const [viewMode, setViewMode] = useState('chart');
  const [showResults, setShowResults] = useState(false);
  const [stockData, setStockData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const companies = [
    { symbol: 'AAPL', name: 'Apple Inc.', tags: ['Technology', 'Consumer Electronics', 'Large Cap', 'Blue Chip'] },
    { symbol: 'BWA', name: '.', tags: ['Technology', 'Consumer Electronics', 'Large Cap', 'Blue Chip'] },
    { symbol: 'BUSE', name: '.', tags: ['Technology', 'Consumer Electronics', 'Large Cap', 'Blue Chip'] },

    { symbol: 'GOOGL', name: 'Alphabet Inc.', tags: ['Technology', 'Search Engine', 'Cloud Computing', 'Large Cap'] },
    { symbol: 'TSLA', name: 'Tesla Inc.', tags: ['Automotive', 'Electric Vehicles', 'Clean Energy', 'Growth'] },
    { symbol: 'MSFT', name: 'Microsoft Corp.', tags: ['Technology', 'Software', 'Cloud Computing', 'Large Cap'] },
    { symbol: 'AMZN', name: 'Amazon.com Inc.', tags: ['E-commerce', 'Cloud Computing', 'Large Cap', 'Growth'] }
  ];

  // Color palette for different symbols
  const symbolColors = {
    'AAPL': '#8884d8',
    'GOOGL': '#82ca9d',
    'TSLA': '#ffc658',
    'MSFT': '#ff7300',
    'AMZN': '#a4de6c'
  };

  const handleSearch = async () => {
    if (selectedCompanies.length === 0) {
      setError('Please select at least one company symbol.');
      return;
    }
    
    setShowResults(false);
    setLoading(true);
    setError('');
    const params = [
      ...selectedCompanies.map(sym => `symbols[]=${sym}`),
      `interval=${interval}`,
      `date_from=${dateFrom}`,
      `date_to=${dateTo}`,
      ...selectedMetrics.map(m => `metrics[]=${m}`)
    ].join('&');
    try {
      const res = await fetch(`/api/query_stock?${params}`);
      if (!res.ok) {
        setError('No data found for this search.');
        setStockData([]);
        setLoading(false);
        setShowResults(true);
        return;
      }
      const data = await res.json();
      setStockData(data);
      setLoading(false);
      setShowResults(true);
    } catch (err) {
      setError('Network error or backend not running.');
      setStockData([]);
      setLoading(false);
      setShowResults(true);
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
    const headers = ['Time', 'Symbol', ...selectedMetrics.map(m => m.charAt(0).toUpperCase() + m.slice(1))];
    const csvContent = [
      headers.join(','),
      ...stockData.map(row =>
        [row.time, row.symbol, ...selectedMetrics.map(metric => row[metric])].join(',')
      )
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
    company.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
    company.symbol.toLowerCase().includes(searchQuery.toLowerCase()) ||
    company.tags.some(tag => tag.toLowerCase().includes(searchQuery.toLowerCase()))
  );

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
          <p className="text-gray-600 mt-2">Advanced stock data search with customizable metrics and intervals</p>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Search Section */}
        <div className="bg-white rounded-2xl shadow-xl p-8 mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-6 flex items-center gap-2">
            <Search className="w-6 h-6" />
            Search Financial Data
          </h2>

          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">Company Symbols</label>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search company symbol, name, or tag"
                className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
              {searchQuery && (
                <div className="mt-2 bg-white border border-gray-200 rounded-lg shadow-lg max-h-60 overflow-y-auto absolute w-full z-10">
                  {filteredCompanies.map(company => (
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
                      className="p-4 hover:bg-blue-50 cursor-pointer border-b border-gray-100 last:border-b-0"
                    >
                      <div>
                        <div className="font-semibold text-gray-800">{company.symbol} - {company.name}</div>
                        <div className="flex flex-wrap gap-1 mt-2">
                          {company.tags.map(tag => (
                            <span key={tag} className="px-2 py-1 bg-blue-100 text-blue-700 text-xs rounded-full">
                              {tag}
                            </span>
                          ))}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
            
            {/* Selected Companies Display */}
            {selectedCompanies.length > 0 && (
              <div className="flex gap-2 flex-wrap mt-3">
                {selectedCompanies.map(sym => (
                  <span key={sym} className="bg-blue-100 text-blue-800 rounded-full px-3 py-1 flex items-center gap-2 text-sm font-medium">
                    {sym}
                    <button
                      onClick={() => setSelectedCompanies(prev => prev.filter(c => c !== sym))}
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
            <label className="block text-sm font-medium text-gray-700 mb-2">Interval</label>
            <select
              value={interval}
              onChange={e => setInterval(e.target.value)}
              className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            >
              {INTERVALS.map(intv => (
                <option key={intv} value={intv}>{intv}</option>
              ))}
            </select>
          </div>

          {/* Date Range */}
          <div className="mb-6 grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Date From</label>
              <input
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Date To</label>
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
            <label className="block text-sm font-medium text-gray-700 mb-3">Metrics</label>
            <div className="flex flex-wrap gap-3">
              {METRICS.map(metric => (
                <label key={metric} className="flex items-center">
                  <input
                    type="checkbox"
                    checked={selectedMetrics.includes(metric)}
                    onChange={() => handleMetricToggle(metric)}
                    className="mr-2 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <span className="capitalize font-medium text-gray-700">{metric}</span>
                </label>
              ))}
            </div>
          </div>

          <button
            onClick={handleSearch}
            disabled={selectedCompanies.length === 0}
            className="bg-gradient-to-r from-blue-600 to-indigo-600 text-white px-8 py-3 rounded-lg font-semibold hover:from-blue-700 hover:to-indigo-700 transition-all shadow-lg hover:shadow-xl disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Search Data ({selectedCompanies.length} {selectedCompanies.length === 1 ? 'symbol' : 'symbols'})
          </button>
        </div>

        {loading && <p className="text-blue-600 text-center text-lg">Loading...</p>}
        {error && <p className="text-red-600 text-center text-lg">{error}</p>}

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
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M19 9l-7 7-7-7" />
                    </svg>
                  </div>
                </div>
                <button onClick={downloadCSV} className="flex items-center gap-2 bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition-colors">
                  <Download className="w-4 h-4" />
                  Download CSV
                </button>
              </div>
            </div>

            {/* Chart View */}
            {viewMode === 'chart' && (
              <div className="mb-8">
                <h3 className="text-lg font-semibold mb-4">Price Chart</h3>
                <div className="h-96">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={stockData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="time" />
                      <YAxis />
                      <Tooltip />
                      <Legend />
                      {selectedCompanies.map(symbol => (
                        selectedMetrics.includes('close') && (
                          <Line
                            key={`${symbol}-close`}
                            type="monotone"
                            dataKey="close"
                            data={stockData.filter(d => d.symbol === symbol)}
                            name={`${symbol} Close`}
                            stroke={symbolColors[symbol] || '#8884d8'}
                            strokeWidth={2}
                          />
                        )
                      ))}
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            )}

            {/* Table View */}
            {viewMode === 'table' && (
              <div>
                <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
                  <Table className="w-5 h-5" />
                  Data Table
                </h3>
                <div className="overflow-x-auto">
                  <table className="w-full border-collapse border border-gray-300">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="border border-gray-300 px-4 py-2 text-left font-semibold">Time</th>
                        <th className="border border-gray-300 px-4 py-2 text-left font-semibold">Symbol</th>
                        {selectedMetrics.map(metric =>
                          <th key={metric} className="border border-gray-300 px-4 py-2 text-left font-semibold">
                            {metric.charAt(0).toUpperCase() + metric.slice(1)}
                          </th>
                        )}
                      </tr>
                    </thead>
                    <tbody>
                      {stockData.map((row, index) => (
                        <tr key={index} className="hover:bg-gray-50">
                          <td className="border border-gray-300 px-4 py-2">{row.time}</td>
                          <td className="border border-gray-300 px-4 py-2 font-semibold">{row.symbol}</td>
                          {selectedMetrics.map(metric =>
                            <td key={metric} className="border border-gray-300 px-4 py-2">
                              {metric === 'volume' ? row[metric]?.toLocaleString() : (+row[metric]).toFixed(4)}
                            </td>
                          )}
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