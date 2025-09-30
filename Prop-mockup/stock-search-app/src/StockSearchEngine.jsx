import React, { useState } from 'react';
import { Search, Download, TrendingUp, Calendar, Clock, Filter, BarChart3, Table } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area } from 'recharts';

const StockSearchEngine = () => {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCompany, setSelectedCompany] = useState('');
  const [timeInterval, setTimeInterval] = useState('1h');
  const [dateFrom, setDateFrom] = useState('2005-01-01');
  const [dateTo, setDateTo] = useState('2005-01-10');
  const [selectedMetrics, setSelectedMetrics] = useState(['open', 'high', 'low', 'close']);
  const [viewMode, setViewMode] = useState('chart');
  const [showResults, setShowResults] = useState(false);

  // Mock company data with tags
  const companies = [
    { symbol: 'AAPL', name: 'Apple Inc.', tags: ['Technology', 'Consumer Electronics', 'Large Cap', 'Blue Chip'] },
    { symbol: 'GOOGL', name: 'Alphabet Inc.', tags: ['Technology', 'Search Engine', 'Cloud Computing', 'Large Cap'] },
    { symbol: 'TSLA', name: 'Tesla Inc.', tags: ['Automotive', 'Electric Vehicles', 'Clean Energy', 'Growth'] },
    { symbol: 'MSFT', name: 'Microsoft Corp.', tags: ['Technology', 'Software', 'Cloud Computing', 'Large Cap'] },
    { symbol: 'AMZN', name: 'Amazon.com Inc.', tags: ['E-commerce', 'Cloud Computing', 'Large Cap', 'Growth'] }
  ];

  // Mock stock data
  const mockStockData = [
    { time: '09:00', open: 10.5382, high: 10.5688, low: 10.5119, close: 10.5294, volume: 651999 },
    { time: '10:00', open: 10.5294, high: 10.5731, low: 10.2758, close: 10.3326, volume: 959369 },
    { time: '11:00', open: 10.3326, high: 10.4332, low: 10.2977, close: 10.4114, volume: 564186 },
    { time: '12:00', open: 10.4114, high: 10.4944, low: 10.372, close: 10.4857, volume: 379398 },
    { time: '13:00', open: 10.4857, high: 10.5163, low: 10.4201, close: 10.4245, volume: 562353 },
    { time: '14:00', open: 10.4245, high: 10.4756, low: 10.3891, close: 10.4523, volume: 445721 },
    { time: '15:00', open: 10.4523, high: 10.5234, low: 10.4123, close: 10.4987, volume: 623847 }
  ];

  const handleSearch = () => {
    setShowResults(true);
  };

  const handleMetricToggle = (metric) => {
    setSelectedMetrics(prev => 
      prev.includes(metric) 
        ? prev.filter(m => m !== metric)
        : [...prev, metric]
    );
  };

  const downloadCSV = () => {
    const headers = ['Time', ...selectedMetrics.map(m => m.charAt(0).toUpperCase() + m.slice(1)), 'Volume'];
    const csvContent = [
      headers.join(','),
      ...mockStockData.map(row => 
        [row.time, ...selectedMetrics.map(metric => row[metric]), row.volume].join(',')
      )
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${selectedCompany || 'stock'}_data.csv`;
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
      {/* Header */}
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

          {/* Company Search */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">Company Search</label>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search by company name, symbol, or tags (e.g., Technology, Electric Vehicles)"
                className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
            
            {/* Company Suggestions */}
            {searchQuery && (
              <div className="mt-2 bg-white border border-gray-200 rounded-lg shadow-lg max-h-60 overflow-y-auto">
                {filteredCompanies.map(company => (
                  <div 
                    key={company.symbol}
                    onClick={() => {
                      setSelectedCompany(company.symbol);
                      setSearchQuery(`${company.symbol} - ${company.name}`);
                    }}
                    className="p-4 hover:bg-blue-50 cursor-pointer border-b border-gray-100 last:border-b-0"
                  >
                    <div className="flex justify-between items-start">
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
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Filters Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
            {/* Time Interval */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                <Clock className="w-4 h-4 inline mr-1" />
                Time Interval
              </label>
              <select 
                value={timeInterval}
                onChange={(e) => setTimeInterval(e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
              >
                <option value="1m">1 Minute</option>
                <option value="5m">5 Minutes</option>
                <option value="15m">15 Minutes</option>
                <option value="1h">1 Hour</option>
                <option value="1d">1 Day</option>
                <option value="1w">1 Week</option>
              </select>
            </div>

            {/* Date From */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                <Calendar className="w-4 h-4 inline mr-1" />
                From Date
              </label>
              <input
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
              />
            </div>

            {/* Date To */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                <Calendar className="w-4 h-4 inline mr-1" />
                To Date
              </label>
              <input
                type="date"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
              />
            </div>

            {/* View Mode */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                <BarChart3 className="w-4 h-4 inline mr-1" />
                View Mode
              </label>
              <select 
                value={viewMode}
                onChange={(e) => setViewMode(e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
              >
                <option value="chart">Chart View</option>
                <option value="table">Table View</option>
                <option value="both">Both</option>
              </select>
            </div>
          </div>

          {/* Metrics Selection */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-3">
              <Filter className="w-4 h-4 inline mr-1" />
              Select Metrics to Display
            </label>
            <div className="flex flex-wrap gap-3">
              {['open', 'high', 'low', 'close', 'volume'].map(metric => (
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

          {/* Search Button */}
          <button
            onClick={handleSearch}
            className="bg-gradient-to-r from-blue-600 to-indigo-600 text-white px-8 py-3 rounded-lg font-semibold hover:from-blue-700 hover:to-indigo-700 transition-all shadow-lg hover:shadow-xl"
          >
            Search Data
          </button>
        </div>

        {/* Results Section */}
        {showResults && (
          <div className="bg-white rounded-2xl shadow-xl p-8">
            <div className="flex justify-between items-center mb-6">
              <h2 className="text-2xl font-bold text-gray-800">
                Results for {selectedCompany || 'Selected Stock'} ({timeInterval} intervals)
              </h2>
              <button
                onClick={downloadCSV}
                className="flex items-center gap-2 bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition-colors"
              >
                <Download className="w-4 h-4" />
                Download CSV
              </button>
            </div>

            {/* Chart View */}
            {(viewMode === 'chart' || viewMode === 'both') && (
              <div className="mb-8">
                <h3 className="text-lg font-semibold mb-4">Price Chart</h3>
                <div className="h-96">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={mockStockData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="time" />
                      <YAxis domain={['dataMin - 0.1', 'dataMax + 0.1']} />
                      <Tooltip />
                      {selectedMetrics.includes('open') && <Line type="monotone" dataKey="open" stroke="#8884d8" strokeWidth={2} />}
                      {selectedMetrics.includes('high') && <Line type="monotone" dataKey="high" stroke="#82ca9d" strokeWidth={2} />}
                      {selectedMetrics.includes('low') && <Line type="monotone" dataKey="low" stroke="#ffc658" strokeWidth={2} />}
                      {selectedMetrics.includes('close') && <Line type="monotone" dataKey="close" stroke="#ff7300" strokeWidth={2} />}
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            )}

            {/* Table View */}
            {(viewMode === 'table' || viewMode === 'both') && (
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
                        {selectedMetrics.includes('open') && <th className="border border-gray-300 px-4 py-2 text-left font-semibold">Open</th>}
                        {selectedMetrics.includes('high') && <th className="border border-gray-300 px-4 py-2 text-left font-semibold">High</th>}
                        {selectedMetrics.includes('low') && <th className="border border-gray-300 px-4 py-2 text-left font-semibold">Low</th>}
                        {selectedMetrics.includes('close') && <th className="border border-gray-300 px-4 py-2 text-left font-semibold">Close</th>}
                        {selectedMetrics.includes('volume') && <th className="border border-gray-300 px-4 py-2 text-left font-semibold">Volume</th>}
                      </tr>
                    </thead>
                    <tbody>
                      {mockStockData.map((row, index) => (
                        <tr key={index} className="hover:bg-gray-50">
                          <td className="border border-gray-300 px-4 py-2">{row.time}</td>
                          {selectedMetrics.includes('open') && <td className="border border-gray-300 px-4 py-2">{row.open.toFixed(4)}</td>}
                          {selectedMetrics.includes('high') && <td className="border border-gray-300 px-4 py-2">{row.high.toFixed(4)}</td>}
                          {selectedMetrics.includes('low') && <td className="border border-gray-300 px-4 py-2">{row.low.toFixed(4)}</td>}
                          {selectedMetrics.includes('close') && <td className="border border-gray-300 px-4 py-2">{row.close.toFixed(4)}</td>}
                          {selectedMetrics.includes('volume') && <td className="border border-gray-300 px-4 py-2">{row.volume.toLocaleString()}</td>}
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