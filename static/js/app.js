// API Base URL
const API_BASE = 'http://localhost:8000/api';

// Global state
let currentTab = 'whales';
let whalesData = [];
let betsData = [];
let marketsData = [];

// Initialize app
document.addEventListener('DOMContentLoaded', () => {
    initializeTabs();
    initializeEventListeners();
    loadSummaryStats();
    loadWhalesData();

    // Auto-refresh every 5 minutes
    setInterval(() => {
        refreshCurrentTab();
    }, 300000);
});

// Tab management
function initializeTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const tabName = button.dataset.tab;

            // Update active states
            tabButtons.forEach(btn => btn.classList.remove('active'));
            tabContents.forEach(content => content.classList.remove('active'));

            button.classList.add('active');
            document.getElementById(`${tabName}-tab`).classList.add('active');

            currentTab = tabName;

            // Load data for the selected tab
            switch(tabName) {
                case 'whales':
                    loadWhalesData();
                    break;
                case 'bets':
                    loadBetsData();
                    break;
                case 'markets':
                    loadMarketsData();
                    break;
            }
        });
    });
}

// Event listeners
function initializeEventListeners() {
    // Refresh button
    document.getElementById('refresh-btn').addEventListener('click', () => {
        refreshAllData();
    });

    // Filter bets button
    document.getElementById('filter-bets-btn').addEventListener('click', () => {
        const minAmount = document.getElementById('min-bet-amount').value;
        loadBetsData(minAmount);
    });

    // Track wallet button
    document.getElementById('track-wallet-btn').addEventListener('click', () => {
        trackNewWallet();
    });

    // Modal close button
    document.querySelector('.close').addEventListener('click', () => {
        document.getElementById('wallet-modal').style.display = 'none';
    });

    // Click outside modal to close
    window.addEventListener('click', (event) => {
        const modal = document.getElementById('wallet-modal');
        if (event.target === modal) {
            modal.style.display = 'none';
        }
    });
}

// Load summary statistics
async function loadSummaryStats() {
    try {
        const response = await fetch(`${API_BASE}/stats/summary`);
        const data = await response.json();

        if (data.success) {
            const stats = data.data;
            document.getElementById('total-whales').textContent = stats.total_whales_tracked || '0';
            document.getElementById('total-volume').textContent = formatCurrency(stats.total_volume_tracked || 0);
            document.getElementById('avg-win-rate').textContent = `${stats.average_win_rate || 0}%`;
            document.getElementById('last-update').textContent = formatTime(stats.last_update);
        }
    } catch (error) {
        console.error('Error loading summary stats:', error);
    }
}

// Load whales data
async function loadWhalesData() {
    try {
        const response = await fetch(`${API_BASE}/whales`);
        const data = await response.json();

        if (data.success) {
            whalesData = data.data;
            renderWhalesTable(whalesData);
        }
    } catch (error) {
        console.error('Error loading whales data:', error);
        document.getElementById('whales-tbody').innerHTML =
            '<tr><td colspan="9" class="loading">Error loading data</td></tr>';
    }
}

// Load bets data
async function loadBetsData(minAmount = 1000) {
    try {
        const response = await fetch(`${API_BASE}/bets/recent?min_amount=${minAmount}`);
        const data = await response.json();

        if (data.success) {
            betsData = data.data;
            renderBetsTable(betsData);
        }
    } catch (error) {
        console.error('Error loading bets data:', error);
        document.getElementById('bets-tbody').innerHTML =
            '<tr><td colspan="8" class="loading">Error loading data</td></tr>';
    }
}

// Load markets data
async function loadMarketsData() {
    try {
        const response = await fetch(`${API_BASE}/markets/active`);
        const data = await response.json();

        if (data.success) {
            marketsData = data.data;
            renderMarketsTable(marketsData);
        }
    } catch (error) {
        console.error('Error loading markets data:', error);
        document.getElementById('markets-tbody').innerHTML =
            '<tr><td colspan="4" class="loading">Error loading data</td></tr>';
    }
}

// Render whales table
function renderWhalesTable(whales) {
    const tbody = document.getElementById('whales-tbody');

    if (whales.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9" class="loading">No whale data available</td></tr>';
        return;
    }

    tbody.innerHTML = whales.map(whale => `
        <tr>
            <td>${shortenAddress(whale.address)}</td>
            <td>${formatCurrency(whale.total_volume)}</td>
            <td>${whale.total_bets}</td>
            <td class="${whale.win_rate >= 50 ? 'positive' : 'negative'}">
                ${whale.win_rate}%
            </td>
            <td class="${whale.profit_loss >= 0 ? 'positive' : 'negative'}">
                ${formatCurrency(whale.profit_loss)}
            </td>
            <td>${formatCurrency(whale.largest_bet)}</td>
            <td>${whale.markets_traded}</td>
            <td>${formatTime(whale.last_activity)}</td>
            <td>
                <button class="view-details-btn" onclick="viewWalletDetails('${whale.address}')">
                    View Details
                </button>
            </td>
        </tr>
    `).join('');
}

// Render bets table
function renderBetsTable(bets) {
    const tbody = document.getElementById('bets-tbody');

    if (bets.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="loading">No recent large bets</td></tr>';
        return;
    }

    tbody.innerHTML = bets.map(bet => `
        <tr>
            <td>${formatTime(bet.timestamp)}</td>
            <td>${bet.market?.question || 'Unknown Market'}</td>
            <td>${shortenAddress(bet.maker || bet.taker)}</td>
            <td>${bet.side}</td>
            <td>${bet.price.toFixed(4)}</td>
            <td>${bet.size.toFixed(2)}</td>
            <td>${formatCurrency(bet.value)}</td>
            <td>${bet.is_whale ? '<span class="whale-badge">🐋 WHALE</span>' : '-'}</td>
        </tr>
    `).join('');
}

// Render markets table
function renderMarketsTable(markets) {
    const tbody = document.getElementById('markets-tbody');

    if (markets.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="loading">No active markets with whale activity</td></tr>';
        return;
    }

    tbody.innerHTML = markets.map(market => `
        <tr>
            <td>${market.question}</td>
            <td>${market.whale_count}</td>
            <td>${formatCurrency(market.total_whale_volume)}</td>
            <td>
                ${market.recent_trades.slice(0, 3).map(trade =>
                    `<div>${shortenAddress(trade.wallet)}: ${formatCurrency(trade.value)}</div>`
                ).join('')}
            </td>
        </tr>
    `).join('');
}

// View wallet details
async function viewWalletDetails(address) {
    try {
        const response = await fetch(`${API_BASE}/whales/${address}`);
        const data = await response.json();

        if (data.success) {
            const details = data.data;
            showWalletModal(details);
        }
    } catch (error) {
        console.error('Error loading wallet details:', error);
        alert('Error loading wallet details');
    }
}

// Show wallet modal
function showWalletModal(details) {
    const modal = document.getElementById('wallet-modal');
    const detailsDiv = document.getElementById('wallet-details');

    detailsDiv.innerHTML = `
        <div class="wallet-summary">
            <h3>Wallet: ${details.address}</h3>
            <div class="detail-grid">
                <div class="detail-item">
                    <span class="detail-label">Total Volume</span>
                    <span class="detail-value">${formatCurrency(details.total_volume)}</span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">Total Bets</span>
                    <span class="detail-value">${details.total_bets}</span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">Win Rate</span>
                    <span class="detail-value ${details.win_rate >= 0.5 ? 'positive' : 'negative'}">
                        ${(details.win_rate * 100).toFixed(2)}%
                    </span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">P&L</span>
                    <span class="detail-value ${details.profit_loss >= 0 ? 'positive' : 'negative'}">
                        ${formatCurrency(details.profit_loss)}
                    </span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">Largest Bet</span>
                    <span class="detail-value">${formatCurrency(details.largest_bet)}</span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">Markets Traded</span>
                    <span class="detail-value">${details.markets_traded.length}</span>
                </div>
            </div>
        </div>

        <h3>Recent Trades</h3>
        <div class="trade-list">
            ${details.recent_trades.map(trade => `
                <div class="trade-item">
                    <div class="trade-item-header">
                        <strong>Market: ${trade.market_id}</strong>
                        <span>${formatTime(trade.timestamp)}</span>
                    </div>
                    <div class="trade-details">
                        <div class="detail-item">
                            <span class="detail-label">Side</span>
                            <span class="detail-value">${trade.side}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Price</span>
                            <span class="detail-value">${trade.price.toFixed(4)}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Size</span>
                            <span class="detail-value">${trade.size.toFixed(2)}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Value</span>
                            <span class="detail-value">${formatCurrency(trade.value)}</span>
                        </div>
                    </div>
                </div>
            `).join('')}
        </div>
    `;

    modal.style.display = 'block';
}

// Track new wallet
async function trackNewWallet() {
    const addressInput = document.getElementById('wallet-address-input');
    const resultDiv = document.getElementById('track-result');
    const address = addressInput.value.trim();

    if (!address) {
        showMessage(resultDiv, 'Please enter a wallet address', 'error');
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/track/wallet/${address}`, {
            method: 'POST'
        });
        const data = await response.json();

        if (data.success) {
            showMessage(resultDiv, data.message, 'success');
            addressInput.value = '';
            // Refresh whales data after adding new wallet
            setTimeout(() => {
                loadWhalesData();
                loadSummaryStats();
            }, 2000);
        } else {
            showMessage(resultDiv, data.message, 'error');
        }
    } catch (error) {
        console.error('Error tracking wallet:', error);
        showMessage(resultDiv, 'Error tracking wallet', 'error');
    }
}

// Refresh all data
async function refreshAllData() {
    const btn = document.getElementById('refresh-btn');
    btn.disabled = true;
    btn.textContent = '⏳ Refreshing...';

    try {
        await fetch(`${API_BASE}/refresh`, { method: 'POST' });
        await loadSummaryStats();
        await refreshCurrentTab();
    } catch (error) {
        console.error('Error refreshing data:', error);
    } finally {
        btn.disabled = false;
        btn.textContent = '🔄 Refresh Data';
    }
}

// Refresh current tab
function refreshCurrentTab() {
    switch(currentTab) {
        case 'whales':
            loadWhalesData();
            break;
        case 'bets':
            loadBetsData();
            break;
        case 'markets':
            loadMarketsData();
            break;
    }
}

// Utility functions
function formatCurrency(value) {
    if (!value && value !== 0) return '-';
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 0,
        maximumFractionDigits: 2
    }).format(value);
}

function formatTime(timestamp) {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    const now = new Date();
    const diff = now - date;

    if (diff < 60000) return 'Just now';
    if (diff < 3600000) return `${Math.floor(diff / 60000)} min ago`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)} hours ago`;

    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
}

function shortenAddress(address) {
    if (!address) return '-';
    return `${address.slice(0, 6)}...${address.slice(-4)}`;
}

function showMessage(element, message, type) {
    element.textContent = message;
    element.className = `result-message ${type}`;
    element.style.display = 'block';

    setTimeout(() => {
        element.style.display = 'none';
    }, 5000);
}