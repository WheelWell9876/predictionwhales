import React, { useState, useEffect } from 'react';
import { Box, Paper, Grid, Typography, Tab, Tabs } from '@mui/material';
import { useSearchParams } from 'react-router-dom';
import UsersTable from '../components/Tables/UsersTable';
import EventsTable from '../components/Tables/EventsTable';
import MarketsTable from '../components/Tables/MarketsTable';
import TradingChart from '../components/Charts/TradingChart';
import UserDetail from '../components/Details/UserDetail';
import EventDetail from '../components/Details/EventDetail';
import MarketDetail from '../components/Details/MarketDetail';
import { styled } from '@mui/material/styles';

const StyledTab = styled(Tab)(({ theme }) => ({
  minHeight: 32,
  fontSize: '0.75rem',
  fontWeight: 600,
  color: '#666',
  borderBottom: '2px solid transparent',
  '&.Mui-selected': {
    color: '#00ff00',
    borderBottomColor: '#00ff00',
  },
  '&:hover': {
    color: '#b0b0b0',
  },
}));

const TradePage = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [selectedView, setSelectedView] = useState(searchParams.get('view') || 'users');
  const [selectedItem, setSelectedItem] = useState(null);
  const [detailExpanded, setDetailExpanded] = useState(false);

  useEffect(() => {
    const view = searchParams.get('view');
    if (view) {
      setSelectedView(view);
    }
  }, [searchParams]);

  const handleViewChange = (event, newValue) => {
    setSelectedView(newValue);
    setSearchParams({ view: newValue });
    setSelectedItem(null);
    setDetailExpanded(false);
  };

  const handleItemSelect = (item) => {
    setSelectedItem(item);
    setDetailExpanded(true);
  };

  const renderTable = () => {
    switch (selectedView) {
      case 'users':
        return (
          <UsersTable 
            onUserSelect={handleItemSelect}
            selectedUser={selectedItem}
          />
        );
      case 'events':
        return (
          <EventsTable 
            onEventSelect={handleItemSelect}
            selectedEvent={selectedItem}
          />
        );
      case 'markets':
        return (
          <MarketsTable 
            onMarketSelect={handleItemSelect}
            selectedMarket={selectedItem}
          />
        );
      case 'series':
        // Series component would go here
        return (
          <Paper sx={{ p: 3, bgcolor: '#000', border: '2px solid #333' }}>
            <Typography>Series View - Coming Soon</Typography>
          </Paper>
        );
      case 'tags':
        // Tags component would go here
        return (
          <Paper sx={{ p: 3, bgcolor: '#000', border: '2px solid #333' }}>
            <Typography>Tags View - Coming Soon</Typography>
          </Paper>
        );
      default:
        return <UsersTable onUserSelect={handleItemSelect} />;
    }
  };

  const renderDetail = () => {
    if (!selectedItem) return null;

    switch (selectedView) {
      case 'users':
        return <UserDetail user={selectedItem} />;
      case 'events':
        return <EventDetail event={selectedItem} />;
      case 'markets':
        return <MarketDetail market={selectedItem} />;
      default:
        return null;
    }
  };

  const renderChart = () => {
    if (!selectedItem) {
      return <TradingChart title="MARKET OVERVIEW" />;
    }

    switch (selectedView) {
      case 'users':
        return (
          <TradingChart 
            title={`PORTFOLIO: ${selectedItem.custom_alias || selectedItem.username || selectedItem.proxy_wallet.substring(0, 10)}`}
            data={selectedItem.chartData}
          />
        );
      case 'events':
        return (
          <TradingChart 
            title={`EVENT: ${selectedItem.title?.substring(0, 50)}`}
            data={selectedItem.chartData}
          />
        );
      case 'markets':
        return (
          <TradingChart 
            title={`MARKET: ${selectedItem.question?.substring(0, 50)}`}
            data={selectedItem.chartData}
          />
        );
      default:
        return <TradingChart title="OVERVIEW" />;
    }
  };

  return (
    <Box sx={{ height: 'calc(100vh - 100px)', display: 'flex', flexDirection: 'column' }}>
      {/* View Tabs */}
      <Paper
        elevation={0}
        sx={{
          bgcolor: '#0a0a0a',
          border: '1px solid #333',
          borderRadius: 0,
          mb: 2,
        }}
      >
        <Tabs
          value={selectedView}
          onChange={handleViewChange}
          variant="scrollable"
          scrollButtons="auto"
          sx={{
            minHeight: 32,
            '& .MuiTabs-indicator': {
              backgroundColor: '#00ff00',
              height: 2,
            },
          }}
        >
          <StyledTab label="USERS" value="users" />
          <StyledTab label="EVENTS" value="events" />
          <StyledTab label="MARKETS" value="markets" />
          <StyledTab label="SERIES" value="series" />
          <StyledTab label="TAGS" value="tags" />
        </Tabs>
      </Paper>

      {/* Main Content Grid */}
      <Grid container spacing={2} sx={{ flexGrow: 1, overflow: 'hidden' }}>
        {/* Left Panel - Table */}
        <Grid 
          item 
          xs={12} 
          md={detailExpanded ? 6 : 12}
          sx={{ 
            height: detailExpanded ? '50%' : '100%',
            transition: 'all 0.3s ease',
          }}
        >
          <Box sx={{ height: '100%', overflow: 'auto' }}>
            {renderTable()}
          </Box>
        </Grid>

        {/* Right Panel - Chart and Details */}
        {detailExpanded && (
          <Grid item xs={12} md={6} sx={{ height: '100%' }}>
            <Grid container spacing={2} sx={{ height: '100%' }}>
              {/* Chart */}
              <Grid item xs={12} sx={{ height: '60%' }}>
                {renderChart()}
              </Grid>
              
              {/* Details Panel */}
              <Grid item xs={12} sx={{ height: '40%', overflow: 'auto' }}>
                {renderDetail()}
              </Grid>
            </Grid>
          </Grid>
        )}
      </Grid>
    </Box>
  );
};

export default TradePage;