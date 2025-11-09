import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Chip,
  IconButton,
  Tooltip,
} from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import {
  Launch as LaunchIcon,
  TrendingUp,
  TrendingDown,
  ShowChart,
  Gavel,
} from '@mui/icons-material';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import numeral from 'numeral';

const MarketsTable = ({ onMarketSelect, selectedMarket, eventId }) => {
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [sortModel, setSortModel] = useState([{ field: 'volume', sort: 'desc' }]);

  // Fetch markets data
  const { data: marketsData, isLoading, error } = useQuery({
    queryKey: ['markets', page, pageSize, sortModel, eventId],
    queryFn: async () => {
      const response = await axios.get('/api/markets', {
        params: {
          page: page + 1,
          limit: pageSize,
          sort_by: sortModel[0]?.field || 'volume',
          sort_order: sortModel[0]?.sort?.toUpperCase() || 'DESC',
          event_id: eventId,
        },
      });
      return response.data;
    },
  });

  const formatPrice = (value) => {
    if (value == null) return '-';
    return numeral(value).format('0.000');
  };

  const formatCompact = (value) => {
    if (value == null) return '-';
    return numeral(value).format('$0.0a').toUpperCase();
  };

  const formatSpread = (value) => {
    if (value == null) return '-';
    return numeral(value).format('0.0000');
  };

  const formatPercentChange = (value) => {
    if (value == null) return '-';
    const formatted = numeral(value).format('0.00%');
    return value >= 0 ? `+${formatted}` : formatted;
  };

  const columns = [
    {
      field: 'question',
      headerName: 'MARKET',
      flex: 1,
      minWidth: 350,
      renderCell: (params) => (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, py: 0.5 }}>
          <Typography variant="body2" sx={{ fontWeight: 600, color: '#fff' }}>
            {params.value?.substring(0, 150)}
          </Typography>
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            {params.row.outcomes && (
              <Box sx={{ display: 'flex', gap: 0.5 }}>
                {JSON.parse(params.row.outcomes || '[]').slice(0, 2).map((outcome, idx) => (
                  <Chip
                    key={idx}
                    label={outcome}
                    size="small"
                    sx={{
                      height: 18,
                      fontSize: '0.65rem',
                      bgcolor: idx === 0 ? 'rgba(0, 255, 0, 0.1)' : 'rgba(255, 0, 0, 0.1)',
                      color: idx === 0 ? '#00ff00' : '#ff0000',
                      border: `1px solid ${idx === 0 ? '#00ff00' : '#ff0000'}`,
                      borderRadius: 0,
                    }}
                  />
                ))}
              </Box>
            )}
            {params.row.neg_risk && (
              <Chip
                label="NEG RISK"
                size="small"
                sx={{
                  height: 18,
                  fontSize: '0.6rem',
                  bgcolor: 'rgba(255, 165, 0, 0.1)',
                  color: '#ffa500',
                  border: '1px solid #ffa500',
                  borderRadius: 0,
                }}
              />
            )}
          </Box>
        </Box>
      ),
    },
    {
      field: 'last_trade_price',
      headerName: 'PRICE',
      width: 100,
      type: 'number',
      renderCell: (params) => (
        <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end' }}>
          <Typography
            variant="body2"
            sx={{
              color: '#fff',
              fontWeight: 600,
              fontFamily: 'monospace',
            }}
          >
            ${formatPrice(params.value)}
          </Typography>
          {params.row.one_day_price_change != null && (
            <Typography
              variant="caption"
              sx={{
                color: params.row.one_day_price_change >= 0 ? '#00ff00' : '#ff0000',
                fontWeight: 600,
              }}
            >
              {formatPercentChange(params.row.one_day_price_change)}
            </Typography>
          )}
        </Box>
      ),
    },
    {
      field: 'volume',
      headerName: 'VOLUME',
      width: 100,
      type: 'number',
      renderCell: (params) => (
        <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end' }}>
          <Typography
            variant="body2"
            sx={{
              color: params.value > 100000 ? '#00ff00' : '#ffffff',
              fontWeight: params.value > 100000 ? 700 : 400,
            }}
          >
            {formatCompact(params.value)}
          </Typography>
          {params.row.volume_24hr && (
            <Typography variant="caption" sx={{ color: '#666', fontSize: '0.65rem' }}>
              24h: {formatCompact(params.row.volume_24hr)}
            </Typography>
          )}
        </Box>
      ),
    },
    {
      field: 'liquidity',
      headerName: 'LIQUIDITY',
      width: 100,
      type: 'number',
      renderCell: (params) => (
        <Typography
          variant="body2"
          sx={{
            color: params.value > 50000 ? '#00bfff' : '#ffffff',
            fontWeight: params.value > 50000 ? 600 : 400,
          }}
        >
          {formatCompact(params.value)}
        </Typography>
      ),
    },
    {
      field: 'spread',
      headerName: 'SPREAD',
      width: 80,
      type: 'number',
      renderCell: (params) => {
        const spread = params.value || (params.row.best_ask - params.row.best_bid);
        let color = '#ffffff';
        if (spread < 0.01) color = '#00ff00';
        else if (spread < 0.05) color = '#ffa500';
        else color = '#ff0000';
        
        return (
          <Typography
            variant="body2"
            sx={{
              color,
              fontFamily: 'monospace',
              fontWeight: 600,
            }}
          >
            {formatSpread(spread)}
          </Typography>
        );
      },
    },
    {
      field: 'best_bid',
      headerName: 'BID',
      width: 70,
      type: 'number',
      renderCell: (params) => (
        <Typography
          variant="body2"
          sx={{
            color: '#00ff00',
            fontFamily: 'monospace',
            fontSize: '0.7rem',
          }}
        >
          {formatPrice(params.value)}
        </Typography>
      ),
    },
    {
      field: 'best_ask',
      headerName: 'ASK',
      width: 70,
      type: 'number',
      renderCell: (params) => (
        <Typography
          variant="body2"
          sx={{
            color: '#ff0000',
            fontFamily: 'monospace',
            fontSize: '0.7rem',
          }}
        >
          {formatPrice(params.value)}
        </Typography>
      ),
    },
    {
      field: 'active',
      headerName: 'STATUS',
      width: 80,
      renderCell: (params) => {
        let status = 'ACTIVE';
        let color = '#00ff00';
        let icon = <ShowChart sx={{ fontSize: 12 }} />;
        
        if (params.row.closed) {
          status = 'CLOSED';
          color = '#ff0000';
          icon = <Gavel sx={{ fontSize: 12 }} />;
        } else if (!params.value) {
          status = 'PAUSED';
          color = '#ffa500';
        }
        
        return (
          <Chip
            label={status}
            icon={icon}
            size="small"
            sx={{
              height: 20,
              fontSize: '0.65rem',
              bgcolor: `${color}20`,
              color: color,
              border: `1px solid ${color}`,
              borderRadius: 0,
              fontWeight: 700,
              '& .MuiChip-icon': {
                color: color,
              },
            }}
          />
        );
      },
    },
    {
      field: 'link',
      headerName: '',
      width: 40,
      sortable: false,
      renderCell: (params) => (
        <Tooltip title="View on Polymarket">
          <IconButton
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              window.open(`https://polymarket.com/market/${params.row.slug}`, '_blank');
            }}
            sx={{ padding: 0, color: '#666' }}
          >
            <LaunchIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      ),
    },
  ];

  const rows = marketsData?.markets || [];

  return (
    <Paper
      elevation={0}
      sx={{
        height: '100%',
        bgcolor: '#000000',
        border: '2px solid #333',
        borderRadius: 0,
        p: 0,
      }}
    >
      <Box
        sx={{
          height: 48,
          bgcolor: '#0a0a0a',
          borderBottom: '2px solid #333',
          display: 'flex',
          alignItems: 'center',
          px: 2,
        }}
      >
        <Typography
          variant="h6"
          sx={{
            fontWeight: 700,
            fontSize: '0.875rem',
            color: '#00ff00',
            letterSpacing: 1,
          }}
        >
          MARKETS ORDERBOOK
        </Typography>
        <Box sx={{ flexGrow: 1 }} />
        <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <Box sx={{ width: 8, height: 8, bgcolor: '#00ff00', borderRadius: '50%' }} />
            <Typography variant="caption" sx={{ color: '#666' }}>
              LIVE
            </Typography>
          </Box>
          <Typography variant="caption" sx={{ color: '#666' }}>
            MARKETS: {rows.length}
          </Typography>
        </Box>
      </Box>

      <DataGrid
        rows={rows}
        columns={columns}
        getRowId={(row) => row.id}
        loading={isLoading}
        paginationMode="server"
        rowCount={rows.length}
        pageSizeOptions={[25, 50, 100]}
        paginationModel={{
          page,
          pageSize,
        }}
        onPaginationModelChange={(model) => {
          setPage(model.page);
          setPageSize(model.pageSize);
        }}
        sortingMode="server"
        sortModel={sortModel}
        onSortModelChange={(model) => setSortModel(model)}
        onRowClick={(params) => onMarketSelect(params.row)}
        disableColumnMenu
        getRowHeight={() => 'auto'}
        sx={{
          border: 'none',
          '& .MuiDataGrid-main': {
            bgcolor: '#000000',
          },
          '& .MuiDataGrid-cell': {
            borderBottom: '1px solid #1a1a1a',
            borderRight: '1px solid #1a1a1a',
            fontSize: '0.75rem',
            py: 1,
          },
          '& .MuiDataGrid-columnHeaders': {
            bgcolor: '#0a0a0a',
            borderBottom: '2px solid #333',
            minHeight: '40px !important',
            maxHeight: '40px !important',
          },
          '& .MuiDataGrid-columnHeader': {
            height: '40px',
          },
          '& .MuiDataGrid-columnHeaderTitle': {
            fontSize: '0.75rem',
            fontWeight: 700,
            letterSpacing: 0.5,
          },
          '& .MuiDataGrid-row': {
            cursor: 'pointer',
            '&:hover': {
              bgcolor: 'rgba(0, 255, 0, 0.02)',
            },
            '&.Mui-selected': {
              bgcolor: 'rgba(0, 255, 0, 0.05)',
              '&:hover': {
                bgcolor: 'rgba(0, 255, 0, 0.08)',
              },
            },
          },
          '& .MuiDataGrid-footerContainer': {
            borderTop: '2px solid #333',
            bgcolor: '#0a0a0a',
          },
          '& .MuiTablePagination-root': {
            color: '#b0b0b0',
          },
          '& .MuiDataGrid-selectedRowCount': {
            display: 'none',
          },
        }}
      />
    </Paper>
  );
};

export default MarketsTable;