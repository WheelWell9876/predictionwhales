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
  Schedule,
  AttachMoney,
  LocalOffer,
} from '@mui/icons-material';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import numeral from 'numeral';
import { format } from 'date-fns';

const EventsTable = ({ onEventSelect, selectedEvent }) => {
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [sortModel, setSortModel] = useState([{ field: 'volume', sort: 'desc' }]);

  // Fetch events data
  const { data: eventsData, isLoading, error } = useQuery({
    queryKey: ['events', page, pageSize, sortModel],
    queryFn: async () => {
      const response = await axios.get('/api/events', {
        params: {
          page: page + 1,
          limit: pageSize,
          sort_by: sortModel[0]?.field || 'volume',
          sort_order: sortModel[0]?.sort?.toUpperCase() || 'DESC',
          active_only: true,
        },
      });
      return response.data;
    },
  });

  const formatValue = (value) => {
    if (value == null) return '-';
    return numeral(value).format('$0,0.00');
  };

  const formatCompact = (value) => {
    if (value == null) return '-';
    return numeral(value).format('$0.0a').toUpperCase();
  };

  const formatPercentage = (value) => {
    if (value == null) return '-';
    return numeral(value).format('0.00%');
  };

  const columns = [
    {
      field: 'title',
      headerName: 'EVENT',
      flex: 1,
      minWidth: 300,
      renderCell: (params) => (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, py: 0.5 }}>
          <Typography variant="body2" sx={{ fontWeight: 600, color: '#fff' }}>
            {params.value?.substring(0, 100)}
          </Typography>
          {params.row.ticker && (
            <Box sx={{ display: 'flex', gap: 0.5, alignItems: 'center' }}>
              <Chip
                label={params.row.ticker}
                size="small"
                sx={{
                  height: 18,
                  fontSize: '0.65rem',
                  bgcolor: 'rgba(0, 255, 0, 0.1)',
                  color: '#00ff00',
                  border: '1px solid #00ff00',
                  borderRadius: 0,
                }}
              />
              {params.row.market_count > 0 && (
                <Typography variant="caption" sx={{ color: '#666' }}>
                  {params.row.market_count} markets
                </Typography>
              )}
            </Box>
          )}
        </Box>
      ),
    },
    {
      field: 'volume',
      headerName: 'VOLUME',
      width: 120,
      type: 'number',
      renderCell: (params) => (
        <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end' }}>
          <Typography
            variant="body2"
            sx={{
              color: params.value > 1000000 ? '#00ff00' : '#ffffff',
              fontWeight: params.value > 1000000 ? 700 : 400,
            }}
          >
            {formatCompact(params.value)}
          </Typography>
          {params.row.volume_24hr && (
            <Typography variant="caption" sx={{ color: '#666' }}>
              24h: {formatCompact(params.row.volume_24hr)}
            </Typography>
          )}
        </Box>
      ),
    },
    {
      field: 'liquidity',
      headerName: 'LIQUIDITY',
      width: 110,
      type: 'number',
      renderCell: (params) => (
        <Typography
          variant="body2"
          sx={{
            color: params.value > 100000 ? '#00bfff' : '#ffffff',
            fontWeight: params.value > 100000 ? 600 : 400,
          }}
        >
          {formatCompact(params.value)}
        </Typography>
      ),
    },
    {
      field: 'competitive',
      headerName: 'COMP',
      width: 80,
      type: 'number',
      renderCell: (params) => {
        const value = params.value || 0;
        let color = '#ffffff';
        if (value > 0.7) color = '#00ff00';
        else if (value > 0.4) color = '#ffa500';
        else color = '#ff0000';
        
        return (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <Box
              sx={{
                width: 40,
                height: 4,
                bgcolor: '#1a1a1a',
                position: 'relative',
              }}
            >
              <Box
                sx={{
                  position: 'absolute',
                  left: 0,
                  top: 0,
                  height: '100%',
                  width: `${value * 100}%`,
                  bgcolor: color,
                }}
              />
            </Box>
            <Typography variant="caption" sx={{ color }}>
              {formatPercentage(value)}
            </Typography>
          </Box>
        );
      },
    },
    {
      field: 'comment_count',
      headerName: 'ACTIVITY',
      width: 90,
      type: 'number',
      renderCell: (params) => (
        <Chip
          label={params.value || 0}
          size="small"
          icon={<LocalOffer sx={{ fontSize: 12 }} />}
          sx={{
            height: 20,
            fontSize: '0.7rem',
            bgcolor: params.value > 10 ? 'rgba(255, 165, 0, 0.1)' : 'transparent',
            color: params.value > 10 ? '#ffa500' : '#666',
            border: '1px solid',
            borderColor: params.value > 10 ? '#ffa500' : '#333',
            borderRadius: 0,
          }}
        />
      ),
    },
    {
      field: 'end_date',
      headerName: 'ENDS',
      width: 100,
      renderCell: (params) => {
        if (!params.value) return <Typography variant="caption" sx={{ color: '#666' }}>-</Typography>;
        
        const endDate = new Date(params.value);
        const now = new Date();
        const daysLeft = Math.ceil((endDate - now) / (1000 * 60 * 60 * 24));
        
        let color = '#666';
        if (daysLeft <= 1) color = '#ff0000';
        else if (daysLeft <= 7) color = '#ffa500';
        else color = '#00ff00';
        
        return (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <Schedule sx={{ fontSize: 14, color }} />
            <Typography variant="caption" sx={{ color }}>
              {daysLeft > 0 ? `${daysLeft}d` : 'Ended'}
            </Typography>
          </Box>
        );
      },
    },
    {
      field: 'status',
      headerName: 'STATUS',
      width: 80,
      renderCell: (params) => {
        const isActive = params.row.active;
        const isClosed = params.row.closed;
        
        let status = 'ACTIVE';
        let color = '#00ff00';
        
        if (isClosed) {
          status = 'CLOSED';
          color = '#ff0000';
        } else if (!isActive) {
          status = 'PAUSED';
          color = '#ffa500';
        }
        
        return (
          <Chip
            label={status}
            size="small"
            sx={{
              height: 20,
              fontSize: '0.65rem',
              bgcolor: `${color}20`,
              color: color,
              border: `1px solid ${color}`,
              borderRadius: 0,
              fontWeight: 700,
            }}
          />
        );
      },
    },
    {
      field: 'link',
      headerName: '',
      width: 50,
      sortable: false,
      renderCell: (params) => (
        <Tooltip title="View on Polymarket">
          <IconButton
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              window.open(`https://polymarket.com/event/${params.row.slug}`, '_blank');
            }}
            sx={{ padding: 0, color: '#666' }}
          >
            <LaunchIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      ),
    },
  ];

  const rows = eventsData?.events || [];
  const totalRows = eventsData?.total || 0;

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
          EVENTS MONITOR
        </Typography>
        <Box sx={{ flexGrow: 1 }} />
        <Typography variant="caption" sx={{ color: '#666' }}>
          TOTAL: {totalRows.toLocaleString()} EVENTS
        </Typography>
      </Box>

      <DataGrid
        rows={rows}
        columns={columns}
        getRowId={(row) => row.id}
        loading={isLoading}
        paginationMode="server"
        rowCount={totalRows}
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
        onRowClick={(params) => onEventSelect(params.row)}
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

export default EventsTable;