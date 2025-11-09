import React, { useState, useCallback } from 'react';
import {
  Box,
  Paper,
  Typography,
  IconButton,
  TextField,
  Button,
  Chip,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import {
  Star as StarIcon,
  StarBorder as StarBorderIcon,
  Edit as EditIcon,
  Launch as LaunchIcon,
  TrendingUp,
  TrendingDown,
  MoreVert,
} from '@mui/icons-material';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import axios from 'axios';
import toast from 'react-hot-toast';
import numeral from 'numeral';

const UsersTable = ({ onUserSelect, selectedUser }) => {
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [sortModel, setSortModel] = useState([{ field: 'total_value', sort: 'desc' }]);
  const [aliasDialog, setAliasDialog] = useState({ open: false, user: null });
  const [newAlias, setNewAlias] = useState('');
  const queryClient = useQueryClient();

  // Fetch users data
  const { data: usersData, isLoading, error } = useQuery({
    queryKey: ['users', page, pageSize, sortModel],
    queryFn: async () => {
      const response = await axios.get('/api/users', {
        params: {
          page: page + 1,
          limit: pageSize,
          sort_by: sortModel[0]?.field || 'total_value',
          sort_order: sortModel[0]?.sort?.toUpperCase() || 'DESC',
        },
      });
      return response.data;
    },
  });

  // Star/unstar user mutation
  const starMutation = useMutation({
    mutationFn: async ({ walletAddress, isStarred, customAlias }) => {
      const response = await axios.post(`/api/users/${walletAddress}/star`, {
        is_starred: isStarred,
        custom_alias: customAlias,
      });
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries(['users']);
      toast.success('User updated successfully');
    },
  });

  const handleStarToggle = useCallback((user) => {
    starMutation.mutate({
      walletAddress: user.proxy_wallet,
      isStarred: !user.is_starred,
      customAlias: user.custom_alias,
    });
  }, [starMutation]);

  const handleAliasEdit = useCallback((user) => {
    setAliasDialog({ open: true, user });
    setNewAlias(user.custom_alias || '');
  }, []);

  const handleAliasSave = useCallback(() => {
    if (aliasDialog.user) {
      starMutation.mutate({
        walletAddress: aliasDialog.user.proxy_wallet,
        isStarred: aliasDialog.user.is_starred,
        customAlias: newAlias,
      });
      setAliasDialog({ open: false, user: null });
      setNewAlias('');
    }
  }, [aliasDialog.user, newAlias, starMutation]);

  const formatValue = (value) => {
    if (value == null) return '-';
    return numeral(value).format('$0,0.00');
  };

  const formatPercentage = (value) => {
    if (value == null) return '-';
    const formatted = numeral(value).format('0.00%');
    return value >= 0 ? `+${formatted}` : formatted;
  };

  const columns = [
    {
      field: 'is_starred',
      headerName: '★',
      width: 50,
      sortable: false,
      renderCell: (params) => (
        <IconButton
          size="small"
          onClick={(e) => {
            e.stopPropagation();
            handleStarToggle(params.row);
          }}
          sx={{ 
            color: params.value ? '#ffa500' : '#333',
            padding: 0,
          }}
        >
          {params.value ? <StarIcon fontSize="small" /> : <StarBorderIcon fontSize="small" />}
        </IconButton>
      ),
    },
    {
      field: 'proxy_wallet',
      headerName: 'WALLET',
      width: 150,
      renderCell: (params) => (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography variant="body2" sx={{ fontFamily: 'monospace', color: '#00ff00' }}>
            {params.value.substring(0, 6)}...{params.value.substring(params.value.length - 4)}
          </Typography>
          <Tooltip title="View on Etherscan">
            <IconButton
              size="small"
              onClick={(e) => {
                e.stopPropagation();
                window.open(`https://etherscan.io/address/${params.value}`, '_blank');
              }}
              sx={{ padding: 0, color: '#666' }}
            >
              <LaunchIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Box>
      ),
    },
    {
      field: 'alias',
      headerName: 'ALIAS',
      width: 150,
      renderCell: (params) => (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography variant="body2">
            {params.row.custom_alias || params.row.username || '-'}
          </Typography>
          <IconButton
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              handleAliasEdit(params.row);
            }}
            sx={{ padding: 0, color: '#666' }}
          >
            <EditIcon fontSize="small" />
          </IconButton>
        </Box>
      ),
    },
    {
      field: 'total_value',
      headerName: 'TOTAL VALUE',
      width: 150,
      type: 'number',
      renderCell: (params) => (
        <Typography
          variant="body2"
          sx={{
            color: params.value > 100000 ? '#00ff00' : '#ffffff',
            fontWeight: params.value > 100000 ? 700 : 400,
          }}
        >
          {formatValue(params.value)}
        </Typography>
      ),
    },
    {
      field: 'positions_value',
      headerName: 'POSITIONS',
      width: 130,
      type: 'number',
      renderCell: (params) => (
        <Typography variant="body2">
          {formatValue(params.value)}
        </Typography>
      ),
    },
    {
      field: 'total_pnl',
      headerName: 'P&L',
      width: 130,
      type: 'number',
      renderCell: (params) => (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          {params.value > 0 ? (
            <TrendingUp sx={{ fontSize: 16, color: '#00ff00' }} />
          ) : params.value < 0 ? (
            <TrendingDown sx={{ fontSize: 16, color: '#ff0000' }} />
          ) : null}
          <Typography
            variant="body2"
            sx={{
              color: params.value > 0 ? '#00ff00' : params.value < 0 ? '#ff0000' : '#ffffff',
              fontWeight: 600,
            }}
          >
            {formatValue(params.value)}
          </Typography>
        </Box>
      ),
    },
    {
      field: 'active_positions',
      headerName: 'ACTIVE',
      width: 100,
      type: 'number',
      renderCell: (params) => (
        <Chip
          label={params.value || 0}
          size="small"
          sx={{
            bgcolor: params.value > 0 ? 'rgba(0, 255, 0, 0.1)' : 'transparent',
            color: params.value > 0 ? '#00ff00' : '#666',
            border: '1px solid',
            borderColor: params.value > 0 ? '#00ff00' : '#333',
            borderRadius: 0,
            fontSize: '0.75rem',
          }}
        />
      ),
    },
    {
      field: 'actions',
      headerName: 'ACTIONS',
      width: 80,
      sortable: false,
      renderCell: (params) => (
        <IconButton
          size="small"
          onClick={(e) => {
            e.stopPropagation();
            // Add more actions menu here
          }}
          sx={{ color: '#666' }}
        >
          <MoreVert fontSize="small" />
        </IconButton>
      ),
    },
  ];

  const rows = usersData?.users || [];
  const totalRows = usersData?.total || 0;

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
          USERS TRACKER
        </Typography>
        <Box sx={{ flexGrow: 1 }} />
        <Typography variant="caption" sx={{ color: '#666' }}>
          TOTAL: {totalRows.toLocaleString()} USERS
        </Typography>
      </Box>

      <DataGrid
        rows={rows}
        columns={columns}
        getRowId={(row) => row.proxy_wallet}
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
        onRowClick={(params) => onUserSelect(params.row)}
        disableColumnMenu
        density="compact"
        sx={{
          border: 'none',
          '& .MuiDataGrid-main': {
            bgcolor: '#000000',
          },
          '& .MuiDataGrid-cell': {
            borderBottom: '1px solid #1a1a1a',
            borderRight: '1px solid #1a1a1a',
            fontSize: '0.75rem',
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

      {/* Alias Edit Dialog */}
      <Dialog
        open={aliasDialog.open}
        onClose={() => setAliasDialog({ open: false, user: null })}
        PaperProps={{
          sx: {
            bgcolor: '#0a0a0a',
            border: '2px solid #333',
            borderRadius: 0,
            minWidth: 400,
          },
        }}
      >
        <DialogTitle sx={{ borderBottom: '1px solid #333' }}>
          EDIT ALIAS
        </DialogTitle>
        <DialogContent sx={{ mt: 2 }}>
          <TextField
            autoFocus
            fullWidth
            label="Custom Alias"
            value={newAlias}
            onChange={(e) => setNewAlias(e.target.value)}
            variant="outlined"
            sx={{
              '& .MuiOutlinedInput-root': {
                borderRadius: 0,
                '& fieldset': {
                  borderColor: '#333',
                },
                '&:hover fieldset': {
                  borderColor: '#666',
                },
                '&.Mui-focused fieldset': {
                  borderColor: '#00ff00',
                },
              },
            }}
          />
        </DialogContent>
        <DialogActions sx={{ borderTop: '1px solid #333', p: 2 }}>
          <Button
            onClick={() => setAliasDialog({ open: false, user: null })}
            sx={{ borderRadius: 0 }}
          >
            CANCEL
          </Button>
          <Button
            onClick={handleAliasSave}
            variant="contained"
            sx={{
              borderRadius: 0,
              bgcolor: '#00ff00',
              color: '#000',
              '&:hover': {
                bgcolor: '#00cc00',
              },
            }}
          >
            SAVE
          </Button>
        </DialogActions>
      </Dialog>
    </Paper>
  );
};

export default UsersTable;