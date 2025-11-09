import React from 'react';
import {
  Box,
  Paper,
  Typography,
  Divider,
  Chip,
  Grid,
  List,
  ListItem,
  ListItemText,
  IconButton,
  Tooltip,
} from '@mui/material';
import {
  AccountBalance,
  TrendingUp,
  TrendingDown,
  Launch,
  ContentCopy,
  Timeline,
} from '@mui/icons-material';
import numeral from 'numeral';
import { format } from 'date-fns';

const UserDetail = ({ user }) => {
  if (!user) return null;

  const formatValue = (value) => {
    if (value == null) return '-';
    return numeral(value).format('$0,0.00');
  };

  const formatPercentage = (value) => {
    if (value == null) return '-';
    const formatted = numeral(value).format('0.00%');
    return value >= 0 ? `+${formatted}` : formatted;
  };

  const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text);
    // You could add a toast notification here
  };

  // Sample positions data (would come from API)
  const positions = user.positions || [
    {
      title: 'Will BTC reach $100k by EOY 2024?',
      outcome: 'Yes',
      size: 1000,
      avgPrice: 0.45,
      currentPrice: 0.62,
      pnl: 377.78,
      pnlPercent: 37.78,
    },
    {
      title: 'US Presidential Election 2024',
      outcome: 'Trump',
      size: 500,
      avgPrice: 0.38,
      currentPrice: 0.41,
      pnl: 15.00,
      pnlPercent: 7.89,
    },
  ];

  return (
    <Paper
      elevation={0}
      sx={{
        bgcolor: '#000000',
        border: '2px solid #333',
        borderRadius: 0,
        height: '100%',
        overflow: 'auto',
      }}
    >
      {/* Header */}
      <Box
        sx={{
          bgcolor: '#0a0a0a',
          borderBottom: '2px solid #333',
          p: 2,
        }}
      >
        <Typography
          variant="h6"
          sx={{
            fontWeight: 700,
            fontSize: '0.875rem',
            color: '#00ff00',
            letterSpacing: 1,
            mb: 1,
          }}
        >
          USER PROFILE
        </Typography>
        
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <Box sx={{ flex: 1 }}>
            <Typography variant="body2" sx={{ color: '#666', fontSize: '0.7rem' }}>
              WALLET ADDRESS
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Typography
                variant="body1"
                sx={{ fontFamily: 'monospace', color: '#fff' }}
              >
                {user.proxy_wallet}
              </Typography>
              <Tooltip title="Copy address">
                <IconButton
                  size="small"
                  onClick={() => copyToClipboard(user.proxy_wallet)}
                  sx={{ padding: 0, color: '#666' }}
                >
                  <ContentCopy fontSize="small" />
                </IconButton>
              </Tooltip>
              <Tooltip title="View on Etherscan">
                <IconButton
                  size="small"
                  onClick={() => window.open(`https://etherscan.io/address/${user.proxy_wallet}`, '_blank')}
                  sx={{ padding: 0, color: '#666' }}
                >
                  <Launch fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>
          </Box>
          
          {user.custom_alias && (
            <Chip
              label={user.custom_alias}
              sx={{
                bgcolor: 'rgba(255, 165, 0, 0.1)',
                color: '#ffa500',
                border: '1px solid #ffa500',
                borderRadius: 0,
              }}
            />
          )}
          
          {user.is_starred && (
            <Chip
              label="★ STARRED"
              sx={{
                bgcolor: 'rgba(255, 165, 0, 0.1)',
                color: '#ffa500',
                border: '1px solid #ffa500',
                borderRadius: 0,
              }}
            />
          )}
        </Box>
      </Box>

      {/* Stats Grid */}
      <Grid container spacing={0} sx={{ borderBottom: '1px solid #333' }}>
        <Grid item xs={6} sx={{ borderRight: '1px solid #333' }}>
          <Box sx={{ p: 2 }}>
            <Typography variant="caption" sx={{ color: '#666' }}>
              TOTAL VALUE
            </Typography>
            <Typography
              variant="h6"
              sx={{ color: '#00ff00', fontWeight: 700, fontFamily: 'monospace' }}
            >
              {formatValue(user.total_value)}
            </Typography>
          </Box>
        </Grid>
        <Grid item xs={6}>
          <Box sx={{ p: 2 }}>
            <Typography variant="caption" sx={{ color: '#666' }}>
              TOTAL P&L
            </Typography>
            <Typography
              variant="h6"
              sx={{
                color: user.total_pnl >= 0 ? '#00ff00' : '#ff0000',
                fontWeight: 700,
                fontFamily: 'monospace',
              }}
            >
              {formatValue(user.total_pnl)}
            </Typography>
          </Box>
        </Grid>
        <Grid item xs={6} sx={{ borderRight: '1px solid #333', borderTop: '1px solid #333' }}>
          <Box sx={{ p: 2 }}>
            <Typography variant="caption" sx={{ color: '#666' }}>
              ACTIVE POSITIONS
            </Typography>
            <Typography variant="h6" sx={{ color: '#fff', fontWeight: 700 }}>
              {user.active_positions || 0}
            </Typography>
          </Box>
        </Grid>
        <Grid item xs={6} sx={{ borderTop: '1px solid #333' }}>
          <Box sx={{ p: 2 }}>
            <Typography variant="caption" sx={{ color: '#666' }}>
              WIN RATE
            </Typography>
            <Typography variant="h6" sx={{ color: '#00bfff', fontWeight: 700 }}>
              {user.win_rate || '67.3%'}
            </Typography>
          </Box>
        </Grid>
      </Grid>

      {/* Current Positions */}
      <Box sx={{ p: 2 }}>
        <Typography
          variant="subtitle2"
          sx={{
            color: '#00ff00',
            fontWeight: 700,
            mb: 1,
            fontSize: '0.75rem',
            letterSpacing: 0.5,
          }}
        >
          CURRENT POSITIONS
        </Typography>
        
        <List sx={{ p: 0 }}>
          {positions.map((position, index) => (
            <ListItem
              key={index}
              sx={{
                bgcolor: '#0a0a0a',
                border: '1px solid #1a1a1a',
                mb: 1,
                p: 1,
                '&:hover': {
                  bgcolor: 'rgba(0, 255, 0, 0.02)',
                },
              }}
            >
              <ListItemText
                primary={
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                    <Typography variant="body2" sx={{ fontWeight: 600, fontSize: '0.75rem' }}>
                      {position.title}
                    </Typography>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      {position.pnl >= 0 ? (
                        <TrendingUp sx={{ fontSize: 14, color: '#00ff00' }} />
                      ) : (
                        <TrendingDown sx={{ fontSize: 14, color: '#ff0000' }} />
                      )}
                      <Typography
                        variant="body2"
                        sx={{
                          color: position.pnl >= 0 ? '#00ff00' : '#ff0000',
                          fontWeight: 700,
                        }}
                      >
                        {formatPercentage(position.pnlPercent / 100)}
                      </Typography>
                    </Box>
                  </Box>
                }
                secondary={
                  <Grid container spacing={2} sx={{ mt: 0.5 }}>
                    <Grid item xs={3}>
                      <Typography variant="caption" sx={{ color: '#666', fontSize: '0.65rem' }}>
                        OUTCOME
                      </Typography>
                      <Typography variant="caption" sx={{ color: '#fff', display: 'block' }}>
                        {position.outcome}
                      </Typography>
                    </Grid>
                    <Grid item xs={3}>
                      <Typography variant="caption" sx={{ color: '#666', fontSize: '0.65rem' }}>
                        SIZE
                      </Typography>
                      <Typography variant="caption" sx={{ color: '#fff', display: 'block' }}>
                        {position.size}
                      </Typography>
                    </Grid>
                    <Grid item xs={3}>
                      <Typography variant="caption" sx={{ color: '#666', fontSize: '0.65rem' }}>
                        AVG PRICE
                      </Typography>
                      <Typography variant="caption" sx={{ color: '#fff', display: 'block' }}>
                        ${position.avgPrice}
                      </Typography>
                    </Grid>
                    <Grid item xs={3}>
                      <Typography variant="caption" sx={{ color: '#666', fontSize: '0.65rem' }}>
                        P&L
                      </Typography>
                      <Typography
                        variant="caption"
                        sx={{
                          color: position.pnl >= 0 ? '#00ff00' : '#ff0000',
                          display: 'block',
                          fontWeight: 600,
                        }}
                      >
                        {formatValue(position.pnl)}
                      </Typography>
                    </Grid>
                  </Grid>
                }
                sx={{ p: 0 }}
              />
            </ListItem>
          ))}
        </List>
      </Box>
    </Paper>
  );
};

export default UserDetail;