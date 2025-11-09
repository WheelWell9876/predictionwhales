// MarketDetail.js
import React from 'react';
import { Box, Typography, Paper } from '@mui/material';

const MarketDetail = ({ market }) => {
  if (!market) return null;
  
  return (
    <Paper
      elevation={0}
      sx={{
        bgcolor: '#000000',
        border: '2px solid #333',
        borderRadius: 0,
        p: 2,
        height: '100%',
      }}
    >
      <Typography
        variant="h6"
        sx={{
          fontWeight: 700,
          fontSize: '0.875rem',
          color: '#00ff00',
          letterSpacing: 1,
          mb: 2,
        }}
      >
        MARKET DETAILS
      </Typography>
      <Typography variant="body2" sx={{ mb: 1 }}>
        {market.question}
      </Typography>
      <Typography variant="caption" sx={{ color: '#666' }}>
        Price: ${market.last_trade_price || 0}
      </Typography>
    </Paper>
  );
};

export default MarketDetail;