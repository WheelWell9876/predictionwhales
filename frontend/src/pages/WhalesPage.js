// WhalesPage.js
import React from 'react';
import { Box, Typography, Paper } from '@mui/material';

const WhalesPage = () => {
  return (
    <Box sx={{ p: 3 }}>
      <Paper sx={{ p: 3, bgcolor: '#000', border: '2px solid #333' }}>
        <Typography variant="h5" sx={{ color: '#00ff00', mb: 2 }}>
          WHALE TRACKER
        </Typography>
        <Typography>Whale tracking and insider detection coming soon...</Typography>
      </Paper>
    </Box>
  );
};

export default WhalesPage;