// SettingsPage.js
import React from 'react';
import { Box, Typography, Paper } from '@mui/material';

const SettingsPage = () => {
  return (
    <Box sx={{ p: 3 }}>
      <Paper sx={{ p: 3, bgcolor: '#000', border: '2px solid #333' }}>
        <Typography variant="h5" sx={{ color: '#00ff00', mb: 2 }}>
          TERMINAL SETTINGS
        </Typography>
        <Typography>Configuration options coming soon...</Typography>
      </Paper>
    </Box>
  );
};

export default SettingsPage;