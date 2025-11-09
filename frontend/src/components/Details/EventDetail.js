// EventDetail.js
import React from 'react';
import { Box, Typography, Paper } from '@mui/material';

const EventDetail = ({ event }) => {
  if (!event) return null;
  
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
        EVENT DETAILS
      </Typography>
      <Typography variant="body2" sx={{ mb: 1 }}>
        {event.title}
      </Typography>
      <Typography variant="caption" sx={{ color: '#666' }}>
        Volume: ${(event.volume || 0).toLocaleString()}
      </Typography>
    </Paper>
  );
};

export default EventDetail;