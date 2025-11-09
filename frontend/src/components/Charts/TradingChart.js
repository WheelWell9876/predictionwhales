import React, { useEffect, useRef, useState } from 'react';
import { Box, Paper, Typography, ButtonGroup, Button, IconButton } from '@mui/material';
import {
  ZoomIn,
  ZoomOut,
  Fullscreen,
  Timeline,
  ShowChart,
  BarChart as BarChartIcon,
  CandlestickChart,
  Refresh,
} from '@mui/icons-material';
import { createChart, ColorType } from 'lightweight-charts';

const TradingChart = ({ data, title, type = 'candlestick' }) => {
  const chartContainerRef = useRef();
  const chartRef = useRef();
  const [timeframe, setTimeframe] = useState('1D');
  const [chartType, setChartType] = useState(type);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: '#000000' },
        textColor: '#b0b0b0',
      },
      grid: {
        vertLines: {
          color: '#1a1a1a',
          style: 1,
        },
        horzLines: {
          color: '#1a1a1a',
          style: 1,
        },
      },
      crosshair: {
        mode: 0,
        vertLine: {
          width: 1,
          color: '#666',
          style: 2,
        },
        horzLine: {
          width: 1,
          color: '#666',
          style: 2,
        },
      },
      rightPriceScale: {
        borderColor: '#333',
        borderVisible: true,
      },
      timeScale: {
        borderColor: '#333',
        borderVisible: true,
        timeVisible: true,
        secondsVisible: false,
      },
      watermark: {
        visible: false,
      },
      width: chartContainerRef.current.clientWidth,
      height: 400,
    });

    chartRef.current = chart;

    // Add series based on chart type
    let series;
    if (chartType === 'candlestick') {
      series = chart.addCandlestickSeries({
        upColor: '#00ff00',
        downColor: '#ff0000',
        borderUpColor: '#00ff00',
        borderDownColor: '#ff0000',
        wickUpColor: '#00ff00',
        wickDownColor: '#ff0000',
      });
    } else if (chartType === 'line') {
      series = chart.addLineSeries({
        color: '#00ff00',
        lineWidth: 2,
      });
    } else if (chartType === 'area') {
      series = chart.addAreaSeries({
        lineColor: '#00ff00',
        topColor: 'rgba(0, 255, 0, 0.3)',
        bottomColor: 'rgba(0, 255, 0, 0.0)',
      });
    }

    // Add volume series
    const volumeSeries = chart.addHistogramSeries({
      color: '#26a69a',
      priceFormat: {
        type: 'volume',
      },
      priceScaleId: '',
      scaleMargins: {
        top: 0.8,
        bottom: 0,
      },
    });

    // Generate sample data if no data provided
    const sampleData = data || generateSampleData();
    
    if (chartType === 'candlestick') {
      series.setData(sampleData.candlesticks);
    } else {
      series.setData(sampleData.line);
    }
    volumeSeries.setData(sampleData.volume);

    // Fit content
    chart.timeScale().fitContent();

    // Handle resize
    const handleResize = () => {
      if (chartContainerRef.current) {
        chart.applyOptions({ 
          width: chartContainerRef.current.clientWidth 
        });
      }
    };

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, [data, chartType, timeframe]);

  const generateSampleData = () => {
    const candlesticks = [];
    const line = [];
    const volume = [];
    const now = Math.floor(Date.now() / 1000);
    const oneDay = 24 * 60 * 60;
    let lastClose = 100;

    for (let i = 100; i >= 0; i--) {
      const time = now - i * oneDay;
      const volatility = 0.02 + Math.random() * 0.03;
      const random = Math.random();
      const change = 2 * volatility * (random - 0.5);
      
      const open = lastClose;
      const close = open * (1 + change);
      const high = Math.max(open, close) * (1 + Math.random() * volatility);
      const low = Math.min(open, close) * (1 - Math.random() * volatility);
      
      candlesticks.push({
        time,
        open,
        high,
        low,
        close,
      });

      line.push({
        time,
        value: close,
      });

      volume.push({
        time,
        value: Math.random() * 1000000,
        color: close >= open ? '#00ff0066' : '#ff000066',
      });

      lastClose = close;
    }

    return { candlesticks, line, volume };
  };

  const timeframes = ['1H', '4H', '1D', '1W', '1M', '3M', '1Y', 'ALL'];
  const chartTypes = [
    { type: 'line', icon: <ShowChart /> },
    { type: 'candlestick', icon: <CandlestickChart /> },
    { type: 'area', icon: <BarChartIcon /> },
  ];

  return (
    <Paper
      elevation={0}
      sx={{
        bgcolor: '#000000',
        border: '2px solid #333',
        borderRadius: 0,
        overflow: 'hidden',
        height: '100%',
      }}
    >
      {/* Chart Header */}
      <Box
        sx={{
          height: 48,
          bgcolor: '#0a0a0a',
          borderBottom: '2px solid #333',
          display: 'flex',
          alignItems: 'center',
          px: 2,
          gap: 2,
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
          {title || 'PORTFOLIO VALUE'}
        </Typography>

        <Box sx={{ flexGrow: 1 }} />

        {/* Chart Type Selector */}
        <ButtonGroup
          size="small"
          variant="outlined"
          sx={{
            '& .MuiButton-root': {
              borderColor: '#333',
              color: '#666',
              minWidth: 32,
              px: 1,
              '&.Mui-selected': {
                bgcolor: 'rgba(0, 255, 0, 0.1)',
                borderColor: '#00ff00',
                color: '#00ff00',
              },
            },
          }}
        >
          {chartTypes.map((ct) => (
            <Button
              key={ct.type}
              onClick={() => setChartType(ct.type)}
              className={chartType === ct.type ? 'Mui-selected' : ''}
            >
              {ct.icon}
            </Button>
          ))}
        </ButtonGroup>

        {/* Timeframe Selector */}
        <ButtonGroup
          size="small"
          variant="outlined"
          sx={{
            '& .MuiButton-root': {
              borderColor: '#333',
              color: '#666',
              minWidth: 32,
              px: 1,
              '&.Mui-selected': {
                bgcolor: 'rgba(0, 255, 0, 0.1)',
                borderColor: '#00ff00',
                color: '#00ff00',
              },
            },
          }}
        >
          {timeframes.map((tf) => (
            <Button
              key={tf}
              onClick={() => setTimeframe(tf)}
              className={timeframe === tf ? 'Mui-selected' : ''}
            >
              {tf}
            </Button>
          ))}
        </ButtonGroup>

        {/* Chart Controls */}
        <Box sx={{ display: 'flex', gap: 0.5 }}>
          <IconButton size="small" sx={{ color: '#666' }}>
            <ZoomIn fontSize="small" />
          </IconButton>
          <IconButton size="small" sx={{ color: '#666' }}>
            <ZoomOut fontSize="small" />
          </IconButton>
          <IconButton size="small" sx={{ color: '#666' }}>
            <Refresh fontSize="small" />
          </IconButton>
          <IconButton size="small" sx={{ color: '#666' }}>
            <Fullscreen fontSize="small" />
          </IconButton>
        </Box>
      </Box>

      {/* Chart Stats */}
      <Box
        sx={{
          height: 32,
          bgcolor: '#0a0a0a',
          borderBottom: '1px solid #1a1a1a',
          display: 'flex',
          alignItems: 'center',
          px: 2,
          gap: 3,
        }}
      >
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Typography variant="caption" sx={{ color: '#666' }}>OPEN:</Typography>
          <Typography variant="caption" sx={{ color: '#fff', fontWeight: 600 }}>98.45</Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Typography variant="caption" sx={{ color: '#666' }}>HIGH:</Typography>
          <Typography variant="caption" sx={{ color: '#00ff00', fontWeight: 600 }}>105.23</Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Typography variant="caption" sx={{ color: '#666' }}>LOW:</Typography>
          <Typography variant="caption" sx={{ color: '#ff0000', fontWeight: 600 }}>97.12</Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Typography variant="caption" sx={{ color: '#666' }}>CLOSE:</Typography>
          <Typography variant="caption" sx={{ color: '#fff', fontWeight: 600 }}>103.67</Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Typography variant="caption" sx={{ color: '#666' }}>CHANGE:</Typography>
          <Typography variant="caption" sx={{ color: '#00ff00', fontWeight: 600 }}>+5.22 (+5.31%)</Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Typography variant="caption" sx={{ color: '#666' }}>VOLUME:</Typography>
          <Typography variant="caption" sx={{ color: '#fff', fontWeight: 600 }}>1.2M</Typography>
        </Box>
      </Box>

      {/* Chart Container */}
      <Box 
        ref={chartContainerRef}
        sx={{ 
          width: '100%', 
          height: 400,
          bgcolor: '#000000',
        }}
      />
    </Paper>
  );
};

export default TradingChart;