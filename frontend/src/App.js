import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { Toaster } from 'react-hot-toast';

// Layout Components
import MainLayout from './components/Layout/MainLayout';

// Page Components
import TradePage from './pages/TradingPage';
import PositionsPage from './pages/PositionsPage';
import StatsPage from './pages/StatsPage';
import WhalesPage from './pages/WhalesPage';
import SettingsPage from './pages/SettingsPage';

// Create dark theme similar to Bloomberg Terminal
const darkTheme = createTheme({
  palette: {
    mode: 'dark',
    primary: {
      main: '#00ff00', // Terminal green
    },
    secondary: {
      main: '#ffa500', // Terminal orange
    },
    background: {
      default: '#000000',
      paper: '#0a0a0a',
    },
    text: {
      primary: '#ffffff',
      secondary: '#b0b0b0',
    },
    success: {
      main: '#00ff00',
    },
    error: {
      main: '#ff0000',
    },
    warning: {
      main: '#ffa500',
    },
    info: {
      main: '#00bfff',
    },
  },
  typography: {
    fontFamily: '"Courier New", Courier, monospace',
    fontSize: 12,
    h1: {
      fontSize: '1.8rem',
      fontWeight: 600,
    },
    h2: {
      fontSize: '1.5rem',
      fontWeight: 600,
    },
    h3: {
      fontSize: '1.3rem',
      fontWeight: 600,
    },
    h4: {
      fontSize: '1.1rem',
      fontWeight: 600,
    },
    body1: {
      fontSize: '0.875rem',
    },
    body2: {
      fontSize: '0.75rem',
    },
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 0,
          textTransform: 'uppercase',
          fontWeight: 600,
          border: '1px solid',
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          borderRadius: 0,
          border: '1px solid #333',
          backgroundImage: 'none',
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: {
          borderBottom: '1px solid #333',
          padding: '4px 8px',
        },
      },
    },
    MuiTab: {
      styleOverrides: {
        root: {
          borderRadius: 0,
          minHeight: 40,
          textTransform: 'uppercase',
          '&:hover': {
            backgroundColor: 'rgba(255, 255, 255, 0.1)',
          },
        },
      },
    },
    MuiDataGrid: {
      styleOverrides: {
        root: {
          border: '1px solid #333',
          '& .MuiDataGrid-cell': {
            borderBottom: '1px solid #333',
            borderRight: '1px solid #333',
          },
          '& .MuiDataGrid-columnHeaders': {
            backgroundColor: '#0a0a0a',
            borderBottom: '2px solid #333',
          },
          '& .MuiDataGrid-row:hover': {
            backgroundColor: 'rgba(255, 255, 255, 0.05)',
          },
        },
      },
    },
  },
});

// Create React Query client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 2,
      staleTime: 60000, // 1 minute
    },
  },
});

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider theme={darkTheme}>
        <CssBaseline />
        <Router>
          <MainLayout>
            <Routes>
              <Route path="/" element={<TradePage />} />
              <Route path="/trade" element={<TradePage />} />
              <Route path="/positions" element={<PositionsPage />} />
              <Route path="/stats" element={<StatsPage />} />
              <Route path="/whales" element={<WhalesPage />} />
              <Route path="/settings" element={<SettingsPage />} />
            </Routes>
          </MainLayout>
        </Router>
        <Toaster 
          position="bottom-right"
          toastOptions={{
            style: {
              background: '#0a0a0a',
              color: '#fff',
              border: '1px solid #333',
              borderRadius: 0,
              fontSize: '12px',
            },
            success: {
              iconTheme: {
                primary: '#00ff00',
                secondary: '#000',
              },
            },
            error: {
              iconTheme: {
                primary: '#ff0000',
                secondary: '#000',
              },
            },
          }}
        />
      </ThemeProvider>
    </QueryClientProvider>
  );
}

export default App;