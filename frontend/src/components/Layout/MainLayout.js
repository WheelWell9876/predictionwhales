import React, { useState } from 'react';
import {
  Box,
  AppBar,
  Toolbar,
  Typography,
  IconButton,
  Drawer,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Collapse,
  Button,
  Avatar,
  Paper,
  Divider,
  Tab,
  Tabs,
} from '@mui/material';
import {
  Home as HomeIcon,
  Menu as MenuIcon,
  ChevronLeft as ChevronLeftIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
  TrendingUp,
  AccountBalance,
  Assessment,
  Settings,
  Person,
  ShowChart,
  AttachMoney,
  Timeline,
  PieChart,
  BarChart,
} from '@mui/icons-material';
import { useNavigate, useLocation } from 'react-router-dom';
import { styled } from '@mui/material/styles';

const drawerWidth = 240;

const StyledAppBar = styled(AppBar)(({ theme }) => ({
  zIndex: theme.zIndex.drawer + 1,
  backgroundColor: '#000000',
  borderBottom: '2px solid #333',
  boxShadow: 'none',
}));

const StyledTab = styled(Tab)(({ theme }) => ({
  color: '#b0b0b0',
  fontWeight: 600,
  fontSize: '0.875rem',
  minHeight: 48,
  '&.Mui-selected': {
    color: '#ffffff',
    backgroundColor: 'rgba(255, 255, 255, 0.1)',
  },
  '&:hover': {
    color: '#ffffff',
    backgroundColor: 'rgba(255, 255, 255, 0.05)',
  },
}));

const StyledDrawer = styled(Drawer)(({ theme }) => ({
  width: drawerWidth,
  flexShrink: 0,
  '& .MuiDrawer-paper': {
    width: drawerWidth,
    backgroundColor: '#0a0a0a',
    borderRight: '2px solid #333',
    marginTop: 64,
  },
}));

const StatsPanel = styled(Paper)(({ theme }) => ({
  backgroundColor: '#000000',
  border: '1px solid #333',
  borderRadius: 0,
  padding: theme.spacing(2),
  margin: theme.spacing(2),
}));

const MainLayout = ({ children }) => {
  const [drawerOpen, setDrawerOpen] = useState(true);
  const [statsExpanded, setStatsExpanded] = useState(true);
  const navigate = useNavigate();
  const location = useLocation();

  const tabs = [
    { label: 'TRADE', value: '/trade', icon: <ShowChart /> },
    { label: 'POSITIONS', value: '/positions', icon: <AccountBalance /> },
    { label: 'STATS', value: '/stats', icon: <Assessment /> },
    { label: 'WHALES', value: '/whales', icon: <TrendingUp /> },
    { label: 'SETTINGS', value: '/settings', icon: <Settings /> },
  ];

  const sidebarOptions = [
    { label: 'Users Table', icon: <Person />, path: '/trade?view=users' },
    { label: 'Events Table', icon: <Timeline />, path: '/trade?view=events' },
    { label: 'Markets Table', icon: <AttachMoney />, path: '/trade?view=markets' },
    { label: 'Series View', icon: <BarChart />, path: '/trade?view=series' },
    { label: 'Tags View', icon: <PieChart />, path: '/trade?view=tags' },
  ];

  const handleTabChange = (event, newValue) => {
    navigate(newValue);
  };

  const handleDrawerToggle = () => {
    setDrawerOpen(!drawerOpen);
  };

  const currentTab = tabs.find(tab => location.pathname === tab.value)?.value || '/trade';

  // Mock user data
  const user = {
    initials: 'JD',
    name: 'John Doe',
    wallet: '0x1234...5678'
  };

  // Mock stats data
  const stats = {
    totalVolume: '$1,234,567',
    activeMarkets: '456',
    totalUsers: '12,345',
    avgLiquidity: '$89,012',
  };

  return (
    <Box sx={{ display: 'flex', bgcolor: '#000000', minHeight: '100vh' }}>
      {/* Header */}
      <StyledAppBar position="fixed">
        <Toolbar sx={{ minHeight: 64 }}>
          <IconButton
            edge="start"
            color="inherit"
            aria-label="menu"
            onClick={handleDrawerToggle}
            sx={{ mr: 2 }}
          >
            {drawerOpen ? <ChevronLeftIcon /> : <MenuIcon />}
          </IconButton>
          
          <IconButton
            edge="start"
            color="inherit"
            aria-label="home"
            onClick={() => navigate('/')}
            sx={{ mr: 2 }}
          >
            <HomeIcon />
          </IconButton>
          
          <Typography variant="h6" component="div" sx={{ flexGrow: 0, mr: 4, fontWeight: 700 }}>
            POLYMARKET TERMINAL
          </Typography>
          
          <Tabs
            value={currentTab}
            onChange={handleTabChange}
            sx={{ flexGrow: 1 }}
          >
            {tabs.map((tab) => (
              <StyledTab
                key={tab.value}
                label={tab.label}
                value={tab.value}
                icon={tab.icon}
                iconPosition="start"
              />
            ))}
          </Tabs>
          
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Typography variant="body2" sx={{ color: '#b0b0b0' }}>
              {new Date().toLocaleString()}
            </Typography>
            <Button
              variant="outlined"
              startIcon={
                <Avatar sx={{ width: 24, height: 24, bgcolor: 'primary.main', color: '#000' }}>
                  {user.initials}
                </Avatar>
              }
              sx={{ 
                borderColor: '#333',
                color: '#fff',
                '&:hover': {
                  borderColor: '#666',
                  backgroundColor: 'rgba(255, 255, 255, 0.05)'
                }
              }}
            >
              {user.wallet}
            </Button>
          </Box>
        </Toolbar>
      </StyledAppBar>
      
      {/* Sidebar */}
      <StyledDrawer
        variant="persistent"
        anchor="left"
        open={drawerOpen}
      >
        <List sx={{ pt: 2 }}>
          <ListItem sx={{ mb: 1 }}>
            <Typography variant="h6" sx={{ color: '#00ff00', fontWeight: 700 }}>
              QUICK ACCESS
            </Typography>
          </ListItem>
          <Divider sx={{ bgcolor: '#333', mb: 1 }} />
          
          {sidebarOptions.map((option, index) => (
            <ListItem
              button
              key={index}
              onClick={() => navigate(option.path)}
              sx={{
                '&:hover': {
                  backgroundColor: 'rgba(255, 255, 255, 0.05)',
                },
                borderLeft: location.pathname + location.search === option.path ? '3px solid #00ff00' : '3px solid transparent',
              }}
            >
              <ListItemIcon sx={{ color: '#b0b0b0', minWidth: 40 }}>
                {option.icon}
              </ListItemIcon>
              <ListItemText 
                primary={option.label} 
                primaryTypographyProps={{
                  fontSize: '0.875rem',
                  fontWeight: 600,
                }}
              />
            </ListItem>
          ))}
        </List>
        
        <Box sx={{ position: 'absolute', bottom: 0, width: '100%', p: 2 }}>
          <ListItem
            button
            onClick={() => setStatsExpanded(!statsExpanded)}
            sx={{ bgcolor: '#1a1a1a', mb: 1 }}
          >
            <ListItemText 
              primary="LIVE STATISTICS" 
              primaryTypographyProps={{
                fontSize: '0.875rem',
                fontWeight: 700,
                color: '#00ff00',
              }}
            />
            {statsExpanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
          </ListItem>
          
          <Collapse in={statsExpanded}>
            <StatsPanel elevation={0}>
              {Object.entries(stats).map(([key, value]) => (
                <Box key={key} sx={{ mb: 1.5 }}>
                  <Typography variant="caption" sx={{ color: '#b0b0b0' }}>
                    {key.replace(/([A-Z])/g, ' $1').toUpperCase()}
                  </Typography>
                  <Typography variant="body1" sx={{ color: '#00ff00', fontWeight: 700 }}>
                    {value}
                  </Typography>
                </Box>
              ))}
            </StatsPanel>
          </Collapse>
        </Box>
      </StyledDrawer>
      
      {/* Main Content */}
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          bgcolor: '#000000',
          p: 3,
          marginLeft: drawerOpen ? `${drawerWidth}px` : 0,
          marginTop: '64px',
          transition: 'margin 0.3s',
        }}
      >
        {children}
      </Box>
    </Box>
  );
};

export default MainLayout;