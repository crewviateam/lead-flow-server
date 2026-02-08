// app.js
// Express server with PostgreSQL (Prisma)
// Production-ready with security headers and CORS

const express = require('express');
const compression = require('compression');
const helmet = require('helmet');
const { loggers } = require('./lib/logger');
const log = loggers.default;
require('dotenv').config();

// ============================================
// ENVIRONMENT VALIDATION
// ============================================
const requiredEnvVars = ['DATABASE_URL'];
const optionalButWarnEnvVars = ['BREVO_API_KEY', 'REDIS_URL'];

requiredEnvVars.forEach(key => {
  if (!process.env[key]) {
    console.error(`❌ FATAL: Missing required environment variable: ${key}`);
    process.exit(1);
  }
});

optionalButWarnEnvVars.forEach(key => {
  if (!process.env[key]) {
    console.warn(`⚠️ WARNING: Missing optional environment variable: ${key}`);
  }
});

// Initialize Express app
const app = express();

// Initialize Prisma client
const { prisma, disconnectPrisma } = require('./lib/prisma');

// ============================================
// SECURITY MIDDLEWARE
// ============================================

// Helmet.js - Sets various HTTP headers for security
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
      fontSrc: ["'self'", "https://fonts.gstatic.com"],
      imgSrc: ["'self'", "data:", "https:"],
      scriptSrc: ["'self'"],
      connectSrc: ["'self'", "wss:", "ws:"]
    }
  },
  crossOriginEmbedderPolicy: false, // Required for some external resources
  crossOriginResourcePolicy: { policy: "cross-origin" }
}));

// Compression middleware (reduces payload sizes by 60-80%)
app.use(compression({
  level: 6,              // Balanced compression level (1-9)
  threshold: 1024,       // Only compress responses > 1KB
  filter: (req, res) => {
    if (req.headers['x-no-compression']) return false;
    return compression.filter(req, res);
  }
}));

// Middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// ============================================
// CORS CONFIGURATION
// ============================================
const allowedOrigins = process.env.ALLOWED_ORIGINS 
  ? process.env.ALLOWED_ORIGINS.split(',').map(o => o.trim())
  : ['http://localhost:5173', 'http://localhost:3000', 'https://lead-flow-client.vercel.app', 'https://outflow.crewvia.in']; // Dev defaults

app.use((req, res, next) => {
  const origin = req.headers.origin;
  
  // Allow if origin is in allowed list or if no origin (same-origin requests)
  if (!origin || allowedOrigins.includes(origin) || allowedOrigins.includes('*')) {
    res.header('Access-Control-Allow-Origin', origin || '*');
  }
  
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Credentials', 'true');
  res.header('Access-Control-Max-Age', '86400'); // Cache preflight for 24 hours
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  next();
});

// Test PostgreSQL connection on startup and clear settings cache
prisma.$connect()
  .then(async () => {
    log.info('PostgreSQL (Prisma) connected');
    // Clear settings cache on startup to ensure fresh data
    const { cache } = require('./lib/cache');
    await cache.invalidateSettings();
    log.info('Settings cache cleared');
  })
  .catch(err => log.error({ error: err.message }, 'PostgreSQL connection error'));

// Initialize event handlers
require('./events/handlers');

// Initialize workers
const emailWorker = require('./workers/emailWorker');
const followupWorker = require('./workers/followupWorker');
const analyticsWorker = require('./workers/analyticsWorker');
const CronService = require('./services/CronService');

log.info('Workers initialized');

// Initialize Cron Service
CronService.init();

// Rate limiting middleware
const { apiLimiter } = require('./middleware/rateLimiter');

// Inngest integration for durable workflows
const { serve } = require('inngest/express');
const { inngest, functions } = require('./inngest');

// Serve Inngest functions at /api/inngest
app.use(
  '/api/inngest',
  serve({
    client: inngest,
    functions: functions
  })
);

// Routes - with rate limiting
const routes = require('./routes');
app.use('/api', apiLimiter, routes);

// Import queues for metrics
const { emailSendQueue, followupQueue, analyticsQueue } = require('./queues/emailQueues');
const redisConnection = require('./config/redis');
const { AppError } = require('./lib/errors');

// Enhanced health check with detailed metrics
app.get('/health', async (req, res) => {
  let postgresStatus = 'disconnected';
  let redisStatus = 'disconnected';
  
  // Check PostgreSQL
  try {
    await prisma.$queryRaw`SELECT 1`;
    postgresStatus = 'connected';
  } catch (e) {
    postgresStatus = 'error';
  }

  // Check Redis
  try {
    await redisConnection.ping();
    redisStatus = 'connected';
  } catch (e) {
    redisStatus = 'error';
  }

  // Get queue metrics
  let queueMetrics = {};
  try {
    const [emailCounts, followupCounts, analyticsCounts] = await Promise.all([
      emailSendQueue.getJobCounts(),
      followupQueue.getJobCounts(),
      analyticsQueue.getJobCounts()
    ]);
    queueMetrics = {
      emailSend: emailCounts,
      followup: followupCounts,
      analytics: analyticsCounts
    };
  } catch (e) {
    queueMetrics = { error: 'Failed to fetch queue metrics' };
  }

  // Get memory usage
  const memoryUsage = process.memoryUsage();
  const formatBytes = (bytes) => `${Math.round(bytes / 1024 / 1024)}MB`;

  res.status(200).json({ 
    status: 'healthy',
    uptime: Math.round(process.uptime()),
    timestamp: new Date().toISOString(),
    database: {
      postgres: postgresStatus,
      redis: redisStatus
    },
    workers: {
      email: emailWorker.isRunning() ? 'running' : 'stopped',
      followup: followupWorker.isRunning() ? 'running' : 'stopped',
      analytics: analyticsWorker.isRunning() ? 'running' : 'stopped'
    },
    queues: queueMetrics,
    memory: {
      heapUsed: formatBytes(memoryUsage.heapUsed),
      heapTotal: formatBytes(memoryUsage.heapTotal),
      rss: formatBytes(memoryUsage.rss)
    }
  });
});

// Metrics endpoint for external monitoring (Prometheus format or JSON)
app.get('/metrics', async (req, res) => {
  try {
    const [emailCounts, followupCounts] = await Promise.all([
      emailSendQueue.getJobCounts(),
      followupQueue.getJobCounts()
    ]);
    
    const memoryUsage = process.memoryUsage();
    
    res.status(200).json({
      timestamp: Date.now(),
      uptime_seconds: Math.round(process.uptime()),
      memory_heap_used_bytes: memoryUsage.heapUsed,
      memory_heap_total_bytes: memoryUsage.heapTotal,
      memory_rss_bytes: memoryUsage.rss,
      queue_email_waiting: emailCounts.waiting || 0,
      queue_email_active: emailCounts.active || 0,
      queue_email_completed: emailCounts.completed || 0,
      queue_email_failed: emailCounts.failed || 0,
      queue_followup_waiting: followupCounts.waiting || 0,
      queue_followup_active: followupCounts.active || 0
    });
  } catch (e) {
    res.status(500).json({ error: 'Failed to collect metrics' });
  }
});

// Centralized error handling middleware
app.use((err, req, res, next) => {
  // Use structured logger if available
  console.error('[Error]', {
    message: err.message,
    code: err.code,
    statusCode: err.statusCode,
    path: req.path,
    method: req.method
  });
  
  // Handle AppError instances (our custom errors)
  if (err instanceof AppError) {
    return res.status(err.statusCode).json(err.toJSON());
  }
  
  // Handle Prisma errors
  if (err.code === 'P2002') {
    return res.status(409).json({ error: 'Duplicate entry', code: 'CONFLICT' });
  }
  if (err.code === 'P2025') {
    return res.status(404).json({ error: 'Record not found', code: 'NOT_FOUND' });
  }
  
  // Handle validation errors from Joi
  if (err.name === 'ValidationError') {
    return res.status(400).json({ error: err.message, code: 'VALIDATION_ERROR' });
  }
  
  // Default error response
  res.status(err.status || err.statusCode || 500).json({
    error: err.message || 'Internal server error',
    code: 'INTERNAL_ERROR',
    ...(process.env.NODE_ENV === 'development' && { stack: err.stack })
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Route not found' });
});

// Import WebSocket service
const websocketService = require('./lib/websocket');

// Graceful shutdown
const shutdown = async (signal) => {
  console.log(`${signal} received, shutting down gracefully...`);
  
  await websocketService.close();
  await emailWorker.close();
  await followupWorker.close();
  await analyticsWorker.close();
  
  await disconnectPrisma();
  
  process.exit(0);
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

const PORT = process.env.PORT || 3000;

// Create HTTP server for WebSocket support
const http = require('http');
const server = http.createServer(app);

// Initialize WebSocket server
websocketService.init(server, redisConnection).then(() => {
  console.log('✅ WebSocket server ready');
}).catch(err => {
  console.warn('[WebSocket] Initialization warning:', err.message);
});

server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📡 WebSocket enabled for real-time updates`);
});

module.exports = { app, server, websocketService };