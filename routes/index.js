// routes/index.js
// Main router - Clean orchestrator for all route modules
// Organized by category for scalability and maintainability

const express = require('express');
const router = express.Router();

// ============================================
// ROUTE MODULES
// ============================================

// Core business routes
const leadRoutes = require('./leadRoutes');
const emailJobRoutes = require('./emailJobRoutes');
const tagRoutes = require('./tagRoutes');
const settingsRoutes = require('./settingsRoutes');
const scheduleRoutes = require('./scheduleRoutes');

// Analytics & monitoring
const analyticsRoutes = require('./analyticsRoutes');
const terminalStatesRoutes = require('./terminalStatesRoutes');

// External integrations
const webhookRoutes = require('./webhookRoutes');
const templateRoutes = require('./templateRoutes');
const conditionalEmailRoutes = require('./conditionalEmailRoutes');

// System routes
const notificationRoutes = require('./notificationRoutes');
const devRoutes = require('./devRoutes');

// Rate limit controller (standalone)
const RateLimitController = require('../controllers/RateLimitController');

// ============================================
// MOUNT ROUTES
// ============================================

// Core business
router.use('/leads', leadRoutes);
router.use('/email-jobs', emailJobRoutes);
router.use('/tags', tagRoutes);
router.use('/settings', settingsRoutes);
router.use('/schedule', scheduleRoutes);

// Analytics & monitoring
router.use('/analytics', analyticsRoutes);
router.use('/terminal-states', terminalStatesRoutes);

// External integrations - webhook at /brevo for Brevo compatibility
router.use('/brevo', webhookRoutes);
router.use('/templates', templateRoutes);
router.use('/conditional-emails', conditionalEmailRoutes);

// System
router.use('/notifications', notificationRoutes);
router.use('/dev', devRoutes);

// Standalone routes
router.get('/rate-limits', RateLimitController.getRateLimitStatus.bind(RateLimitController));
router.get('/config', RateLimitController.getConfig.bind(RateLimitController));

// ============================================
// REAL-TIME STREAM (SSE)
// ============================================
router.get('/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  
  res.write(`data: ${JSON.stringify({ type: 'connected', timestamp: new Date() })}\n\n`);
  
  const heartbeat = setInterval(() => {
    res.write(`data: ${JSON.stringify({ type: 'heartbeat', timestamp: new Date() })}\n\n`);
  }, 30000);
  
  req.on('close', () => {
    clearInterval(heartbeat);
  });
});

module.exports = router;