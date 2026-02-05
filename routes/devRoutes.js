// routes/devRoutes.js
// Developer mode testing and debugging routes
// ⚠️ These routes should be disabled in production

const express = require('express');
const router = express.Router();
const TestingController = require('../controllers/TestingController');

// Email Testing
router.post('/email/send-test', TestingController.sendTestEmail.bind(TestingController));
router.post('/email/preview', TestingController.previewTemplate.bind(TestingController));
router.get('/email/templates', TestingController.getTemplates.bind(TestingController));

// Webhook Simulation
router.post('/webhook/simulate', TestingController.simulateWebhook.bind(TestingController));

// Scheduling Testing
router.post('/job/fast-forward', TestingController.fastForwardJob.bind(TestingController));
router.post('/followup/trigger', TestingController.triggerFollowup.bind(TestingController));
router.get('/rate-limit/status', TestingController.getRateLimitStatus.bind(TestingController));

// Integration Testing
router.get('/status/brevo', TestingController.testBrevoConnection.bind(TestingController));
router.get('/status/redis', TestingController.testRedisConnection.bind(TestingController));
router.get('/status/database', TestingController.testDatabaseConnection.bind(TestingController));

// Debug Tools
router.get('/queue/status', TestingController.getQueueStatus.bind(TestingController));
router.get('/job/:id/inspect', TestingController.inspectJob.bind(TestingController));
router.get('/lead/:id/inspect', TestingController.inspectLead.bind(TestingController));
router.get('/leads', TestingController.getRecentLeads.bind(TestingController));
router.get('/jobs', TestingController.getRecentJobs.bind(TestingController));
router.delete('/test-data', TestingController.clearTestData.bind(TestingController));

// Clear database (DEV ONLY)
router.delete('/clear-database', async (req, res) => {
  try {
    const { prisma } = require('../lib/prisma');
    
    await prisma.notification.deleteMany({});
    await prisma.emailJob.deleteMany({});
    await prisma.manualMail.deleteMany({});
    await prisma.eventHistory.deleteMany({});
    await prisma.emailSchedule.deleteMany({});
    await prisma.eventStore.deleteMany({});
    await prisma.lead.deleteMany({});
    
    try {
      const RateLimitService = require('../services/RateLimitService');
      await RateLimitService.clearAllSlots();
    } catch (e) { /* ignore if no redis */ }
    
    res.status(200).json({ 
      message: 'Database cleared successfully',
      clearedTables: ['notifications', 'emailJobs', 'manualMails', 'eventHistory', 'emailSchedules', 'eventStore', 'leads']
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============================================
// FCM TEST ENDPOINT
// ============================================
router.post('/fcm/test', async (req, res) => {
  try {
    const NotificationService = require('../services/NotificationService');
    const { prisma } = require('../lib/prisma');
    
    // Check FCM status
    const fcmService = NotificationService.fcmService;
    const isReady = fcmService?.isReady() || false;
    
    // Get registered tokens
    const tokens = await prisma.deviceToken.findMany();
    
    if (!isReady) {
      return res.json({
        success: false,
        fcmReady: false,
        message: 'FCM not initialized. Check FIREBASE_SERVICE_ACCOUNT_PATH in .env',
        registeredTokens: tokens.length
      });
    }
    
    if (tokens.length === 0) {
      return res.json({
        success: false,
        fcmReady: true,
        message: 'No device tokens registered. Enable notifications in the browser first.',
        registeredTokens: 0
      });
    }
    
    // Send test notification
    const result = await NotificationService.createNotification({
      type: 'success',
      message: 'Test notification from LeadFlow! 🔔',
      metadata: { event: 'fcm_test' }
    }, true);
    
    res.json({
      success: true,
      fcmReady: true,
      message: 'Test notification sent!',
      registeredTokens: tokens.length,
      result
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// Get FCM status
router.get('/fcm/status', async (req, res) => {
  try {
    const NotificationService = require('../services/NotificationService');
    const { prisma } = require('../lib/prisma');
    
    const fcmService = NotificationService.fcmService;
    const isReady = fcmService?.isReady() || false;
    const tokens = await prisma.deviceToken.findMany({
      select: { platform: true, createdAt: true }
    });
    
    res.json({
      fcmReady: isReady,
      registeredDevices: tokens.length,
      devices: tokens.map(t => ({
        platform: t.platform,
        registeredAt: t.createdAt
      }))
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
