// routes/index.js
const express = require('express');
const { leadController, upload } = require('../controllers/LeadController');
const WebhookController = require('../controllers/WebhookController');
const AnalyticsController = require('../controllers/AnalyticsController');
const EmailJobController = require('../controllers/EmailJobController');
const RateLimitController = require('../controllers/RateLimitController');
const SettingsController = require('../controllers/SettingsController');
const { validateBody, validateParams } = require('../middleware/validate');

const router = express.Router();

// Lead routes
router.post('/upload-leads', upload.single('file'), leadController.uploadLeads.bind(leadController));
router.post('/schedule-emails', leadController.scheduleEmails.bind(leadController));
router.post('/leads/:id/freeze', validateParams('idParam'), leadController.freezeLead.bind(leadController));
router.post('/leads/:id/unfreeze', validateParams('idParam'), leadController.unfreezeLead.bind(leadController));
router.post('/leads/:id/convert', validateParams('idParam'), leadController.convertLead.bind(leadController));
router.put('/leads/:id', validateParams('idParam'), validateBody('updateLead'), leadController.updateLead.bind(leadController));
router.get('/leads/:id/slots', validateParams('idParam'), leadController.getAvailableSlots.bind(leadController));
// Email Controls
router.post('/leads/:id/manual-schedule', validateParams('idParam'), validateBody('scheduleManualSlot'), leadController.scheduleManualSlot.bind(leadController));
router.delete('/leads/:id/email-jobs/:jobId', validateParams('idParam'), leadController.deleteEmailJob.bind(leadController));
router.post('/leads/:id/pause', validateParams('idParam'), leadController.pauseFollowups.bind(leadController));
router.post('/leads/:id/resume', validateParams('idParam'), leadController.resumeFollowups.bind(leadController));
router.post('/leads/:id/skip', validateParams('idParam'), leadController.skipFollowup.bind(leadController));
router.post('/leads/:id/revert-skip', validateParams('idParam'), leadController.revertSkipFollowup.bind(leadController));
router.delete('/leads/:id/followup/:stepName', validateParams('idParam'), leadController.deleteFollowupFromLead.bind(leadController));
router.get('/leads', leadController.getLeads.bind(leadController));
router.get('/leads/:id', async (req, res) => {
  try {
    const { LeadRepository, EmailJobRepository } = require('../repositories');
    
    const lead = await LeadRepository.findById(req.params.id, {
      include: { emailSchedule: true, eventHistory: true, manualMails: true }
    });
    if (!lead) {
      return res.status(404).json({ error: 'Lead not found' });
    }
    
    // Get all email jobs for this lead
    const emailJobs = await EmailJobRepository.findByLeadId(req.params.id);
    
    res.status(200).json({ lead, emailJobs });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});
router.delete('/leads/:id', async (req, res) => {
  try {
    const { LeadRepository, EmailJobRepository } = require('../repositories');
    const { prisma } = require('../lib/prisma');
    
    // Delete all email jobs for this lead
    await prisma.emailJob.deleteMany({ where: { leadId: parseInt(req.params.id) } });
    // Delete the lead
    await LeadRepository.delete(req.params.id);
    
    res.status(200).json({ message: 'Lead deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Email Job routes
router.get('/email-jobs', EmailJobController.getEmailJobs.bind(EmailJobController));
router.get('/email-jobs/:id', EmailJobController.getEmailJob.bind(EmailJobController));
router.post('/email-jobs/:id/retry', EmailJobController.retryJob.bind(EmailJobController));
router.post('/email-jobs/:id/resume', EmailJobController.resumeJob.bind(EmailJobController));
router.put('/email-jobs/:id/reschedule', EmailJobController.rescheduleJob.bind(EmailJobController));
router.delete('/email-jobs/:id', EmailJobController.cancelJob.bind(EmailJobController));

// Tag routes
const TagController = require('../controllers/TagController');
router.get('/tags', TagController.getAllTags.bind(TagController));
router.post('/leads/:id/tags', TagController.addTags.bind(TagController));
router.delete('/leads/:id/tags/:tag', TagController.removeTag.bind(TagController));
router.post('/leads/bulk-tag', TagController.bulkAddTags.bind(TagController));
router.post('/leads/bulk-untag', TagController.bulkRemoveTags.bind(TagController));

// Settings routes
router.get('/settings', SettingsController.getSettings.bind(SettingsController));
router.put('/settings', SettingsController.updateSettings.bind(SettingsController));
router.get('/settings/followups', SettingsController.getFollowups.bind(SettingsController));
router.post('/settings/followups', SettingsController.addFollowup.bind(SettingsController));
router.put('/settings/followups/:id', SettingsController.updateFollowup.bind(SettingsController));
router.delete('/settings/followups/:id', SettingsController.deleteFollowup.bind(SettingsController));
router.post('/settings/followups/reorder', SettingsController.reorderFollowups.bind(SettingsController));
router.post('/settings/clear-logs', SettingsController.clearBrevoLogs.bind(SettingsController));

// Paused dates and weekend configuration routes
router.get('/settings/paused-dates', SettingsController.getPausedDates.bind(SettingsController));
router.post('/settings/pause-date', SettingsController.pauseDate.bind(SettingsController));
router.post('/settings/unpause-date', SettingsController.unpauseDate.bind(SettingsController));
router.post('/settings/weekend-days', SettingsController.updateWeekendDays.bind(SettingsController));
router.post('/settings/reschedule-paused', SettingsController.reschedulePausedEmails.bind(SettingsController));
router.post('/settings/test-brevo', SettingsController.testBrevoConnection.bind(SettingsController));

// Rulebook routes
router.get('/settings/rulebook', SettingsController.getRulebook.bind(SettingsController));
router.put('/settings/rulebook', SettingsController.updateRulebook.bind(SettingsController));
router.post('/settings/rulebook/reset', SettingsController.resetRulebook.bind(SettingsController));
router.get('/settings/rulebook/defaults', SettingsController.getDefaultRulebook.bind(SettingsController));
router.get('/settings/rulebook/permissions', SettingsController.getMailTypePermissions.bind(SettingsController));


// Schedule Routes
const scheduleController = require('../controllers/ScheduleController');
router.get('/schedule', scheduleController.getSchedule.bind(scheduleController));
router.get('/schedule/timezones', scheduleController.getTimezones.bind(scheduleController));

// Rate Limit routes
router.get('/rate-limits', RateLimitController.getRateLimitStatus.bind(RateLimitController));
router.get('/config', RateLimitController.getConfig.bind(RateLimitController));

// Webhook routes
router.post('/brevo/webhook', WebhookController.handleBrevoWebhook.bind(WebhookController));
router.get('/brevo/webhook', WebhookController.verifyWebhook.bind(WebhookController));

// Analytics routes
router.get('/analytics/summary', AnalyticsController.getSummary.bind(AnalyticsController));
router.get('/analytics/dashboard', AnalyticsController.getDashboardStats.bind(AnalyticsController));
router.get('/analytics/email-jobs', AnalyticsController.getEmailJobStats.bind(AnalyticsController));
router.get('/analytics/leads', AnalyticsController.getLeadStats.bind(AnalyticsController));
router.get('/analytics/breakdown', AnalyticsController.getDetailedBreakdown.bind(AnalyticsController));
router.get('/analytics/hierarchy', AnalyticsController.getHierarchicalAnalytics.bind(AnalyticsController));
router.get('/analytics/recent-activity', AnalyticsController.getRecentActivity.bind(AnalyticsController));
router.post('/analytics/sync', AnalyticsController.syncFromBrevo.bind(AnalyticsController));

// Terminal States routes (Dead, Unsubscribed, Complaint)
const TerminalStatesController = require('../controllers/TerminalStatesController');
router.get('/terminal-states', TerminalStatesController.getLeadsByState.bind(TerminalStatesController));
router.get('/terminal-states/stats', TerminalStatesController.getStats.bind(TerminalStatesController));
router.get('/terminal-states/:id', TerminalStatesController.getLeadDetails.bind(TerminalStatesController));
router.post('/terminal-states/:id/resurrect', TerminalStatesController.resurrect.bind(TerminalStatesController));

// Lead retry route (for failed outreach page)
router.post('/leads/:id/retry', leadController.retryLead.bind(leadController));

// Real-time updates via Server-Sent Events
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

// Template routes
const templateRoutes = require('./templateRoutes');
router.use('/templates', templateRoutes);

// Notification routes
const NotificationController = require('../controllers/NotificationController');
router.get('/notifications', NotificationController.getNotifications.bind(NotificationController));
router.put('/notifications/read', NotificationController.markRead.bind(NotificationController));

// Device Token routes (FCM Push Notifications)
const NotificationService = require('../services/NotificationService');
router.post('/device-tokens', async (req, res) => {
  try {
    const { userId = 'admin', token, platform = 'web' } = req.body;
    if (!token) {
      return res.status(400).json({ error: 'Token is required' });
    }
    const result = await NotificationService.registerDeviceToken(userId, token, platform);
    if (result === true) {
      res.status(200).json({ success: true, message: 'Device registered for push notifications' });
    } else {
      res.status(200).json({ success: false, ...result });
    }
  } catch (error) {
    console.error('Device token registration error:', error);
    res.status(500).json({ error: error.message });
  }
});

router.delete('/device-tokens/:token', async (req, res) => {
  try {
    const { token } = req.params;
    const result = await NotificationService.unregisterDeviceToken(token);
    res.status(200).json({ success: true, removed: result });
  } catch (error) {
    console.error('Device token removal error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Conditional Email routes
const conditionalEmailRoutes = require('./conditionalEmailRoutes');
router.use('/conditional-emails', conditionalEmailRoutes);

// ⚠️ DEV ONLY: Clear entire database
router.delete('/clear-database', async (req, res) => {
  try {
    const { prisma } = require('../lib/prisma');
    
    // Clear in order (respect foreign keys)
    await prisma.notification.deleteMany({});
    await prisma.emailJob.deleteMany({});
    await prisma.manualMail.deleteMany({});
    await prisma.eventHistory.deleteMany({});
    await prisma.emailSchedule.deleteMany({});
    await prisma.eventStore.deleteMany({});
    await prisma.lead.deleteMany({});
    
    // Clear Redis rate limit slots if needed
    try {
      const RateLimitService = require('../services/RateLimitService');
      await RateLimitService.clearAllSlots();
    } catch (e) { /* ignore if no redis */ }
    
    res.status(200).json({ 
      message: 'Database cleared successfully',
      clearedTables: ['notifications', 'emailJobs', 'manualMails', 'eventHistory', 'emailSchedules', 'eventStore', 'leads']
    });
  } catch (error) {
    console.error('Clear database error:', error);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;