// controllers/TestingController.js
// Developer Mode testing endpoints for debugging and testing all system features

const { prisma } = require('../lib/prisma');
const redisConnection = require('../config/redis');
const BrevoEmailService = require('../services/BrevoEmailService');
const { SettingsRepository, EmailJobRepository, LeadRepository } = require('../repositories');
const EventBus = require('../events/EventBus');

class TestingController {
  
  // ============================================
  // EMAIL TESTING
  // ============================================
  
  /**
   * Send a test email to a specified address
   * POST /api/dev/email/send-test
   */
  async sendTestEmail(req, res) {
    try {
      const { recipientEmail, templateId, leadData = {} } = req.body;
      
      if (!recipientEmail) {
        return res.status(400).json({ error: 'recipientEmail is required' });
      }
      
      const startTime = Date.now();
      
      // Build mock lead for variable substitution
      const mockLead = {
        name: leadData.name || 'Test User',
        email: recipientEmail,
        company: leadData.company || 'Test Company',
        city: leadData.city || 'Test City',
        country: leadData.country || 'Test Country',
        position: leadData.position || 'Test Position',
        ...leadData
      };
      
      // Create a mock email job
      const mockEmailJob = {
        id: 0,
        email: recipientEmail,
        type: 'developer_test',
        templateId: templateId ? parseInt(templateId) : null,
        idempotencyKey: `test_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        metadata: {
          isTest: true,
          testTimestamp: new Date().toISOString()
        }
      };
      
      const result = await BrevoEmailService.sendEmail(mockEmailJob, mockLead);
      
      const duration = Date.now() - startTime;
      
      res.json({
        success: true,
        messageId: result.messageId,
        duration: `${duration}ms`,
        recipient: recipientEmail,
        templateId: templateId || 'default',
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      console.error('[DevMode] Send test email error:', error);
      res.status(500).json({ 
        success: false, 
        error: error.message 
      });
    }
  }
  
  /**
   * Preview a template with variable substitution
   * POST /api/dev/email/preview
   */
  async previewTemplate(req, res) {
    try {
      const { templateId, leadData = {} } = req.body;
      
      if (!templateId) {
        return res.status(400).json({ error: 'templateId is required' });
      }
      
      const template = await prisma.emailTemplate.findUnique({
        where: { id: parseInt(templateId) }
      });
      
      if (!template) {
        return res.status(404).json({ error: 'Template not found' });
      }
      
      // Build variable replacements
      const mockLead = {
        name: leadData.name || 'John Doe',
        email: leadData.email || 'john@example.com',
        company: leadData.company || 'Acme Corp',
        city: leadData.city || 'New York',
        country: leadData.country || 'USA',
        position: leadData.position || 'CEO',
        ...leadData
      };
      
      const firstName = mockLead.name.split(' ')[0] || '';
      const lastName = mockLead.name.split(' ').slice(1).join(' ') || '';
      
      let renderedSubject = template.subject || '';
      let renderedBody = template.body || '';
      
      const replacements = {
        '{{name}}': mockLead.name,
        '{{firstName}}': firstName,
        '{{lastName}}': lastName,
        '{{email}}': mockLead.email,
        '{{company}}': mockLead.company,
        '{{companyName}}': mockLead.company,
        '{{city}}': mockLead.city,
        '{{country}}': mockLead.country,
        '{{position}}': mockLead.position,
        '{{title}}': mockLead.position
      };
      
      for (const [variable, value] of Object.entries(replacements)) {
        const regex = new RegExp(variable.replace(/[{}]/g, '\\$&'), 'gi');
        renderedSubject = renderedSubject.replace(regex, value);
        renderedBody = renderedBody.replace(regex, value);
      }
      
      res.json({
        success: true,
        template: {
          id: template.id,
          name: template.name,
          originalSubject: template.subject,
          originalBody: template.body
        },
        rendered: {
          subject: renderedSubject,
          body: renderedBody
        },
        variables: replacements,
        leadData: mockLead
      });
    } catch (error) {
      console.error('[DevMode] Preview template error:', error);
      res.status(500).json({ 
        success: false, 
        error: error.message 
      });
    }
  }
  
  /**
   * Get all templates for testing
   * GET /api/dev/email/templates
   */
  async getTemplates(req, res) {
    try {
      const templates = await prisma.emailTemplate.findMany({
        select: {
          id: true,
          name: true,
          subject: true,
          createdAt: true
        },
        orderBy: { name: 'asc' }
      });
      
      res.json({ success: true, templates });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
  
  // ============================================
  // WEBHOOK SIMULATION
  // ============================================
  
  /**
   * Simulate a Brevo webhook event
   * POST /api/dev/webhook/simulate
   */
  async simulateWebhook(req, res) {
    try {
      const { jobId, eventType, messageId } = req.body;
      
      if (!jobId || !eventType) {
        return res.status(400).json({ error: 'jobId and eventType are required' });
      }
      
      const validEvents = ['delivered', 'opened', 'clicked', 'soft_bounce', 'hard_bounce', 'blocked', 'spam', 'invalid', 'deferred', 'error'];
      if (!validEvents.includes(eventType.toLowerCase())) {
        return res.status(400).json({ 
          error: `Invalid eventType. Valid types: ${validEvents.join(', ')}` 
        });
      }
      
      // Find the job
      const job = await prisma.emailJob.findUnique({
        where: { id: parseInt(jobId) },
        include: { lead: true }
      });
      
      if (!job) {
        return res.status(404).json({ error: 'Email job not found' });
      }
      
      // Create simulated webhook payload
      const webhookPayload = {
        event: eventType.toLowerCase(),
        email: job.email,
        'message-id': messageId || job.brevoMessageId || `test_${Date.now()}`,
        date: new Date().toISOString(),
        ts_event: Math.floor(Date.now() / 1000),
        tag: `[DEV_TEST] Simulated ${eventType}`,
        // Mark as test event
        'X-Dev-Test': true
      };
      
      console.log(`[DevMode] Simulating ${eventType} webhook for job ${jobId}`);
      
      // Emit the event through EventBus
      await EventBus.emit(`email:${eventType.toLowerCase()}`, {
        emailJobId: job.id,
        leadId: job.leadId,
        eventType: eventType.toLowerCase(),
        messageId: webhookPayload['message-id'],
        timestamp: new Date(),
        isTest: true,
        rawPayload: webhookPayload
      });
      
      // Get updated job status
      const updatedJob = await prisma.emailJob.findUnique({
        where: { id: parseInt(jobId) },
        select: { id: true, status: true, updatedAt: true }
      });
      
      res.json({
        success: true,
        message: `Simulated ${eventType} event for job ${jobId}`,
        jobId: parseInt(jobId),
        eventType,
        previousStatus: job.status,
        newStatus: updatedJob?.status || 'unknown',
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      console.error('[DevMode] Simulate webhook error:', error);
      res.status(500).json({ 
        success: false, 
        error: error.message 
      });
    }
  }
  
  // ============================================
  // SCHEDULING TESTING
  // ============================================
  
  /**
   * Fast-forward a job to be sent immediately
   * POST /api/dev/job/fast-forward
   */
  async fastForwardJob(req, res) {
    try {
      const { jobId } = req.body;
      
      if (!jobId) {
        return res.status(400).json({ error: 'jobId is required' });
      }
      
      const job = await prisma.emailJob.findUnique({
        where: { id: parseInt(jobId) }
      });
      
      if (!job) {
        return res.status(404).json({ error: 'Email job not found' });
      }
      
      if (!['pending', 'scheduled', 'queued', 'rescheduled', 'deferred'].includes(job.status)) {
        return res.status(400).json({ 
          error: `Cannot fast-forward job with status: ${job.status}. Must be pending/scheduled/queued.` 
        });
      }
      
      const now = new Date();
      
      // Update job to be scheduled for now
      const updatedJob = await prisma.emailJob.update({
        where: { id: parseInt(jobId) },
        data: {
          scheduledFor: now,
          status: 'queued',
          metadata: {
            ...job.metadata,
            fastForwarded: true,
            fastForwardedAt: now.toISOString(),
            originalScheduledFor: job.scheduledFor?.toISOString()
          }
        }
      });
      
      // Add to BullMQ queue for immediate processing
      const { emailSendQueue } = require('../queues/emailQueues');
      await emailSendQueue.add(
        'send-email',
        {
          emailJobId: job.id,
          leadId: job.leadId,
          leadEmail: job.email,
          emailType: job.type
        },
        {
          jobId: `fast_forward_${job.id}_${Date.now()}`,
          delay: 0
        }
      );
      
      res.json({
        success: true,
        message: `Job ${jobId} fast-forwarded to immediate execution`,
        jobId: parseInt(jobId),
        originalScheduledFor: job.scheduledFor,
        newScheduledFor: now,
        status: 'queued'
      });
    } catch (error) {
      console.error('[DevMode] Fast-forward job error:', error);
      res.status(500).json({ 
        success: false, 
        error: error.message 
      });
    }
  }
  
  /**
   * Force trigger next followup for a lead
   * POST /api/dev/followup/trigger
   */
  async triggerFollowup(req, res) {
    try {
      const { leadId } = req.body;
      
      if (!leadId) {
        return res.status(400).json({ error: 'leadId is required' });
      }
      
      const lead = await LeadRepository.findById(leadId);
      if (!lead) {
        return res.status(404).json({ error: 'Lead not found' });
      }
      
      const EmailSchedulerService = require('../services/EmailSchedulerService');
      const result = await EmailSchedulerService.scheduleNextEmail(parseInt(leadId), 'pending');
      
      res.json({
        success: true,
        message: result ? 'Followup scheduled successfully' : 'No followup to schedule',
        leadId: parseInt(leadId),
        jobCreated: result ? {
          id: result.id,
          type: result.type,
          scheduledFor: result.scheduledFor
        } : null
      });
    } catch (error) {
      console.error('[DevMode] Trigger followup error:', error);
      res.status(500).json({ 
        success: false, 
        error: error.message 
      });
    }
  }
  
  /**
   * Check rate limiter status
   * GET /api/dev/rate-limit/status
   */
  async getRateLimitStatus(req, res) {
    try {
      const { timezone = 'UTC' } = req.query;
      
      const settings = await SettingsRepository.getSettings();
      const RateLimitService = require('../services/RateLimitService');
      
      // Get current window stats
      const windowMinutes = settings.rateLimit?.windowMinutes || 15;
      const emailsPerWindow = settings.rateLimit?.emailsPerWindow || 2;
      
      // Calculate window
      const now = new Date();
      const windowStart = Math.floor(now.getTime() / (windowMinutes * 60 * 1000)) * (windowMinutes * 60 * 1000);
      
      // Count emails in current window
      const count = await prisma.emailJob.count({
        where: {
          scheduledFor: {
            gte: new Date(windowStart),
            lt: new Date(windowStart + windowMinutes * 60 * 1000)
          },
          status: { in: ['sent', 'delivered', 'opened', 'clicked'] }
        }
      });
      
      res.json({
        success: true,
        timezone,
        currentWindow: {
          start: new Date(windowStart).toISOString(),
          end: new Date(windowStart + windowMinutes * 60 * 1000).toISOString(),
          windowMinutes
        },
        limits: {
          emailsPerWindow,
          currentCount: count,
          remaining: Math.max(0, emailsPerWindow - count)
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
  
  // ============================================
  // INTEGRATION TESTING
  // ============================================
  
  /**
   * Test Brevo API connection
   * GET /api/dev/status/brevo
   */
  async testBrevoConnection(req, res) {
    try {
      const axios = require('axios');
      const startTime = Date.now();
      
      const credentials = await BrevoEmailService.getCredentials();
      
      if (!credentials.apiKey) {
        return res.json({
          success: false,
          connected: false,
          error: 'No API key configured',
          duration: `${Date.now() - startTime}ms`
        });
      }
      
      // Test with account info endpoint
      const response = await axios.get('https://api.brevo.com/v3/account', {
        headers: { 'api-key': credentials.apiKey }
      });
      
      res.json({
        success: true,
        connected: true,
        account: {
          email: response.data.email,
          firstName: response.data.firstName,
          lastName: response.data.lastName,
          company: response.data.companyName
        },
        fromEmail: credentials.fromEmail,
        fromName: credentials.fromName,
        duration: `${Date.now() - startTime}ms`
      });
    } catch (error) {
      res.json({
        success: false,
        connected: false,
        error: error.response?.data?.message || error.message,
        duration: `${Date.now() - Date.now()}ms`
      });
    }
  }
  
  /**
   * Test Redis connection
   * GET /api/dev/status/redis
   */
  async testRedisConnection(req, res) {
    try {
      const startTime = Date.now();
      
      // Ping Redis
      const pong = await redisConnection.ping();
      
      // Get some stats
      const info = await redisConnection.info('memory');
      const memoryMatch = info.match(/used_memory_human:(\S+)/);
      
      res.json({
        success: true,
        connected: pong === 'PONG',
        ping: pong,
        memory: memoryMatch ? memoryMatch[1] : 'unknown',
        duration: `${Date.now() - startTime}ms`
      });
    } catch (error) {
      res.json({
        success: false,
        connected: false,
        error: error.message
      });
    }
  }
  
  /**
   * Test database connection
   * GET /api/dev/status/database
   */
  async testDatabaseConnection(req, res) {
    try {
      const startTime = Date.now();
      
      // Simple query to test connection
      const result = await prisma.$queryRaw`SELECT 1 as test`;
      
      // Get some counts
      const [leadCount, jobCount, templateCount] = await Promise.all([
        prisma.lead.count(),
        prisma.emailJob.count(),
        prisma.emailTemplate.count()
      ]);
      
      res.json({
        success: true,
        connected: true,
        stats: {
          leads: leadCount,
          emailJobs: jobCount,
          templates: templateCount
        },
        duration: `${Date.now() - startTime}ms`
      });
    } catch (error) {
      res.json({
        success: false,
        connected: false,
        error: error.message
      });
    }
  }
  
  // ============================================
  // DEBUG TOOLS
  // ============================================
  
  /**
   * Get queue status
   * GET /api/dev/queue/status
   */
  async getQueueStatus(req, res) {
    try {
      const { emailSendQueue, followupQueue } = require('../queues/emailQueues');
      
      const [
        emailWaiting,
        emailActive,
        emailCompleted,
        emailFailed,
        followupWaiting,
        followupActive
      ] = await Promise.all([
        emailSendQueue.getWaitingCount(),
        emailSendQueue.getActiveCount(),
        emailSendQueue.getCompletedCount(),
        emailSendQueue.getFailedCount(),
        followupQueue.getWaitingCount(),
        followupQueue.getActiveCount()
      ]);
      
      // Get pending jobs from DB
      const pendingJobs = await prisma.emailJob.count({
        where: { status: { in: ['pending', 'scheduled', 'queued'] } }
      });
      
      res.json({
        success: true,
        emailSendQueue: {
          waiting: emailWaiting,
          active: emailActive,
          completed: emailCompleted,
          failed: emailFailed
        },
        followupQueue: {
          waiting: followupWaiting,
          active: followupActive
        },
        database: {
          pendingJobs
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
  
  /**
   * Inspect a specific job
   * GET /api/dev/job/:id/inspect
   */
  async inspectJob(req, res) {
    try {
      const { id } = req.params;
      
      const job = await prisma.emailJob.findUnique({
        where: { id: parseInt(id) },
        include: {
          lead: {
            select: { id: true, name: true, email: true, status: true }
          },
          template: {
            select: { id: true, name: true, subject: true }
          }
        }
      });
      
      if (!job) {
        return res.status(404).json({ error: 'Job not found' });
      }
      
      res.json({ success: true, job });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
  
  /**
   * Inspect a specific lead
   * GET /api/dev/lead/:id/inspect
   */
  async inspectLead(req, res) {
    try {
      const { id } = req.params;
      
      const lead = await prisma.lead.findUnique({
        where: { id: parseInt(id) },
        include: {
          emailJobs: {
            orderBy: { createdAt: 'desc' },
            take: 20,
            select: {
              id: true,
              type: true,
              status: true,
              scheduledFor: true,
              sentAt: true,
              templateId: true
            }
          },
          emailSchedule: true,
          eventHistory: {
            orderBy: { createdAt: 'desc' },
            take: 20
          },
          manualMails: true
        }
      });
      
      if (!lead) {
        return res.status(404).json({ error: 'Lead not found' });
      }
      
      res.json({ success: true, lead });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
  
  /**
   * Get recent leads for testing
   * GET /api/dev/leads
   */
  async getRecentLeads(req, res) {
    try {
      const { limit = 20 } = req.query;
      
      const leads = await prisma.lead.findMany({
        take: parseInt(limit),
        orderBy: { createdAt: 'desc' },
        select: {
          id: true,
          name: true,
          email: true,
          company: true,
          status: true,
          createdAt: true
        }
      });
      
      res.json({ success: true, leads });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
  
  /**
   * Get recent email jobs for testing
   * GET /api/dev/jobs
   */
  async getRecentJobs(req, res) {
    try {
      const { limit = 20, status } = req.query;
      
      const where = {};
      if (status) {
        where.status = status;
      }
      
      const jobs = await prisma.emailJob.findMany({
        where,
        take: parseInt(limit),
        orderBy: { createdAt: 'desc' },
        select: {
          id: true,
          email: true,
          type: true,
          status: true,
          scheduledFor: true,
          sentAt: true,
          templateId: true,
          lead: {
            select: { id: true, name: true }
          }
        }
      });
      
      res.json({ success: true, jobs });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
  
  /**
   * Clear test data
   * DELETE /api/dev/test-data
   */
  async clearTestData(req, res) {
    try {
      // Only delete jobs marked as test
      const deletedJobs = await prisma.emailJob.deleteMany({
        where: {
          OR: [
            { type: 'developer_test' },
            { metadata: { path: ['isTest'], equals: true } }
          ]
        }
      });
      
      res.json({
        success: true,
        message: 'Test data cleared',
        deleted: {
          jobs: deletedJobs.count
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }
}

module.exports = new TestingController();
