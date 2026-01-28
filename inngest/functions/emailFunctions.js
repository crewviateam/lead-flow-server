// inngest/functions/emailFunctions.js
// Durable functions for email sending, retries, and delivery tracking

const { inngest, EVENTS } = require('../client');
const BrevoEmailService = require('../../services/BrevoEmailService');
const { EmailJobRepository, LeadRepository } = require('../../repositories');
const RulebookService = require('../../services/RulebookService');
const eventBus = require('../../events/EventBus');
const { prisma } = require('../../lib/prisma');

/**
 * Send Email Function
 * Durable function with automatic retries and step management
 */
const sendEmail = inngest.createFunction(
  {
    id: 'send-email',
    name: 'Send Email',
    retries: 5,
    // Rate limit: 10 emails per second
    rateLimit: {
      limit: 10,
      period: '1s'
    }
  },
  { event: EVENTS.EMAIL_SEND },
  async ({ event, step }) => {
    const { emailJobId, leadId } = event.data;
    
    // Step 1: Fetch email job with retryable step
    const emailJob = await step.run('fetch-email-job', async () => {
      const job = await EmailJobRepository.findById(emailJobId);
      if (!job) throw new Error(`Email job ${emailJobId} not found`);
      return job;
    });
    
    // Step 2: Check if already processed
    const processedStatuses = RulebookService.getProcessedStatuses();
    if (processedStatuses.includes(emailJob.status)) {
      return { status: 'skipped', reason: `Already ${emailJob.status}` };
    }
    
    // Step 3: Fetch lead
    const lead = await step.run('fetch-lead', async () => {
      const l = await LeadRepository.findById(leadId);
      if (!l) throw new Error(`Lead ${leadId} not found`);
      return l;
    });
    
    // Step 4: Prepare and send email
    const sendResult = await step.run('send-email', async () => {
      // Get template
      const template = await prisma.emailTemplate.findFirst({
        where: { isDefault: true }
      });
      
      // Send via Brevo
      const result = await BrevoEmailService.sendEmail({
        to: lead.email,
        toName: lead.name,
        subject: emailJob.subject || template?.subject,
        body: emailJob.body || template?.body,
        tags: [emailJob.type, `lead:${leadId}`]
      });
      
      return result;
    });
    
    // Step 5: Update job status
    await step.run('update-job-status', async () => {
      await EmailJobRepository.update(emailJobId, {
        status: 'sent',
        sentAt: new Date(),
        messageId: sendResult.messageId
      });
      
      // Emit event
      await eventBus.emit('email.sent', {
        emailJobId,
        leadId,
        messageId: sendResult.messageId
      });
    });
    
    // Step 6: Update lead status
    await step.run('update-lead-status', async () => {
      const { status, reason } = await RulebookService.resolveLeadStatus(leadId);
      await LeadRepository.update(leadId, { status });
    });
    
    return { 
      status: 'sent', 
      messageId: sendResult.messageId,
      jobId: emailJobId 
    };
  }
);

/**
 * Retry Failed Email Function
 * Handles retry logic with exponential backoff
 */
const retryEmail = inngest.createFunction(
  {
    id: 'retry-email',
    name: 'Retry Failed Email',
    retries: 3
  },
  { event: EVENTS.EMAIL_RETRY },
  async ({ event, step }) => {
    const { emailJobId, reason } = event.data;
    
    // Step 1: Get the job and check retry limit
    const job = await step.run('check-retry-limit', async () => {
      const j = await EmailJobRepository.findById(emailJobId);
      if (!j) throw new Error(`Job ${emailJobId} not found`);
      
      const maxRetries = await RulebookService.getMaxRetries(j.type);
      if (j.retryCount >= maxRetries) {
        // Mark as dead
        await EmailJobRepository.update(emailJobId, {
          status: 'dead',
          lastError: 'Max retries exceeded'
        });
        return { exceeded: true, job: j };
      }
      
      return { exceeded: false, job: j };
    });
    
    if (job.exceeded) {
      return { status: 'dead', reason: 'Max retries exceeded' };
    }
    
    // Step 2: Calculate retry delay
    const delayHours = await step.run('calculate-delay', async () => {
      return await RulebookService.getRetryDelayHours();
    });
    
    // Step 3: Sleep for retry delay
    await step.sleep('wait-for-retry', `${delayHours}h`);
    
    // Step 4: Schedule retry
    await step.run('schedule-retry', async () => {
      await EmailJobRepository.update(emailJobId, {
        status: 'pending',
        retryCount: job.job.retryCount + 1,
        scheduledFor: new Date(),
        lastError: reason
      });
    });
    
    // Step 5: Trigger the send
    await step.sendEvent('trigger-send', {
      name: EVENTS.EMAIL_SEND,
      data: {
        emailJobId,
        leadId: job.job.leadId
      }
    });
    
    return { status: 'retrying', retryCount: job.job.retryCount + 1 };
  }
);

/**
 * Process Webhook Event Function
 * Handles incoming Brevo webhooks with deduplication
 */
const processWebhook = inngest.createFunction(
  {
    id: 'process-webhook',
    name: 'Process Brevo Webhook',
    // Deduplicate by messageId + event type
    idempotency: 'event.data.messageId + "-" + event.data.eventType',
    retries: 3
  },
  { event: EVENTS.WEBHOOK_RECEIVED },
  async ({ event, step }) => {
    const { messageId, eventType, email, timestamp } = event.data;
    
    // Step 1: Find the email job
    const job = await step.run('find-job', async () => {
      return await prisma.emailJob.findFirst({
        where: { messageId }
      });
    });
    
    if (!job) {
      return { status: 'skipped', reason: 'Job not found for messageId' };
    }
    
    // Step 2: Update job status based on event type
    const newStatus = await step.run('update-job-status', async () => {
      const statusMap = {
        'delivered': 'delivered',
        'opened': 'opened',
        'unique_opened': 'opened',
        'click': 'clicked',
        'hard_bounce': 'hard_bounce',
        'soft_bounce': 'soft_bounce',
        'blocked': 'blocked',
        'spam': 'complaint',
        'unsubscribed': 'unsubscribed',
        'error': 'error'
      };
      
      const newStatus = statusMap[eventType] || eventType;
      
      await EmailJobRepository.update(job.id, {
        status: newStatus,
        ...(eventType === 'delivered' && { deliveredAt: new Date(timestamp) }),
        ...(eventType === 'opened' && { openedAt: new Date(timestamp) }),
        ...(eventType === 'click' && { clickedAt: new Date(timestamp) })
      });
      
      // Emit event
      await eventBus.emit(`email.${newStatus}`, {
        emailJobId: job.id,
        leadId: job.leadId,
        eventType: newStatus
      });
      
      return newStatus;
    });
    
    // Step 3: Update lead status
    await step.run('update-lead-status', async () => {
      const { status } = await RulebookService.resolveLeadStatus(job.leadId);
      await LeadRepository.update(job.leadId, { status });
    });
    
    // Step 4: Trigger conditional emails if engagement event
    if (['opened', 'clicked'].includes(newStatus)) {
      await step.sendEvent('trigger-conditional', {
        name: EVENTS.CONDITIONAL_TRIGGER,
        data: {
          leadId: job.leadId,
          eventType: newStatus,
          sourceEmailType: job.type,
          sourceJobId: job.id
        }
      });
    }
    
    // Step 5: Handle failures - schedule retry
    if (['soft_bounce', 'deferred'].includes(newStatus)) {
      await step.sendEvent('schedule-retry', {
        name: EVENTS.EMAIL_RETRY,
        data: {
          emailJobId: job.id,
          reason: newStatus
        }
      });
    }
    
    return { 
      status: 'processed', 
      newStatus, 
      jobId: job.id 
    };
  }
);

module.exports = {
  sendEmail,
  retryEmail,
  processWebhook
};
