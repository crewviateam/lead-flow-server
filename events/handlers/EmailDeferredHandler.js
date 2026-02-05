// events/handlers/EmailDeferredHandler.js
// Event handler using Prisma with structured logging

const EventBus = require('../EventBus');
const { prisma } = require('../../lib/prisma');
const { SettingsRepository, EmailJobRepository } = require('../../repositories');
const { loggers } = require('../../lib/logger');
const log = loggers.events;

EventBus.on('EmailDeferred', async (payload) => {
  try {
    log.info({ payload }, 'EmailDeferred event received');

    const { emailJobId, leadId, email, eventData } = payload;
    
    const emailJob = await EmailJobRepository.findById(emailJobId);
    if (!emailJob) {
      log.warn({ emailJobId }, 'Job not found');
      return;
    }

    const settings = await SettingsRepository.getSettings();
    const softBounceDelay = settings.retry?.softBounceDelayHours || 24;

    log.info({ emailJobId, softBounceDelay }, 'Email deferred, automatically rescheduling');
    
    const EmailSchedulerService = require('../../services/EmailSchedulerService');
    const newJob = await EmailSchedulerService.rescheduleEmailJob(emailJobId, softBounceDelay, 'rescheduled');

    // Update the OLD job status
    await prisma.emailJob.update({
      where: { id: parseInt(emailJobId) },
      data: { 
        status: 'deferred', 
        lastError: eventData?.reason || 'Email Deferred - Automatically Rescheduled' 
      }
    });

    // Emit Reschedule event
    await EventBus.emit('EmailRescheduled', {
      leadId: emailJob.leadId,
      emailJobId: newJob.id,
      oldJobId: emailJobId,
      type: emailJob.type,
      scheduledFor: newJob.scheduledFor,
      reason: 'Email Deferred'
    });

  } catch (error) {
    log.error({ error: error.message, stack: error.stack }, 'Error handling EmailDeferred event');
  }
});
