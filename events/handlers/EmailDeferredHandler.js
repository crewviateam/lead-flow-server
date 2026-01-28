// events/handlers/EmailDeferredHandler.js
// Event handler using Prisma

const EventBus = require('../EventBus');
const { prisma } = require('../../lib/prisma');
const { SettingsRepository, EmailJobRepository } = require('../../repositories');

EventBus.on('EmailDeferred', async (payload) => {
  try {
    console.log('EmailDeferred event received:', payload);

    const { emailJobId, leadId, email, eventData } = payload;
    
    const emailJob = await EmailJobRepository.findById(emailJobId);
    if (!emailJob) {
      console.warn(`[DeferredHandler] Job ${emailJobId} not found.`);
      return;
    }

    const settings = await SettingsRepository.getSettings();
    const softBounceDelay = settings.retry?.softBounceDelayHours || 24;

    console.log(`Email deferred for job ${emailJobId}. Automatically rescheduling in ${softBounceDelay} hours...`);
    
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
    console.error('Error handling EmailDeferred event:', error);
  }
});
