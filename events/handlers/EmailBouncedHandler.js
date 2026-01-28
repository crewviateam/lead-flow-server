// events/handlers/EmailBouncedHandler.js
// Event handler for bounce events using SmartDelayService
// Handles soft_bounce (auto-reschedule) and hard_bounce (check dead mail limit)

const EventBus = require('../EventBus');
const { prisma } = require('../../lib/prisma');
const { SettingsRepository, EmailJobRepository, LeadRepository } = require('../../repositories');
const RulebookService = require('../../services/RulebookService');
const SmartDelayService = require('../../services/SmartDelayService');

EventBus.on('EmailBounced', async (payload) => {
  try {
    console.log('[BouncedHandler] EmailBounced event received:', payload);

    const { emailJobId, email, eventData } = payload;
    
    const emailJob = await EmailJobRepository.findById(emailJobId);
    if (!emailJob) {
      console.warn(`[BouncedHandler] Job ${emailJobId} not found.`);
      return;
    }
    
    // Get max retries from RulebookService (reads from Settings)
    const maxAttempts = await RulebookService.getMaxRetries(emailJob.type);
    const currentRetryCount = emailJob.retryCount || 0;

    // Determine bounce type
    const isHardBounce = 
        eventData?.event === 'hard_bounce' || 
        eventData?.fetchedEventType === 'hardBounces' || 
        eventData?.reason?.toLowerCase().includes('hard');

    const eventType = isHardBounce ? 'hard_bounce' : 'soft_bounce';
    
    // Check if should mark as dead (max retries exceeded)
    const shouldMarkDead = await RulebookService.shouldMarkAsDead(
      { ...emailJob, retryCount: currentRetryCount + 1 }, 
      eventType
    );

    // SOFT BOUNCE: Auto-reschedule with smart delay
    if (!isHardBounce && !shouldMarkDead) {
      // Check for existing active job
      const existingActiveJob = await prisma.emailJob.findFirst({
        where: {
          leadId: emailJob.leadId,
          type: emailJob.type,
          status: { in: RulebookService.getActiveStatuses() },
          id: { not: parseInt(emailJobId) }
        }
      });

      if (existingActiveJob) {
        console.log(`[BouncedHandler] Active job already exists for ${emailJob.type} for lead ${emailJob.leadId}. Skipping.`);
        return;
      }
      
      console.log(`[BouncedHandler] Soft bounce - Rescheduling with smart delay (${currentRetryCount + 1}/${maxAttempts})`);
      
      // Get delay hours from settings (SmartDelayService will handle working hours/days)
      const delayHours = await RulebookService.getRetryDelayHours();
      
      // Update old job as rescheduled
      await prisma.emailJob.update({
        where: { id: parseInt(emailJobId) },
        data: { 
          status: 'rescheduled', 
          lastError: eventData?.reason || 'Soft Bounce - Rescheduled',
          retryCount: currentRetryCount + 1,  // Increment retry count on the old job BEFORE creating new one
          metadata: {
            ...emailJob.metadata,
            rescheduled: true,
            rescheduledAt: new Date().toISOString(),
            retryReason: 'soft_bounce'
          }
        }
      });

      // Create new job with smart delay schedule
      // Pass delay hours - rescheduleEmailJob will handle working hours
      const EmailSchedulerService = require('../../services/EmailSchedulerService');
      const newJob = await EmailSchedulerService.rescheduleEmailJob(
        emailJobId, 
        delayHours,  // Pass delay in hours, not Date
        'rescheduled'
      );
      
      // Calculate actual reschedule time for logging
      const rescheduleTime = newJob.scheduledFor;

      await EventBus.emit('EmailRescheduled', {
        leadId: emailJob.leadId,
        emailJobId: newJob.id,
        oldJobId: emailJobId,
        type: emailJob.type,
        scheduledFor: newJob.scheduledFor,
        reason: 'Soft Bounce - Smart Delay'
      });
      
      await LeadRepository.addEvent(emailJob.leadId, 'soft_bounce', {
        reason: eventData?.reason || 'Soft Bounce',
        retryCount: currentRetryCount + 1,
        maxAttempts,
        rescheduledFor: rescheduleTime,
        delayHours,
        source: 'EmailBouncedHandler'
      }, emailJob.type, emailJobId);

      console.log(`[BouncedHandler] Rescheduled to ${rescheduleTime} (delay: ${delayHours}hr)`);

    // HARD BOUNCE or MAX RETRIES EXCEEDED
    } else {
      const statusToSet = shouldMarkDead ? 'dead' : 'hard_bounce';
      const reason = shouldMarkDead 
        ? `Max retries exceeded (${currentRetryCount}/${maxAttempts})`
        : (eventData?.reason || 'Hard Bounce');
      
      console.log(`[BouncedHandler] ${statusToSet}: ${reason}`);
      
      // Update job status
      await prisma.emailJob.update({
        where: { id: parseInt(emailJobId) },
        data: { 
          status: statusToSet,
          failedAt: new Date(),
          lastError: reason
        }
      });

      // If dead mail, update lead terminal state and cancel all pending jobs
      if (shouldMarkDead) {
        await prisma.lead.update({
          where: { id: emailJob.leadId },
          data: {
            status: 'dead',
            terminalState: 'dead',
            terminalStateAt: new Date(),
            terminalReason: reason,
            totalRetries: currentRetryCount + 1
          }
        });

        // Cancel ALL pending jobs for this lead
        const cancelResult = await prisma.emailJob.updateMany({
          where: { 
            leadId: emailJob.leadId,
            status: { in: RulebookService.getActiveStatuses() },
            id: { not: parseInt(emailJobId) }
          },
          data: { 
            status: 'cancelled',
            lastError: 'Lead marked as dead - max retries exceeded'
          }
        });

        console.log(`[BouncedHandler] Lead marked as DEAD, cancelled ${cancelResult.count} pending jobs`);

        // Create notification
        const lead = await LeadRepository.findById(emailJob.leadId);
        await prisma.notification.create({
          data: {
            type: 'warning',
            message: `Lead ${lead?.name || lead?.email} marked as dead`,
            details: `${reason}. ${cancelResult.count} pending emails cancelled.`,
            leadId: emailJob.leadId,
            emailJobId: parseInt(emailJobId),
            event: 'dead'
          }
        });

      } else {
        // Hard bounce but not dead - recalculate lead status
        const StatusUpdateService = require('../../services/StatusUpdateService');
        await StatusUpdateService._recalculateStatus(emailJob.leadId, 'hard_bounce', emailJob.type);
        
        // If not Initial Email, try to schedule next followup
        const isInitial = emailJob.type?.toLowerCase().includes('initial');
        if (!isInitial) {
          console.log(`[BouncedHandler] Attempting to schedule next step after ${emailJob.type} hard bounced`);
          const EmailSchedulerService = require('../../services/EmailSchedulerService');
          try {
            await EmailSchedulerService.scheduleNextEmail(emailJob.leadId, 'failed_previous');
          } catch (schedErr) {
            console.log(`[BouncedHandler] No more steps to schedule: ${schedErr.message}`);
          }
        }
      }
      
      // Add event to history
      await LeadRepository.addEvent(emailJob.leadId, eventType, {
        reason: reason,
        eventData,
        maxRetriesExceeded: shouldMarkDead,
        markedAsDead: shouldMarkDead,
        source: 'EmailBouncedHandler'
      }, emailJob.type, emailJobId);
    }

  } catch (error) {
    console.error('[BouncedHandler] Error handling EmailBounced event:', error);
  }
});

console.log('[BouncedHandler] Registered EmailBounced handler');