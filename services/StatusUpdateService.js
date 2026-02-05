// StatusUpdateService.js
// Centralized service for handling all lead status updates using Prisma
// Uses RulebookService as single source of truth for all rules

const { prisma } = require('../lib/prisma');
const { LeadRepository, EmailJobRepository } = require('../repositories');
const RulebookService = require('./RulebookService');

/**
 * Centralized service for handling all lead status updates.
 * Ensures that events are logged and lead status is properly updated.
 * Uses RulebookService for all rule-based decisions.
 */
class StatusUpdateService {
  /**
   * Update lead status based on an event
   * @param {number} leadId - The ID of the lead
   * @param {string} event - The event name (e.g., 'sent', 'delivered', 'opened', 'clicked')
   * @param {Object} details - Additional details for the event (e.g., messageId)
   * @param {number} emailJobId - The ID of the email job (optional)
   * @param {string} emailType - The type of email (e.g., 'Initial Email', 'Followup 1')
   */
  async updateStatus(leadId, event, details = {}, emailJobId = null, emailType = null) {
    try {
      console.log(
        `[StatusUpdateService] Processing ${event} for lead ${leadId}`,
      );

      const lead = await LeadRepository.findById(leadId, {
        include: { emailSchedule: true },
      });
      if (!lead) {
        console.error(`[StatusUpdateService] Lead ${leadId} not found`);
        return null;
      }

      // If emailType or emailJobId is provided, try to fetch job to populate details
      if (emailJobId && !emailType) {
        const job = await EmailJobRepository.findById(emailJobId);
        if (job) {
          emailType = job.type;
        }
      }

      // 1. Add Event to history
      await LeadRepository.addEvent(
        leadId,
        event,
        details,
        emailType,
        emailJobId,
      );

      // 2. Update counters based on event type
      await this._updateCounters(leadId, event);

      // 3. Recalculate and update lead status
      const newStatus = await this._recalculateStatus(leadId, event, emailType);

      // 4. AUTO-RESUME: If a high-priority mail completed/cancelled, resume paused jobs
      // This ensures followups/manual mails resume after conditional completes
      if (
        emailType &&
        RulebookService.triggersAutoResume(emailType) &&
        RulebookService.shouldTriggerAutoResume(event)
      ) {
        console.log(
          `[StatusUpdateService] Triggering auto-resume for paused jobs after ${emailType} -> ${event}`,
        );
        try {
          const resumeResult = await RulebookService.resumePausedJobsAfter(
            leadId,
            emailType,
            event,
          );
          if (resumeResult.resumedCount > 0) {
            console.log(
              `[StatusUpdateService] ✓ Auto-resumed ${resumeResult.resumedCount} paused jobs`,
            );
          }
        } catch (resumeErr) {
          console.error(
            `[StatusUpdateService] Auto-resume error:`,
            resumeErr.message,
          );
          // Don't throw - main update succeeded
        }
      }

      // 5. Fetch updated lead
      const updatedLead = await LeadRepository.findById(leadId, {
        include: { emailSchedule: true },
      });

      console.log(
        `[StatusUpdateService] Updated lead ${updatedLead.email} status to: ${updatedLead.status}`,
      );
      return updatedLead;
    } catch (error) {
      console.error(`[StatusUpdateService] Error updating status: ${error.message}`, error);
      throw error;
    }
  }

  /**
   * Update counters based on event type
   * Uses RulebookService for score adjustments
   */
  async _updateCounters(leadId, event) {
    const counterMap = {
      'sent': 'emailsSent',
      'delivered': null, // No counter for delivered
      'opened': 'emailsOpened',
      'clicked': 'emailsClicked',
      'bounced': 'emailsBounced',
      'hard_bounce': 'emailsBounced',
      'soft_bounce': 'emailsBounced'
    };

    const counter = counterMap[event];
    if (counter) {
      await LeadRepository.incrementCounter(leadId, counter, 1);
    }

    // Use RulebookService for score adjustments
    const scoreDelta = await RulebookService.getScoreAdjustment(event);
    if (scoreDelta) {
      await LeadRepository.updateScore(leadId, scoreDelta);
    }
  }

  /**
   * Recalculate lead status based on current state
   */
  async _recalculateStatus(leadId, event, emailType = null) {
    const lead = await LeadRepository.findById(leadId);
    if (!lead) return null;
    
    const currentStatus = lead.status || '';
    const currentStatusPart = currentStatus.split(':')[1] || currentStatus;
    
    // SPECIAL STATES: Check for lead-level flags first
    // These override regular status calculation
    if (lead.status === 'converted') {
      console.log(`[StatusUpdateService] Lead ${leadId} is converted, keeping status`);
      return 'converted';
    }
    
    if (lead.frozenUntil && new Date(lead.frozenUntil) > new Date()) {
      console.log(`[StatusUpdateService] Lead ${leadId} is frozen, setting status to frozen`);
      await LeadRepository.updateStatus(leadId, 'frozen');
      return 'frozen';
    }
    
    if (lead.followupsPaused) {
      // If followups are paused due to a conditional email, keep the conditional status
      // Don't overwrite with generic 'paused'
      if (currentStatus && currentStatus.startsWith('condition ')) {
        console.log(`[StatusUpdateService] Lead ${leadId} has followups paused but keeping conditional status: ${currentStatus}`);
        return currentStatus;
      }
      console.log(`[StatusUpdateService] Lead ${leadId} has followups paused, setting status to paused`);
      await LeadRepository.updateStatus(leadId, 'paused');
      return 'paused';
    }
    
    // Status priority levels (higher = more important, won't be overwritten)
    const getStatusPriority = (status) => {
      const s = (status || '').toLowerCase();
      // Terminal states - highest priority
      if (['converted', 'unsubscribed'].includes(s)) return 100;
      // Frozen/Paused - high priority
      if (['frozen', 'paused'].includes(s)) return 90;
      // Scheduled/Rescheduled - HIGHER than failure to allow retries to update status
      if (['scheduled', 'rescheduled', 'queued', 'pending'].includes(s)) return 85;
      // Failure states - high priority
      if (RulebookService.getFailureStatuses().includes(s)) return 80;
      // Engagement events - lower priority
      if (['clicked'].includes(s)) return 40;
      if (['opened', 'unique_opened'].includes(s)) return 35;
      if (['delivered'].includes(s)) return 30;
      if (['sent'].includes(s)) return 25;
      // Skipped/cancelled
      if (['skipped', 'cancelled'].includes(s)) return 20;
      // Idle/unknown
      return 10;
    };
    
    const currentPriority = getStatusPriority(currentStatusPart);
    const newEventPriority = getStatusPriority(event);
    
    console.log(`[StatusUpdateService] Lead ${leadId}: current="${currentStatus}" (priority=${currentPriority}), event="${event}" (priority=${newEventPriority})`);
    
    // RULE 1: Never downgrade from terminal states
    if (currentPriority >= 100) {
      console.log(`[StatusUpdateService] Keeping terminal status: ${currentStatus}`);
      return currentStatus;
    }
    
    // RULE 2: Never downgrade from frozen/paused
    if (currentPriority >= 90 && newEventPriority < 90) {
      console.log(`[StatusUpdateService] Keeping frozen/paused status: ${currentStatus}`);
      return currentStatus;
    }
    
    // RULE 3: Failure events always take priority (using RulebookService)
    if (RulebookService.isFailureStatus(event)) {
      const newStatus = `${emailType || 'email'}:${event}`;
      console.log(`[StatusUpdateService] Setting failure status: ${newStatus}`);
      await LeadRepository.updateStatus(leadId, newStatus);
      return newStatus;
    }
    
    // RULE 4: Never downgrade from failure status with engagement events
    if (currentPriority >= 80 && newEventPriority < 80) {
      console.log(`[StatusUpdateService] Protecting failure status "${currentStatus}" from "${event}"`);
      return currentStatus;
    }
    
    // RULE X: CONDITIONAL EMAIL STATUS HANDLING
    // For conditional emails, check if there's a DIFFERENT pending job first
    // If yes, show that pending job's status instead of the conditional status
    // This ensures lead status reflects the next scheduled action after conditional completes
    if (emailType && emailType.startsWith('conditional:')) {
      // FIRST: Check if there's another pending job (not this conditional)
      const otherPendingJob = await prisma.emailJob.findFirst({
        where: {
          leadId: parseInt(leadId),
          status: { in: RulebookService.getActiveStatuses() },
          NOT: { type: emailType }  // Exclude this conditional email
        },
        orderBy: { scheduledFor: 'asc' }
      });
      
      // If another job is pending OR if this event is 'delivered' (completion event),
      // let the normal logic below handle it to show the next scheduled job
      if (otherPendingJob || event === 'delivered' || event === 'cancelled') {
        console.log(`[StatusUpdateService] Conditional ${emailType} has ${event}, checking for next pending job...`);
        // Don't return early - continue to RULE 5 below to properly set status based on next pending job
      } else {
        // No other pending job and conditional is still in progress - show conditional status
        const emailJob = await prisma.emailJob.findFirst({
          where: { 
            leadId: parseInt(leadId), 
            type: emailType,
            status: { in: [...RulebookService.getSuccessfullySentStatuses(), 'sent', 'pending', event] }
          },
          orderBy: { updatedAt: 'desc' }
        });
        
        const triggerEvent = emailJob?.metadata?.triggerEvent || 'opened';
        const newStatus = RulebookService.formatConditionalStatus(triggerEvent, event);
        console.log(`[StatusUpdateService] Conditional email ${emailType} - setting status to: ${newStatus}`);
        await LeadRepository.updateStatus(leadId, newStatus);
        return newStatus;
      }
    }
    
    // RULE 5: Check for pending jobs - scheduled status should show next action
    const pendingJob = await prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        status: { in: RulebookService.getActiveStatuses() }
      },
      orderBy: { scheduledFor: 'asc' }
    });
    
    let finalStatus;
    
    // STRICT LEAD STATUS ENUM (from RulebookService):
    // - {mail_type}:scheduled
    // - {mail_type}:rescheduled
    // - frozen / converted / idle
    // - {mail_type}:sent
    // - {mail_type}:blocked/failed/hard_bounce
    // Engagement events (opened, clicked, delivered) should NOT overwrite lead status
    
    // Check if event is forbidden for lead status (uses RulebookService)
    const isForbiddenEvent = await RulebookService.isStatusForbiddenForLead(event);
    
    if (pendingJob) {
      // Show next scheduled job - ALWAYS prioritize showing scheduled status
      const isRescheduled = pendingJob.metadata?.rescheduled || pendingJob.metadata?.retryReason;
      let statusWord = isRescheduled ? 'rescheduled' : 'scheduled';
      
      // For conditional emails, use proper format: 'condition {trigger}:{status}'
      // Job type is 'conditional:Name' but lead status should be 'condition {trigger}:scheduled'
      if (
        pendingJob.type.startsWith("conditional:") &&
        pendingJob.metadata?.triggerEvent
      ) {
        finalStatus = RulebookService.formatConditionalStatus(
          pendingJob.metadata.triggerEvent,
          statusWord,
        );
      } else {
        // Use RulebookService for simplified type names
        let displayType = RulebookService.getSimplifiedTypeName(
          pendingJob.type,
          pendingJob.metadata,
        );
        finalStatus = `${displayType}:${statusWord}`;
      }
      console.log(`[StatusUpdateService] Found pending job: ${pendingJob.type}, setting status: ${finalStatus}`);
    } else if (isForbiddenEvent) {
      // ENGAGEMENT EVENTS: Do NOT update lead status, keep current or set to idle
      // Engagement events only update the email job status, not lead status
      console.log(`[StatusUpdateService] Forbidden event '${event}' - keeping lead status unchanged`);
      
      // Check if current status is valid, otherwise check for last sent email
      if (currentStatus && !currentStatus.includes('pending')) {
        return currentStatus;
      }
      
      // Find the most recent sent/completed job
      const lastSentJob = await prisma.emailJob.findFirst({
        where: {
          leadId: parseInt(leadId),
          status: { in: RulebookService.getSuccessfullySentStatuses() }
        },
        orderBy: { sentAt: 'desc' }
      });
      
      if (lastSentJob) {
        finalStatus = `${RulebookService.getSimplifiedTypeName(lastSentJob.type, lastSentJob.metadata)}:sent`;
      } else {
        finalStatus = 'idle';
      }
    } else if (RulebookService.isFailureStatus(event)) {
      // Failure events - these ARE allowed in lead status
      finalStatus = `${RulebookService.getSimplifiedTypeName(emailType)}:${event}`;
    } else if (event === 'sent') {
      // Sent is allowed in lead status
      finalStatus = `${RulebookService.getSimplifiedTypeName(emailType)}:sent`;
    } else if (['cancelled', 'paused', 'skipped'].includes(event)) {
      // CANCELLED/PAUSED/SKIPPED: Check for other scheduled mails, otherwise show idle
      // These events should NOT be shown as lead status
      console.log(`[StatusUpdateService] ${event} event - checking for other scheduled mails`);
      
      // Check for any other pending/scheduled job
      const otherPendingJob = await prisma.emailJob.findFirst({
        where: {
          leadId: parseInt(leadId),
          status: { in: RulebookService.getActiveStatuses() }
        },
        orderBy: { scheduledFor: 'asc' }
      });
      
      if (otherPendingJob) {
        // Found another scheduled job - show that status
        const isRescheduled = otherPendingJob.metadata?.rescheduled || otherPendingJob.metadata?.retryReason;
        let displayType = RulebookService.getSimplifiedTypeName(otherPendingJob.type, otherPendingJob.metadata);
        let statusWord = isRescheduled ? 'rescheduled' : 'scheduled';
        finalStatus = `${displayType}:${statusWord}`;
        console.log(`[StatusUpdateService] Found other pending job: ${otherPendingJob.type}, setting status: ${finalStatus}`);
      } else {
        // No other scheduled mails - show idle
        finalStatus = 'idle';
        console.log(`[StatusUpdateService] No other scheduled mails, setting status: idle`);
      }
    } else {
      // For other events, check if sequence is complete
      const settings = await require('../repositories/SettingsRepository').getSettings();
      const sequence = (settings.followups || []).filter(f => f.enabled && !f.globallySkipped);
      
      const completedStatuses = RulebookService.getSuccessfullySentStatuses();
      const allCompleted = await Promise.all(sequence.map(async step => {
        const job = await prisma.emailJob.findFirst({
          where: { leadId: parseInt(leadId), type: step.name, status: { in: completedStatuses } }
        });
        return !!job;
      }));
      
      if (allCompleted.every(Boolean)) {
        finalStatus = 'sequence_complete';
      } else if (currentStatus && currentStatus.includes(':')) {
        // Keep current valid status
        return currentStatus;
      } else {
        finalStatus = 'idle';
      }
    }
    
    console.log(`[StatusUpdateService] Updating status to: ${finalStatus}`);
    await LeadRepository.updateStatus(leadId, finalStatus);
    return finalStatus;
  }
}

module.exports = new StatusUpdateService();
