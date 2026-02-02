// services/ConditionalEmailService.js
// Core service for conditional email trigger evaluation and scheduling
// Uses RulebookService for action rules and trigger configuration

const { prisma } = require('../lib/prisma');
const ConditionalEmailRepository = require('../repositories/ConditionalEmailRepository');
const RulebookService = require('./RulebookService');
const moment = require('moment-timezone');

class ConditionalEmailService {
  
  /**
   * MAIN ENTRY POINT: Evaluate if any conditional emails should trigger
   * Called from AnalyticsService when events are received
   * 
   * @param {number} leadId - The lead that received the event
   * @param {string} eventType - The event type: 'opened', 'clicked', 'delivered'
   * @param {string} sourceEmailType - The email type that triggered the event: 'Initial Email', 'First Followup', etc.
   * @param {number} sourceJobId - The email job ID that triggered the event
   */
  async evaluateTriggers(leadId, eventType, sourceEmailType, sourceJobId = null) {
    console.log(`[ConditionalEmail] 📋 Evaluating triggers for lead ${leadId}: ${eventType} on ${sourceEmailType}`);
    
    try {
      // Find all enabled conditional emails that match this trigger
      const matchingConditionals = await ConditionalEmailRepository.findByTriggerEvent(eventType, sourceEmailType);
      
      if (matchingConditionals.length === 0) {
        console.log(`[ConditionalEmail] No conditional emails configured for ${eventType} on ${sourceEmailType}`);
        return [];
      }
      
      console.log(`[ConditionalEmail] Found ${matchingConditionals.length} matching conditional email(s)`);
      
      const triggeredJobs = [];
      
      for (const conditional of matchingConditionals) {
        try {
          const job = await this._triggerConditionalEmail(conditional, leadId, eventType, sourceJobId);
          if (job) {
            triggeredJobs.push(job);
          }
        } catch (error) {
          console.error(`[ConditionalEmail] Error triggering ${conditional.name}:`, error.message);
        }
      }
      
      return triggeredJobs;
    } catch (error) {
      console.error(`[ConditionalEmail] Error evaluating triggers:`, error);
      return [];
    }
  }
  
  /**
   * Trigger a specific conditional email for a lead
   */
  async _triggerConditionalEmail(conditional, leadId, eventType, sourceJobId) {
    // Check if this conditional email was already triggered for this lead
    const existingJob = await ConditionalEmailRepository.findJob(conditional.id, leadId);
    
    if (existingJob) {
      console.log(`[ConditionalEmail] ${conditional.name} already triggered for lead ${leadId} (status: ${existingJob.status})`);
      return null;
    }
    
    // DUPLICATE PREVENTION: Also check if there's already an EmailJob for this conditional email
    // This catches race conditions where multiple events trigger simultaneously
    const existingEmailJob = await prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        type: `conditional:${conditional.name}`,
        status: { in: RulebookService.getExistingNonCancelledStatuses() }
      }
    });
    
    if (existingEmailJob) {
      console.log(`[ConditionalEmail] EmailJob for ${conditional.name} already exists for lead ${leadId} (id: ${existingEmailJob.id}, status: ${existingEmailJob.status})`);
      return null;
    }
    
    console.log(`[ConditionalEmail] ✓ Triggering "${conditional.name}" for lead ${leadId}`);
    
    // Get action rules from RulebookService
    const actionRules = await RulebookService.getConditionalEmailActionRules();
    
    // Get lead for timezone
    const lead = await prisma.lead.findUnique({
      where: { id: parseInt(leadId) },
      include: { emailSchedule: true }
    });
    if (!lead) throw new Error(`Lead ${leadId} not found`);
    
    // Calculate MINIMUM time (now + delay in hours)
    const delayHours = conditional.delayHours || 0;
    const minTime = moment().add(delayHours, 'hours').toDate();
    
    // USE FCFS SLOT FINDING - validates business hours, working days, rate limits
    const { SettingsRepository } = require('../repositories');
    const settings = await SettingsRepository.getSettings();
    const EmailSchedulerService = require('./EmailSchedulerService');
    
    const slotResult = await EmailSchedulerService.findNextAvailableSlot(
      lead.timezone || 'UTC',
      minTime,
      settings
    );
    
    if (!slotResult.success) {
      console.error(`[ConditionalEmail] Failed to find slot for ${conditional.name}: ${slotResult.reason}`);
      throw new Error(`No available slot for conditional email: ${slotResult.reason}`);
    }
    
    const scheduledFor = slotResult.scheduledTime;
    console.log(`[ConditionalEmail] Delay ${delayHours}h -> Scheduled at ${moment(scheduledFor).format('YYYY-MM-DD HH:mm')} (slot: ${slotResult.slotInfo?.available}/${slotResult.slotInfo?.total} available)`);
    
    // Cancel pending followups if configured (check both conditional config AND rulebook action rules)
    let cancelledFollowups = [];
    if (conditional.cancelPending && actionRules.cancelPendingFollowupsIfConfigured) {
      cancelledFollowups = await this._cancelPendingFollowups(leadId, conditional.name);
    }
    
    // Create the conditional email job record
    const job = await ConditionalEmailRepository.createJob({
      conditionalEmailId: conditional.id,
      leadId: leadId,
      status: 'pending',
      scheduledFor: scheduledFor,
      triggeredByEvent: eventType,
      triggeredByJobId: sourceJobId,
      cancelledFollowups: cancelledFollowups.length > 0 ? cancelledFollowups : null
    });
    
    // Create an actual EmailJob for sending (now with validated scheduledFor)
    const emailJob = await this._createEmailJob(conditional, lead, scheduledFor, job.id);
    
    // Update conditional job with email job reference
    await ConditionalEmailRepository.updateJob(job.id, {
      emailJobId: emailJob.id,
      status: 'queued'
    });
    
    console.log(`[ConditionalEmail] 📧 Created job ${job.id} -> EmailJob ${emailJob.id} scheduled for ${scheduledFor}`);
    
    return { ...job, emailJob };
  }
  
  /**
   * Create an EmailJob for the conditional email
   * @param {Object} conditional - The conditional email config
   * @param {Object} lead - The lead object (already fetched)
   * @param {Date} scheduledFor - The validated scheduled time
   * @param {number} conditionalJobId - The conditional job ID
   */
  async _createEmailJob(conditional, lead, scheduledFor, conditionalJobId) {
    const leadId = lead.id;
    
    // RESERVE THE SLOT (atomic operation to prevent over-scheduling)
    const RateLimitService = require('./RateLimitService');
    const reservation = await RateLimitService.reserveSlot(lead.timezone || 'UTC', scheduledFor);
    
    if (!reservation.success) {
      console.error(`[ConditionalEmail] Slot reservation failed for ${conditional.name}`);
      throw new Error('Slot became unavailable during conditional email creation');
    }
    
    const emailJob = await prisma.emailJob.create({
      data: {
        leadId: parseInt(leadId),
        email: lead.email,
        type: `conditional:${conditional.name}`,
        category: "conditional",
        scheduledFor: reservation.reservedTime,
        status: "pending", // EmailJob status is 'pending' for queue/schedule pages
        templateId: conditional.templateId,
        metadata: {
          conditionalEmailId: conditional.id,
          conditionalJobId: conditionalJobId,
          triggerEvent: conditional.triggerEvent,
          triggerStep: conditional.triggerStep,
          priority: conditional.priority,
          timezone: lead.timezone,
        },
      },
    });

    // Add to BullMQ queue
    try {
      const { followupQueue } = require("../queues/emailQueues");
      const delay = Math.max(
        0,
        reservation.reservedTime.getTime() - Date.now(),
      );

      await followupQueue.add(
        "sendEmail",
        {
          leadId: leadId,
          emailJobId: emailJob.id,
          type: emailJob.type,
        },
        {
          delay,
          jobId: `conditional-${emailJob.id}`,
          priority: conditional.priority, // Higher priority = processed first
        },
      );

      console.log(
        `[ConditionalEmail] Added to queue with delay ${delay}ms, priority ${conditional.priority}`,
      );

      // Update Lead Status using the correct conditional format
      // Format: "condition {triggerEvent}:scheduled"
      try {
        const triggerEvent = conditional.triggerEvent; // 'opened', 'clicked', etc.
        const conditionalStatus = RulebookService.formatConditionalStatus(
          triggerEvent,
          "scheduled",
        );

        // Directly update lead status with correct format
        await prisma.lead.update({
          where: { id: parseInt(leadId) },
          data: { status: conditionalStatus },
        });

        console.log(
          `[ConditionalEmail] Lead ${leadId} status updated to: ${conditionalStatus}`,
        );

        // UPDATE EMAIL SCHEDULE (Sequence Progress)
        // Add this conditional email to the followups list so it shows in the UI
        const leadWithSchedule = await prisma.lead.findUnique({
          where: { id: parseInt(leadId) },
          include: { emailSchedule: true },
        });

        if (leadWithSchedule?.emailSchedule) {
          let followups = leadWithSchedule.emailSchedule.followups || [];
          if (!Array.isArray(followups)) {
            try {
              followups = JSON.parse(followups);
            } catch (e) {
              followups = [];
            }
          }

          // Remove if exists (re-scheduling)
          const conditionalName = `Conditional: ${conditional.name}`;
          const existingIdx = followups.findIndex(
            (f) => f.name === conditionalName || f.name === emailJob.type,
          );
          if (existingIdx > -1) {
            followups.splice(existingIdx, 1);
          }

          // Append new entry
          followups.push({
            name: `Conditional: ${conditional.name}`,
            scheduledFor: scheduledFor,
            status: "pending",
            order: 999, // High order to put at end, or 0 to put at start? Usually end.
            isConditional: true,
            jobId: emailJob.id,
          });

          await prisma.emailSchedule.update({
            where: { id: leadWithSchedule.emailSchedule.id },
            data: {
              followups,
              nextScheduledEmail:
                scheduledFor <
                (leadWithSchedule.emailSchedule.nextScheduledEmail ||
                  new Date("2099-01-01"))
                  ? scheduledFor
                  : leadWithSchedule.emailSchedule.nextScheduledEmail,
            },
          });
          console.log(
            `[ConditionalEmail] Added to Sequence Progress: ${conditionalName}`,
          );
        }
      } catch (statusErr) {
        console.error(
          `[ConditionalEmail] Failed to update lead status/schedule: ${statusErr.message}`,
        );
      }
    } catch (error) {
      console.error(`[ConditionalEmail] Error adding to queue:`, error.message);
    }
    
    return emailJob;
  }
  
  /**
   * PAUSE (not cancel) pending followups for a lead
   * Called when a conditional email is triggered
   * The paused followups will resume when the conditional email completes
   */
  async _cancelPendingFollowups(leadId, reason) {
    console.log(`[ConditionalEmail] 🔍 Looking for pending followups to PAUSE for lead ${leadId}`);
    
    const pendingFollowups = await prisma.emailJob.findMany({
      where: {
        leadId: parseInt(leadId),
        status: { in: RulebookService.getActiveStatuses() },
        type: { not: { startsWith: 'conditional:' } }, // Don't pause other conditional emails
        NOT: { type: { startsWith: 'manual' } } // Don't pause manual emails
      }
    });
    
    if (pendingFollowups.length === 0) {
      console.log(`[ConditionalEmail] No pending followups to pause for lead ${leadId}`);
      return [];
    }
    
    console.log(`[ConditionalEmail] Found ${pendingFollowups.length} pending jobs to evaluate`);
    
    const pausedIds = [];
    const { emailSendQueue, followupQueue } = require('../queues/emailQueues');
    
    for (const job of pendingFollowups) {
      // Skip Initial Email - never pause it
      if (job.type.toLowerCase().includes('initial')) {
        console.log(`[ConditionalEmail] Skipping Initial Email job ${job.id}`);
        continue;
      }
      
      // PAUSE the job in database (NOT cancel)
      await prisma.emailJob.update({
        where: { id: job.id },
        data: {
          status: 'paused',
          lastError: `Paused by conditional email: ${reason}`,
          cancellationReason: 'priority_paused'
        }
      });
      
      // Remove from BullMQ queues (will be re-added when conditional completes)
      try {
        const queueJobId = job.metadata?.queueJobId;
        if (queueJobId) {
          // Try both queues
          let removed = false;
          
          // Try emailSendQueue first
          try {
            const queueJob = await emailSendQueue.getJob(queueJobId);
            if (queueJob) {
              await queueJob.remove();
              removed = true;
            }
          } catch (e) { /* ignore */ }
          
          // Try followupQueue if not found
          if (!removed) {
            try {
              const queueJob = await followupQueue.getJob(queueJobId);
              if (queueJob) {
                await queueJob.remove();
                removed = true;
              }
            } catch (e) { /* ignore */ }
          }
          
          // Also try by email-{id} pattern
          if (!removed) {
            try {
              const altJobId = `email-${job.id}`;
              let queueJob = await emailSendQueue.getJob(altJobId);
              if (queueJob) await queueJob.remove();
            } catch (e) { /* ignore */ }
          }
        }
      } catch (err) {
        console.log(`[ConditionalEmail] Queue removal warning: ${err.message}`);
      }
      
      pausedIds.push(job.id);
      console.log(`[ConditionalEmail] ⏸️ Paused followup ${job.id} (${job.type})`);
    }
    
    // Mark lead as having paused followups
    if (pausedIds.length > 0) {
      await prisma.lead.update({
        where: { id: parseInt(leadId) },
        data: { followupsPaused: true }
      });
    }
    
    // Also update emailSchedule to mark followups as paused
    if (pausedIds.length > 0) {
      try {
        const lead = await prisma.lead.findUnique({
          where: { id: parseInt(leadId) },
          include: { emailSchedule: true }
        });
        
        if (lead?.emailSchedule) {
          let followups = lead.emailSchedule.followups || [];
          if (!Array.isArray(followups)) {
            try { followups = JSON.parse(followups); } catch (e) { followups = []; }
          }
          
          // Find paused job types
          const pausedTypes = pendingFollowups
            .filter(j => pausedIds.includes(j.id))
            .map(j => j.type);
          
          // Update followup statuses
          let updated = false;
          for (const followup of followups) {
            if (pausedTypes.includes(followup.name) && !RulebookService.getSuccessfullySentStatuses().includes(followup.status)) {
              followup.status = 'paused';
              updated = true;
            }
          }
          
          if (updated) {
            await prisma.emailSchedule.update({
              where: { id: lead.emailSchedule.id },
              data: { followups }
            });
          }
        }
      } catch (err) {
        console.log(`[ConditionalEmail] EmailSchedule update warning: ${err.message}`);
      }
      
      // Add event history
      await prisma.eventHistory.create({
        data: {
          leadId: parseInt(leadId),
          event: 'conditional_triggered',
          timestamp: new Date(),
          details: {
            reason: reason,
            pausedJobs: pausedIds
          },
          emailType: `conditional:${reason}`
        }
      });
    }
    
    console.log(`[ConditionalEmail] ✓ Paused ${pausedIds.length} pending followups`);
    return pausedIds;
  }
  
  /**
   * Get available trigger step options (for UI dropdown)
   */
  async getTriggerStepOptions() {
    const settings = await prisma.settings.findUnique({
      where: { id: 'global' }
    });
    
    const followups = settings?.followups || [];
    
    // Parse if string
    const parsed = typeof followups === 'string' ? JSON.parse(followups) : followups;
    
    return parsed.map(f => ({
      value: f.name,
      label: f.name
    }));
  }
  
  /**
   * Get statistics for conditional emails
   */
  async getStats() {
    const [total, enabled, triggered, pending] = await Promise.all([
      prisma.conditionalEmail.count(),
      prisma.conditionalEmail.count({ where: { enabled: true } }),
      prisma.conditionalEmailJob.count({ where: { status: 'sent' } }),
      prisma.conditionalEmailJob.count({ where: { status: { in: RulebookService.getPendingOnlyStatuses() } } })
    ]);
    
    return { total, enabled, triggered, pending };
  }
}

module.exports = new ConditionalEmailService();
