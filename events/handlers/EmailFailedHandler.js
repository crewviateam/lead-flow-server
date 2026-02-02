// events/handlers/EmailFailedHandler.js
// Handles: blocked, error, invalid, and other failed events
// Pauses all scheduling, manual retry only, checks dead mail limit

const EventBus = require('../EventBus');
const { prisma } = require('../../lib/prisma');
const { EmailJobRepository, LeadRepository } = require('../../repositories');
const RulebookService = require('../../services/RulebookService');

EventBus.on('EmailFailed', async (payload) => {
  try {
    console.log("[FailedHandler] EmailFailed event received:", payload);

    const { emailJobId, email, eventData, type: emailType } = payload;

    const job = await EmailJobRepository.findById(emailJobId);
    if (!job) {
      console.warn(`[FailedHandler] Job ${emailJobId} not found.`);
      return;
    }

    const lead = await LeadRepository.findById(job.leadId);
    if (!lead) {
      console.warn(`[FailedHandler] Lead for job ${emailJobId} not found.`);
      return;
    }

    // Determine the specific failure type from event data
    const rawEventType =
      eventData?.event || eventData?.fetchedEventType || "failed";
    const failureReason =
      eventData?.reason || `Terminal failure: ${rawEventType}`;

    // Normalize the event type
    let eventType = rawEventType.toLowerCase();
    if (eventType === "hardbounces") eventType = "hard_bounce";
    if (eventType === "invalidemail") eventType = "invalid";

    // Get max retries for this job type
    const maxAttempts = await RulebookService.getMaxRetries(job.type);
    const currentRetryCount = job.retryCount || 0;

    // COUNT LEAD-LEVEL FAILURES: Check total failed jobs for this lead
    // This includes blocked, hard_bounce, invalid, error, spam across ALL jobs
    const terminalFailureStatuses = [
      "blocked",
      "hard_bounce",
      "invalid",
      "error",
      "spam",
      "dead",
    ];
    const leadFailureCount = await prisma.emailJob.count({
      where: {
        leadId: job.leadId,
        status: { in: terminalFailureStatuses },
      },
    });

    // Also get lead's totalRetries field (accumulates across manual retries)
    const leadTotalRetries = lead.totalRetries || 0;

    // Total failures = existing failed jobs + current failure + lead's retry history
    const totalFailures = leadFailureCount + 1 + leadTotalRetries;

    // Should mark as dead if total failures exceed max allowed
    const shouldMarkDead = totalFailures > maxAttempts;

    console.log(
      `[FailedHandler] Dead mail check: leadId=${job.leadId}, eventType=${eventType}, ` +
        `leadFailureCount=${leadFailureCount}, leadTotalRetries=${leadTotalRetries}, ` +
        `totalFailures=${totalFailures}, maxAttempts=${maxAttempts}, shouldMarkDead=${shouldMarkDead}`,
    );

    // Update job status to the specific failure type
    await prisma.emailJob.update({
      where: { id: parseInt(emailJobId) },
      data: {
        status: shouldMarkDead ? "dead" : eventType,
        failedAt: new Date(),
        lastError: failureReason,
        retryCount: currentRetryCount + 1,
      },
    });

    // Pause ALL scheduling for this lead (failed events require manual intervention)
    const activeStatuses = RulebookService.getActiveStatuses();
    const pausedJobs = await prisma.emailJob.updateMany({
      where: {
        leadId: job.leadId,
        status: { in: activeStatuses },
        id: { not: parseInt(emailJobId) },
      },
      data: {
        status: "paused",
        lastError: `Paused due to ${eventType} on ${job.type}`,
      },
    });

    console.log(
      `[FailedHandler] Paused ${pausedJobs.count} pending jobs for lead ${job.leadId}`,
    );

    // If max retries exceeded, mark lead as dead
    if (shouldMarkDead) {
      await prisma.lead.update({
        where: { id: job.leadId },
        data: {
          status: "dead",
          terminalState: "dead",
          terminalStateAt: new Date(),
          terminalReason: `Max retries exceeded after ${eventType} (${currentRetryCount + 1}/${maxAttempts})`,
          totalRetries: currentRetryCount + 1,
        },
      });

      // Cancel all paused jobs
      await prisma.emailJob.updateMany({
        where: {
          leadId: job.leadId,
          status: "paused",
        },
        data: {
          status: "cancelled",
          lastError: "Lead marked as dead - max retries exceeded",
        },
      });

      console.log(`[FailedHandler] Lead ${lead.email} marked as DEAD`);

      // Create notification
      await prisma.notification.create({
        data: {
          type: "warning",
          message: `Lead ${lead.name || lead.email} marked as dead`,
          details: `Max retries exceeded after ${eventType}. All pending emails cancelled.`,
          leadId: job.leadId,
          emailJobId: parseInt(emailJobId),
          event: "dead",
        },
      });
    } else {
      // Just update lead status - manual retry required
      const StatusUpdateService = require("../../services/StatusUpdateService");
      await StatusUpdateService._recalculateStatus(
        job.leadId,
        eventType,
        job.type,
      );

      // Create notification for failed event
      await prisma.notification.create({
        data: {
          type: "error",
          message: `Email failed for ${lead.name || lead.email}`,
          details: `${eventType}: ${failureReason}. Manual retry required.`,
          leadId: job.leadId,
          emailJobId: parseInt(emailJobId),
          event: eventType,
        },
      });
    }

    // Add event to history
    await LeadRepository.addEvent(
      job.leadId,
      eventType,
      {
        reason: failureReason,
        eventData: eventData,
        pausedJobs: pausedJobs.count,
        maxRetriesExceeded: shouldMarkDead,
        markedAsDead: shouldMarkDead,
        source: "EmailFailedHandler",
      },
      job.type,
      emailJobId,
    );

    console.log(
      `[FailedHandler] Processed ${eventType} for ${lead.email} - Manual retry required`,
    );
  } catch (error) {
    console.error('[FailedHandler] Error handling EmailFailed event:', error);
  }
});

console.log('[FailedHandler] Registered EmailFailed handler');