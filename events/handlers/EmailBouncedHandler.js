// events/handlers/EmailBouncedHandler.js
// Event handler for bounce events using SmartDelayService
// Handles soft_bounce (auto-reschedule) and hard_bounce (check dead mail limit)

const EventBus = require("../EventBus");
const { prisma } = require("../../lib/prisma");
const {
  SettingsRepository,
  EmailJobRepository,
  LeadRepository,
} = require("../../repositories");
const RulebookService = require("../../services/RulebookService");
const SmartDelayService = require("../../services/SmartDelayService");
const { loggers } = require("../../lib/logger");
const log = loggers.events;

EventBus.on("EmailBounced", async (payload) => {
  try {
    log.info({ payload }, "EmailBounced event received");

    const { emailJobId, email, eventData } = payload;

    const emailJob = await EmailJobRepository.findById(emailJobId);
    if (!emailJob) {
      log.warn({ emailJobId }, "Job not found");
      return;
    }

    // Get max retries from RulebookService (reads from Settings)
    const maxAttempts = await RulebookService.getMaxRetries(emailJob.type);
    const currentRetryCount = emailJob.retryCount || 0;

    // Determine bounce type - deferred is treated as soft bounce
    const isDeferred =
      eventData?.event === "deferred" ||
      eventData?.fetchedEventType === "deferred";

    const isHardBounce =
      eventData?.event === "hard_bounce" ||
      eventData?.fetchedEventType === "hardBounces" ||
      eventData?.reason?.toLowerCase().includes("hard");

    // Deferred = soft bounce for reschedule purposes
    const eventType = isHardBounce
      ? "hard_bounce"
      : isDeferred
        ? "deferred"
        : "soft_bounce";

    log.info(
      { isDeferred, isHardBounce, eventType },
      "Processing bounce event",
    );

    // Check if should mark as dead (max retries exceeded)
    const shouldMarkDead = await RulebookService.shouldMarkAsDead(
      { ...emailJob, retryCount: currentRetryCount + 1 },
      eventType,
    );

    // SOFT BOUNCE: Auto-reschedule with smart delay
    if (!isHardBounce && !shouldMarkDead) {
      // Check for existing active job
      const existingActiveJob = await prisma.emailJob.findFirst({
        where: {
          leadId: emailJob.leadId,
          type: emailJob.type,
          status: { in: RulebookService.getActiveStatuses() },
          id: { not: parseInt(emailJobId) },
        },
      });

      if (existingActiveJob) {
        log.info(
          { leadId: emailJob.leadId, emailType: emailJob.type },
          "Active job already exists, skipping",
        );
        return;
      }

      log.info(
        { isDeferred, retryCount: currentRetryCount + 1, maxAttempts },
        "Rescheduling with smart delay",
      );

      // Get delay hours from settings (SmartDelayService will handle working hours/days)
      const delayHours = await RulebookService.getRetryDelayHours();

      // NOTE: Old job status update is handled by rescheduleEmailJob
      // Do NOT update status here to avoid race conditions

      // Create new job with smart delay schedule
      // Pass delay hours - rescheduleEmailJob will handle working hours
      const EmailSchedulerService = require("../../services/EmailSchedulerService");
      const newJob = await EmailSchedulerService.rescheduleEmailJob(
        emailJobId,
        delayHours, // Pass delay in hours, not Date
        "rescheduled",
      );

      // CRITICAL FIX: Update old job status to 'soft_bounce' AFTER reschedule
      // This preserves the soft_bounce status for analytics counting
      // (rescheduleEmailJob sets status to 'rescheduled' which is excluded from counts)
      await prisma.emailJob.update({
        where: { id: parseInt(emailJobId) },
        data: {
          status: "soft_bounce",
          lastError:
            eventData?.reason || "Soft Bounce - Automatically Rescheduled",
        },
      });

      // Calculate actual reschedule time for logging
      const rescheduleTime = newJob.scheduledFor;

      await EventBus.emit("EmailRescheduled", {
        leadId: emailJob.leadId,
        emailJobId: newJob.id,
        oldJobId: emailJobId,
        type: emailJob.type,
        scheduledFor: newJob.scheduledFor,
        reason: "Soft Bounce - Smart Delay",
      });

      await LeadRepository.addEvent(
        emailJob.leadId,
        "soft_bounce",
        {
          reason: eventData?.reason || "Soft Bounce",
          retryCount: currentRetryCount + 1,
          maxAttempts,
          rescheduledFor: rescheduleTime,
          delayHours,
          source: "EmailBouncedHandler",
        },
        emailJob.type,
        emailJobId,
      );

      log.info({ rescheduleTime, delayHours }, "Rescheduled soft bounce");

      // HARD BOUNCE or MAX RETRIES EXCEEDED
    } else {
      const statusToSet = shouldMarkDead ? "dead" : "hard_bounce";
      const reason = shouldMarkDead
        ? `Max retries exceeded (${currentRetryCount}/${maxAttempts})`
        : eventData?.reason || "Hard Bounce";

      log.info({ statusToSet, reason }, "Setting bounce status");

      // Update job status
      await prisma.emailJob.update({
        where: { id: parseInt(emailJobId) },
        data: {
          status: statusToSet,
          failedAt: new Date(),
          lastError: reason,
        },
      });

      // If dead mail, update lead terminal state and cancel all pending jobs
      if (shouldMarkDead) {
        await prisma.lead.update({
          where: { id: emailJob.leadId },
          data: {
            status: "dead",
            terminalState: "dead",
            terminalStateAt: new Date(),
            terminalReason: reason,
            totalRetries: currentRetryCount + 1,
          },
        });

        // Cancel ALL pending jobs for this lead
        const cancelResult = await prisma.emailJob.updateMany({
          where: {
            leadId: emailJob.leadId,
            status: { in: RulebookService.getActiveStatuses() },
            id: { not: parseInt(emailJobId) },
          },
          data: {
            status: "cancelled",
            lastError: "Lead marked as dead - max retries exceeded",
          },
        });

        log.warn(
          { leadId: emailJob.leadId, cancelledCount: cancelResult.count },
          "Lead marked as DEAD, cancelled pending jobs",
        );

        // Create notification
        const lead = await LeadRepository.findById(emailJob.leadId);
        await prisma.notification.create({
          data: {
            type: "warning",
            message: `Lead ${lead?.name || lead?.email} marked as dead`,
            details: `${reason}. ${cancelResult.count} pending emails cancelled.`,
            leadId: emailJob.leadId,
            emailJobId: parseInt(emailJobId),
            event: "dead",
          },
        });
      } else {
        // Hard bounce but not dead - recalculate lead status
        const StatusUpdateService = require("../../services/StatusUpdateService");
        await StatusUpdateService._recalculateStatus(
          emailJob.leadId,
          "hard_bounce",
          emailJob.type,
        );

        // If not Initial Email, try to schedule next followup
        const isInitial = emailJob.type?.toLowerCase().includes("initial");
        if (!isInitial) {
          log.info(
            { emailType: emailJob.type },
            "Attempting to schedule next step after hard bounce",
          );
          const EmailSchedulerService = require("../../services/EmailSchedulerService");
          try {
            await EmailSchedulerService.scheduleNextEmail(
              emailJob.leadId,
              "failed_previous",
            );
          } catch (schedErr) {
            log.debug({ error: schedErr.message }, "No more steps to schedule");
          }
        }
      }

      // Add event to history
      await LeadRepository.addEvent(
        emailJob.leadId,
        eventType,
        {
          reason: reason,
          eventData,
          maxRetriesExceeded: shouldMarkDead,
          markedAsDead: shouldMarkDead,
          source: "EmailBouncedHandler",
        },
        emailJob.type,
        emailJobId,
      );
    }
  } catch (error) {
    log.error(
      { error: error.message, stack: error.stack },
      "Error handling EmailBounced event",
    );
  }
});

log.info("Registered EmailBounced handler");