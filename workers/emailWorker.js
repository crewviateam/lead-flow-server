// workers/emailWorker.js
// Email send worker using BullMQ

const { Worker } = require('bullmq');
const redisConnection = require('../config/redis');
const BrevoEmailService = require('../services/BrevoEmailService');
const { EmailJobRepository, LeadRepository } = require('../repositories');
const { prisma } = require('../lib/prisma');
const UniqueJourneyService = require('../services/UniqueJourneyService');
const RulebookService = require('../services/RulebookService');
const { loggers } = require('../lib/logger');
const log = loggers.worker;

let worker = null;

const startWorker = () => {
  if (worker) return worker;

  worker = new Worker(
    "email-send-queue",
    async (job) => {
      const { emailJobId, leadId, leadEmail, emailType } = job.data;
      const parsedJobId = parseInt(emailJobId);
      const parsedLeadId = parseInt(leadId);

      try {
        // Get the email job
        const emailJob = await EmailJobRepository.findById(parsedJobId);
        if (!emailJob) {
          // Return early instead of throwing - no point retrying if job was deleted
          log.warn(
            { jobId: parsedJobId },
            "Job not found in database, skipping (may have been deleted)",
          );
          return { status: "skipped", reason: "Job not found in database" };
        }

        // Skip if already processed (including opened/clicked which are terminal)
        const processedStatuses = RulebookService.getProcessedStatuses();
        if (processedStatuses.includes(emailJob.status)) {
          log.info(
            { jobId: parsedJobId, status: emailJob.status },
            "DUPLICATE PREVENTED: Job already processed",
          );
          return { status: "skipped", reason: `Already ${emailJob.status}` };
        }

        // RACE CONDITION PREVENTION: Check again right before processing
        // This catches cases where another worker processed the same job
        const freshJob = await prisma.emailJob.findUnique({
          where: { id: parsedJobId },
        });
        if (freshJob && processedStatuses.includes(freshJob.status)) {
          log.info(
            { jobId: parsedJobId, status: freshJob.status },
            "DUPLICATE PREVENTED (race): Status changed",
          );
          return {
            status: "skipped",
            reason: `Status changed to ${freshJob.status}`,
          };
        }

        // Get lead details
        const lead = await LeadRepository.findById(parsedLeadId);
        if (!lead) {
          throw new Error(`Lead ${parsedLeadId} not found`);
        }

        // ============================================
        // CRITICAL: SEND-TIME DUPLICATE PREVENTION
        // Check if this email type was already sent to this lead
        // This is the FINAL guard against duplicates
        // ============================================
        const alreadySent = await UniqueJourneyService.hasBeenSent(
          parsedLeadId,
          emailJob.type,
        );
        if (alreadySent) {
          log.warn(
            { leadId: parsedLeadId, emailType: emailJob.type },
            "DUPLICATE BLOCKED: Email type already sent to lead",
          );
          await prisma.emailJob.update({
            where: { id: parsedJobId },
            data: {
              status: "cancelled",
              lastError:
                "Duplicate prevention - email type already sent to this lead",
            },
          });
          return { status: "cancelled", reason: "Duplicate - already sent" };
        }

        // ============================================
        // LATE-BINDING TEMPLATE RESOLUTION
        // Fetch the CURRENT template from settings at send time
        // This ensures user's latest template changes take effect
        // even for already-scheduled emails
        //
        // SKIP for manual emails - they use their stored templateId
        // because they are fully user-managed
        // ============================================
        const { SettingsRepository } = require("../repositories");
        let effectiveTemplateId = emailJob.templateId; // Default to stored value

        // Check if this is a manual email (skip late-binding for manual)
        const isManualEmail =
          emailJob.type === "manual" ||
          emailJob.type?.startsWith("manual:") ||
          emailJob.metadata?.manual === true ||
          emailJob.metadata?.manualHtmlContent;

        // For followups/initial ONLY (not manual), get the latest templateId from current settings
        if (!isManualEmail) {
          try {
            const settings = await SettingsRepository.getSettings();
            const followups = settings.followups || [];
            const matchingFollowup = followups.find(
              (f) => f.name === emailJob.type,
            );

            if (matchingFollowup && matchingFollowup.templateId) {
              if (matchingFollowup.templateId !== emailJob.templateId) {
                log.info(
                  {
                    emailType: emailJob.type,
                    oldTemplateId: emailJob.templateId,
                    newTemplateId: matchingFollowup.templateId,
                  },
                  "LATE-BINDING: Template changed",
                );
              }
              effectiveTemplateId = matchingFollowup.templateId;
            }
          } catch (err) {
            log.warn(
              { error: err.message },
              "Could not fetch settings for late-binding",
            );
            // Continue with stored templateId
          }
        } else {
          log.debug(
            { templateId: emailJob.templateId },
            "MANUAL EMAIL: Using stored templateId (no late-binding)",
          );
        }

        // ============================================
        // CRITICAL: ATOMIC SEND ATTEMPT MARKING
        // Use atomic updateMany to claim this job before sending
        // This prevents race conditions with concurrency=5 workers
        // Only ONE worker can successfully mark the send attempt
        // ============================================
        const sendAttemptMarked =
          await UniqueJourneyService.markSendAttempt(parsedJobId);
        if (!sendAttemptMarked) {
          log.info(
            { jobId: parsedJobId },
            "Job already being processed by another worker, skipping",
          );
          return {
            status: "skipped",
            reason: "Already being processed by another worker",
          };
        }

        // Send the email via Brevo
        // Use effectiveTemplateId (late-bound) instead of stored templateId
        log.info(
          {
            jobId: parsedJobId,
            emailType: emailJob.type,
            templateId: effectiveTemplateId || "none",
          },
          "Sending email",
        );

        // Override templateId with late-bound value
        const jobWithLatestTemplate = {
          ...emailJob,
          templateId: effectiveTemplateId,
        };
        const result = await BrevoEmailService.sendEmail(
          jobWithLatestTemplate,
          lead,
        );

        // Update job status to sent
        await prisma.emailJob.update({
          where: { id: parsedJobId },
          data: {
            status: "sent",
            sentAt: new Date(),
            brevoMessageId: result.messageId,
            metadata: { ...emailJob.metadata, sentVia: "worker" },
          },
        });

        // If this is a manual email, update ManualMail status directly
        if (emailJob.type === "manual" || emailJob.metadata?.manual) {
          await prisma.manualMail.updateMany({
            where: { emailJobId: parsedJobId },
            data: { status: "sent" },
          });
          log.debug(
            { jobId: parsedJobId },
            "Updated ManualMail status to sent",
          );
        }

        // Update lead counters
        await LeadRepository.incrementCounter(parsedLeadId, "emailsSent", 1);

        // Use proper status format for conditional emails
        // Job type is 'conditional:Name' but lead status should be 'condition {trigger}:sent'
        let newLeadStatus = `${emailType}:sent`;
        if (
          emailJob.type.startsWith("conditional:") &&
          emailJob.metadata?.triggerEvent
        ) {
          newLeadStatus = RulebookService.formatConditionalStatus(
            emailJob.metadata.triggerEvent,
            "sent",
          );
        }
        await LeadRepository.updateStatus(parsedLeadId, newLeadStatus);

        // Add event to lead history
        await LeadRepository.addEvent(
          parsedLeadId,
          "sent",
          {
            messageId: result.messageId,
            jobId: parsedJobId,
          },
          emailType,
          parsedJobId,
        );

        return { status: "sent", messageId: result.messageId };
      } catch (error) {
        log.error(
          { jobId: parsedJobId, error: error.message, stack: error.stack },
          "Error processing email job",
        );

        // Update job with error - wrap in try-catch to handle non-existent records
        try {
          await prisma.emailJob.update({
            where: { id: parsedJobId },
            data: {
              status: "failed",
              failedAt: new Date(),
              lastError: error.message,
            },
          });
        } catch (updateError) {
          // Job record doesn't exist - this can happen if job was deleted or ID is invalid
          log.error(
            { jobId: parsedJobId, error: updateError.message },
            "Could not update job status",
          );
        }

        throw error;
      }
    },
    {
      connection: redisConnection,
      concurrency: 5,
      // Rate limiter: Max 10 jobs per second to prevent email service overload
      limiter: {
        max: 10,
        duration: 1000,
      },
      removeOnComplete: { count: 1000 },
      removeOnFail: { count: 5000 },
    },
  );

  worker.on("completed", (job, result) => {
    log.debug({ bullJobId: job.id, result }, "Job completed");
  });

  worker.on("failed", (job, error) => {
    log.error({ bullJobId: job?.id, error: error.message }, "Job failed");
  });

  log.info("Email worker started");
  return worker;
};

const emailWorker = {
  start: startWorker,
  close: async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
  },
  isRunning: () => !!worker && !worker.closing
};

// Auto-start
startWorker();

module.exports = emailWorker;
