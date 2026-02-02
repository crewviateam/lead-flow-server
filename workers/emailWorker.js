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

let worker = null;

const startWorker = () => {
  if (worker) return worker;

  worker = new Worker(
    'email-send-queue',
    async (job) => {
      const { emailJobId, leadId, leadEmail, emailType } = job.data;
      const parsedJobId = parseInt(emailJobId);
      const parsedLeadId = parseInt(leadId);

      try {
        // Get the email job
        const emailJob = await EmailJobRepository.findById(parsedJobId);
        if (!emailJob) {
          // Return early instead of throwing - no point retrying if job was deleted
          console.log(
            `[EmailWorker] Job ${parsedJobId} not found in database, skipping (may have been deleted)`,
          );
          return { status: "skipped", reason: "Job not found in database" };
        }

        // Skip if already processed (including opened/clicked which are terminal)
        const processedStatuses = RulebookService.getProcessedStatuses();
        if (processedStatuses.includes(emailJob.status)) {
          console.log(
            `[EmailWorker] DUPLICATE PREVENTED: Job ${parsedJobId} already has status ${emailJob.status}`,
          );
          return { status: "skipped", reason: `Already ${emailJob.status}` };
        }

        // RACE CONDITION PREVENTION: Check again right before processing
        // This catches cases where another worker processed the same job
        const freshJob = await prisma.emailJob.findUnique({
          where: { id: parsedJobId },
        });
        if (freshJob && processedStatuses.includes(freshJob.status)) {
          console.log(
            `[EmailWorker] DUPLICATE PREVENTED (race): Job ${parsedJobId} status changed to ${freshJob.status}`,
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
          console.log(
            `[EmailWorker] DUPLICATE BLOCKED: ${emailJob.type} already sent to lead ${parsedLeadId}`,
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
        // TEMPLATE SELECTION (Simplified)
        // Conditions are now handled by ConditionalEmailService
        // Followups use their assigned templateId directly
        // ============================================
        const effectiveTemplateId = emailJob.templateId;

        // ============================================
        // CRITICAL: ATOMIC SEND ATTEMPT MARKING
        // Use atomic updateMany to claim this job before sending
        // This prevents race conditions with concurrency=5 workers
        // Only ONE worker can successfully mark the send attempt
        // ============================================
        const sendAttemptMarked =
          await UniqueJourneyService.markSendAttempt(parsedJobId);
        if (!sendAttemptMarked) {
          console.log(
            `[EmailWorker] Job ${parsedJobId} already being processed by another worker, skipping`,
          );
          return {
            status: "skipped",
            reason: "Already being processed by another worker",
          };
        }

        // Send the email via Brevo
        console.log(
          `[EmailWorker] Sending email job ${parsedJobId}, type: ${emailJob.type}, templateId: ${emailJob.templateId || "none"}`,
        );
        const result = await BrevoEmailService.sendEmail(emailJob, lead);

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
          console.log(
            `[EmailWorker] Updated ManualMail status to 'sent' for jobId ${parsedJobId}`,
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
        console.error(
          `[EmailWorker] Error processing job ${parsedJobId}:`,
          error,
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
          console.error(
            `[EmailWorker] Could not update job ${parsedJobId} status:`,
            updateError.message,
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
        duration: 1000
      },
      removeOnComplete: { count: 1000 },
      removeOnFail: { count: 5000 }
    }
  );

  worker.on('completed', (job, result) => {
    console.log(`[EmailWorker] Job ${job.id} completed:`, result);
  });

  worker.on('failed', (job, error) => {
    console.error(`[EmailWorker] Job ${job?.id} failed:`, error.message);
  });

  console.log('✅ Email worker started');
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
