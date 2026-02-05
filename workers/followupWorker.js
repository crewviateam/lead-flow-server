// workers/followupWorker.js
// Followup scheduling worker using BullMQ
// UPDATED: Added rate limiter for production scalability

const { Worker } = require('bullmq');
const redisConnection = require('../config/redis');
const EmailSchedulerService = require('../services/EmailSchedulerService');
const { loggers } = require("../lib/logger");
const log = loggers.worker;

let worker = null;

const startFollowupWorker = () => {
  if (worker) return worker;

  worker = new Worker(
    "followup-queue",
    async (job) => {
      const { leadId, originalEmailJobId } = job.data;

      log.info({ leadId }, "Processing followup for lead");

      // Schedule follow-up email using the same scheduler logic
      // 'originalEmailJobId' is not strictly needed for scheduleNextEmail as it inspects the DB,
      // but we log it for context if needed.
      await EmailSchedulerService.scheduleNextEmail(leadId);

      return { status: "followup_scheduled", leadId };
    },
    {
      connection: redisConnection,
      concurrency: 3,
      // Rate limiter: Max 5 jobs per second to prevent overload
      limiter: {
        max: 5,
        duration: 1000,
      },
      removeOnComplete: { count: 1000 },
      removeOnFail: { count: 5000 },
    },
  );

  worker.on("completed", (job, result) => {
    log.debug({ bullJobId: job.id, result }, "Followup job completed");
  });

  worker.on("failed", (job, error) => {
    log.error(
      { bullJobId: job?.id, error: error.message },
      "Followup job failed",
    );
  });

  log.info("Followup worker started with rate limiting");
  return worker;
};

const followupWorker = {
  start: startFollowupWorker,
  close: async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
  },
  isRunning: () => !!worker && !worker.closing
};

// Auto-start
startFollowupWorker();

module.exports = followupWorker;