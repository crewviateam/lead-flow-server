// workers/followupWorker.js
// Followup scheduling worker using BullMQ
// UPDATED: Added rate limiter for production scalability

const { Worker } = require('bullmq');
const redisConnection = require('../config/redis');
const EmailSchedulerService = require('../services/EmailSchedulerService');

let worker = null;

const startFollowupWorker = () => {
  if (worker) return worker;

  worker = new Worker(
    'followup-queue',
    async (job) => {
      const { leadId, originalEmailJobId } = job.data;

      console.log(`[FollowupWorker] Processing followup for lead ${leadId}`);

      // Schedule follow-up email using the same scheduler logic
      // 'originalEmailJobId' is not strictly needed for scheduleNextEmail as it inspects the DB, 
      // but we log it for context if needed.
      await EmailSchedulerService.scheduleNextEmail(leadId);

      return { status: 'followup_scheduled', leadId };
    },
    {
      connection: redisConnection,
      concurrency: 3,
      // Rate limiter: Max 5 jobs per second to prevent overload
      limiter: {
        max: 5,
        duration: 1000
      },
      removeOnComplete: { count: 1000 },
      removeOnFail: { count: 5000 }
    }
  );

  worker.on('completed', (job, result) => {
    console.log(`[FollowupWorker] Job ${job.id} completed:`, result);
  });

  worker.on('failed', (job, error) => {
    console.error(`[FollowupWorker] Job ${job?.id} failed:`, error.message);
  });

  console.log('✅ Followup worker started with rate limiting');
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