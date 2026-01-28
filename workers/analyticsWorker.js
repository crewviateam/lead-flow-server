// workers/analyticsWorker.js
// Analytics event processing worker using BullMQ
// UPDATED: Added rate limiter for production scalability

const { Worker } = require('bullmq');
const redisConnection = require('../config/redis');
const AnalyticsService = require('../services/AnalyticsService');

let worker = null;

const startAnalyticsWorker = () => {
  if (worker) return worker;

  worker = new Worker(
    'analytics-queue',
    async (job) => {
      const { eventType, eventData } = job.data;

      console.log(`[AnalyticsWorker] Processing event: ${eventType}`);

      await AnalyticsService.processWebhookEvent(eventType, eventData);

      return { status: 'processed', eventType };
    },
    {
      connection: redisConnection,
      concurrency: 2,
      // Rate limiter: Max 10 jobs per second
      limiter: {
        max: 10,
        duration: 1000
      },
      removeOnComplete: { count: 500 },
      removeOnFail: { count: 2000 }
    }
  );

  worker.on('completed', (job, result) => {
    console.log(`[AnalyticsWorker] Job ${job.id} completed:`, result);
  });

  worker.on('failed', (job, error) => {
    console.error(`[AnalyticsWorker] Job ${job?.id} failed:`, error.message);
  });

  console.log('✅ Analytics worker started with rate limiting');
  return worker;
};

const analyticsWorker = {
  start: startAnalyticsWorker,
  close: async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
  },
  isRunning: () => !!worker && !worker.closing
};

// Auto-start
startAnalyticsWorker();

module.exports = analyticsWorker;