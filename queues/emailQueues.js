// queues/emailQueues.js
const { Queue } = require('bullmq');
const redisConnection = require('../config/redis');

// Email send queue
const emailSendQueue = new Queue('email-send-queue', {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 60000 // Start with 1 minute
    },
    removeOnComplete: {
      count: 1000, // Keep last 1000 successful jobs
      age: 24 * 3600 // 24 hours
    },
    removeOnFail: {
      count: 5000 // Keep last 5000 failed jobs
    }
  }
});

// Follow-up email queue
const followupQueue = new Queue('followup-queue', {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 60000
    },
    removeOnComplete: {
      count: 1000,
      age: 24 * 3600
    },
    removeOnFail: {
      count: 5000
    }
  }
});

// Analytics processing queue
const analyticsQueue = new Queue('analytics-queue', {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 2,
    backoff: {
      type: 'exponential',
      delay: 30000
    },
    removeOnComplete: {
      count: 500,
      age: 24 * 3600
    }
  }
});

module.exports = {
  emailSendQueue,
  followupQueue,
  analyticsQueue
};