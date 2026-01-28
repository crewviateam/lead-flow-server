// middleware/rateLimiter.js
// API Rate Limiting middleware using Redis store
// Protects against abuse and ensures fair usage

const rateLimit = require('express-rate-limit');
const RedisStore = require('rate-limit-redis').default;
const redisConnection = require('../config/redis');

/**
 * Standard API rate limiter
 * - 100 requests per minute per IP
 * - Uses Redis for distributed state (works across multiple server instances)
 */
const apiLimiter = rateLimit({
  store: new RedisStore({
    sendCommand: (...args) => redisConnection.call(...args),
    prefix: 'leadflow:rl:'
  }),
  windowMs: 60 * 1000,  // 1 minute window
  max: 100,             // 100 requests per window
  standardHeaders: true,
  legacyHeaders: false,
  message: { 
    error: 'Too many requests, please slow down',
    retryAfter: 60 
  },
  skip: (req) => {
    // Skip rate limiting for health checks and webhooks
    return req.path === '/health' || req.path.startsWith('/api/webhooks');
  }
});

/**
 * Strict rate limiter for sensitive operations
 * - 10 requests per minute per IP
 * - Used for: bulk imports, exports, heavy analytics
 */
const strictLimiter = rateLimit({
  store: new RedisStore({
    sendCommand: (...args) => redisConnection.call(...args),
    prefix: 'leadflow:rl:strict:'
  }),
  windowMs: 60 * 1000,  // 1 minute window
  max: 10,              // 10 requests per window
  standardHeaders: true,
  legacyHeaders: false,
  message: { 
    error: 'Rate limit exceeded for this operation',
    retryAfter: 60 
  }
});

/**
 * Upload rate limiter
 * - 5 uploads per 5 minutes per IP
 */
const uploadLimiter = rateLimit({
  store: new RedisStore({
    sendCommand: (...args) => redisConnection.call(...args),
    prefix: 'leadflow:rl:upload:'
  }),
  windowMs: 5 * 60 * 1000,  // 5 minute window
  max: 5,                    // 5 uploads per window
  standardHeaders: true,
  legacyHeaders: false,
  message: { 
    error: 'Too many file uploads, please wait',
    retryAfter: 300 
  }
});

module.exports = {
  apiLimiter,
  strictLimiter,
  uploadLimiter
};
