// controllers/RateLimitController.js
// Rate limit controller using Prisma

const RateLimitService = require('../services/RateLimitService');
const { prisma } = require('../lib/prisma');
const redisConnection = require('../config/redis');

class RateLimitController {
  // Get current rate limit status for all active timezones
  async getRateLimitStatus(req, res) {
    try {
      // Get unique timezones with pending/queued jobs using raw SQL
      const result = await prisma.$queryRaw`
        SELECT DISTINCT metadata->>'timezone' as timezone
        FROM email_jobs
        WHERE status IN ('pending', 'queued')
          AND metadata->>'timezone' IS NOT NULL
      `;
      
      const activeTimezones = result.map(r => r.timezone).filter(Boolean);

      const results = [];
      const now = Date.now();
      const limits = await RateLimitService.getLimits();
      const windowStart = await RateLimitService.getWindowStart(now);

      for (const timezone of activeTimezones) {
        if (!timezone) continue;
        
        const key = RateLimitService.getRateLimitKey(timezone, windowStart);
        const currentCount = await redisConnection.get(key);
        const count = currentCount ? parseInt(currentCount, 10) : 0;
        
        // Count pending jobs for this timezone
        const pendingCount = await prisma.emailJob.count({
          where: {
            status: 'pending',
            metadata: {
              path: ['timezone'],
              equals: timezone
            }
          }
        });

        results.push({
          timezone,
          currentWindow: {
            start: new Date(windowStart),
            end: new Date(windowStart + limits.windowMs),
            used: count,
            max: limits.maxEmails,
            remaining: Math.max(0, limits.maxEmails - count)
          },
          pendingJobs: pendingCount
        });
      }

      res.status(200).json({
        rateLimits: results,
        windowDuration: `${limits.windowMs / 60000} minutes`,
        maxPerWindow: limits.maxEmails
      });
    } catch (error) {
      console.error('Get rate limit status error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Get system configuration
  async getConfig(req, res) {
    try {
      const config = {
        rateLimit: {
          maxEmailsPerWindow: parseInt(process.env.MAX_EMAILS_PER_WINDOW) || 2,
          windowDurationMinutes: parseInt(process.env.WINDOW_DURATION_MINUTES) || 15
        },
        followUp: {
          delayDays: parseInt(process.env.FOLLOWUP_DELAY_DAYS) || 3
        },
        retry: {
          maxAttempts: 5,
          backoffType: 'exponential'
        }
      };

      res.status(200).json(config);
    } catch (error) {
      console.error('Get config error:', error);
      res.status(500).json({ error: error.message });
    }
  }
}

module.exports = new RateLimitController();
