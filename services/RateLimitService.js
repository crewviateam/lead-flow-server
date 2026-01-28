// services/RateLimitService.js
// Rate limit service using Prisma

const redisConnection = require('../config/redis');
const { SettingsRepository } = require('../repositories');
const RulebookService = require('./RulebookService');

class RateLimitService {
  constructor() {
    this.redis = redisConnection;
    // Cache settings briefly to avoid hammering DB
    this.cachedSettings = null;
    this.lastCacheTime = 0;
    this.CACHE_TTL = 60 * 1000; // 1 minute
  }

  async getLimits() {
    const now = Date.now();
    if (this.cachedSettings && (now - this.lastCacheTime < this.CACHE_TTL)) {
      return this.cachedSettings;
    }

    try {
      const settings = await SettingsRepository.getSettings();
      const limits = {
        maxEmails: settings.rateLimit?.emailsPerWindow || 2,
        windowMs: (settings.rateLimit?.windowMinutes || 15) * 60 * 1000
      };
      
      this.cachedSettings = limits;
      this.lastCacheTime = now;
      return limits;
    } catch (error) {
      console.error('Error fetching rate limits:', error);
      // Fallback defaults
      return { maxEmails: 2, windowMs: 15 * 60 * 1000 };
    }
  }

  getRateLimitKey(timezone, windowStart) {
    // Global Rate Limiting: Ignoring timezone to enforce single queue
    return `ratelimit:global:${windowStart}`;
  }

  async getWindowStart(timestamp) {
    const limits = await this.getLimits();
    return Math.floor(timestamp / limits.windowMs) * limits.windowMs;
  }

  async reserveSlot(timezone, targetTime) {
    const timestamp = targetTime.getTime();
    const limits = await this.getLimits();
    
    // Use smaller windows (e.g., 5 min) if high volume, but stick to config for now
    const windowStart = Math.floor(timestamp / limits.windowMs) * limits.windowMs;
    const windowEnd = windowStart + limits.windowMs;
    const key = this.getRateLimitKey(timezone, windowStart);

    // 1. CHECK DATABASE FIRST (Source of Truth)
    // Count ALL active jobs scheduled in this window
    // We ignore 'cancelled' and 'failed' as they don't consume sending capacity
    const { prisma } = require('../lib/prisma');
    const dbCount = await prisma.emailJob.count({
      where: {
        scheduledFor: {
          gte: new Date(windowStart),
          lt: new Date(windowEnd)
        },
        status: { 
          in: RulebookService.getInProgressStatuses() 
        }
      }
    });

    if (dbCount >= limits.maxEmails) {
      console.log(`[RateLimit] Slot full (DB): ${dbCount}/${limits.maxEmails} at ${new Date(windowStart).toLocaleTimeString()}`);
      
      // Update Redis to match reality so subsequent checks are fast
      await this.redis.set(key, dbCount);
      await this.redis.expire(key, Math.ceil(limits.windowMs / 1000) * 2);
      
      return {
        success: false,
        nextWindow: new Date(windowEnd)
      };
    }

    // 2. INCREMENT REDIS (Concurrency Guard)
    // We use Redis `incr` to handle race conditions between parallel processes
    // If Redis is empty/expired, we initialize it with current DB count
    const redisCount = await this.redis.get(key);
    
    if (!redisCount) {
      // Initialize if missing
      await this.redis.set(key, dbCount); 
    }
    
    const newCount = await this.redis.incr(key);

    if (newCount === 1) {
      await this.redis.expire(key, Math.ceil(limits.windowMs / 1000) * 2);
    }

    // Double check: if Redis says full (even if DB said available moments ago), trust Redis (safer)
    if (newCount > limits.maxEmails) {
      console.log(`[RateLimit] Slot full (Redis): ${newCount}/${limits.maxEmails}`);
      return {
        success: false,
        nextWindow: new Date(windowEnd)
      };
    }

    return {
      success: true,
      reservedTime: targetTime
    };
  }

  async getSlotCapacity(timestamp) {
    const limits = await this.getLimits();
    const windowStart = Math.floor(timestamp / limits.windowMs) * limits.windowMs;
    const windowEnd = windowStart + limits.windowMs;
    
    // Always check DB for visualization accuracy
    const { prisma } = require('../lib/prisma');
    const dbCount = await prisma.emailJob.count({
      where: {
        scheduledFor: {
          gte: new Date(windowStart),
          lt: new Date(windowEnd)
        },
        status: { 
          in: RulebookService.getInProgressStatuses() 
        }
      }
    });
    
    // Sync Redis for consistency
    const key = this.getRateLimitKey(null, windowStart);
    await this.redis.set(key, dbCount);
    await this.redis.expire(key, Math.ceil(limits.windowMs / 1000) * 2);
    
    return {
      used: dbCount,
      total: limits.maxEmails,
      available: Math.max(0, limits.maxEmails - dbCount),
      windowStart: new Date(windowStart)
    };
  }
}

module.exports = new RateLimitService();