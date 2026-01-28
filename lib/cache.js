// lib/cache.js
// Centralized Redis caching layer for performance optimization

const redisConnection = require('../config/redis');

const CACHE_TTL = {
  ANALYTICS: 300,      // 5 minutes
  SETTINGS: 3600,      // 1 hour
  DASHBOARD: 60,       // 1 minute
  RATE_LIMIT: 900,     // 15 minutes (window duration)
};

class CacheService {
  constructor(redis) {
    this.redis = redis;
    this.prefix = 'leadflow:';
  }

  _key(namespace, id = '') {
    return `${this.prefix}${namespace}:${id}`;
  }

  // Generic get/set with TTL
  async get(namespace, id = '') {
    const key = this._key(namespace, id);
    const data = await this.redis.get(key);
    return data ? JSON.parse(data) : null;
  }

  async set(namespace, id, data, ttl = 300) {
    const key = this._key(namespace, id);
    await this.redis.setex(key, ttl, JSON.stringify(data));
  }

  async del(namespace, id = '') {
    const key = this._key(namespace, id);
    await this.redis.del(key);
  }

  async delPattern(pattern) {
    const keys = await this.redis.keys(`${this.prefix}${pattern}`);
    if (keys.length > 0) {
      await this.redis.del(...keys);
    }
  }

  // ============================================
  // Analytics Cache
  // ============================================
  async getAnalytics(period) {
    return this.get('analytics', period);
  }

  async setAnalytics(period, data) {
    return this.set('analytics', period, data, CACHE_TTL.ANALYTICS);
  }

  async invalidateAnalytics() {
    return this.delPattern('analytics:*');
  }

  // ============================================
  // Settings Cache
  // ============================================
  async getSettings() {
    return this.get('settings', 'global');
  }

  async setSettings(data) {
    return this.set('settings', 'global', data, CACHE_TTL.SETTINGS);
  }

  async invalidateSettings() {
    return this.del('settings', 'global');
  }

  // ============================================
  // Rate Limiting (Sliding Window)
  // ============================================
  async getRateLimit(timezone, windowStart) {
    const key = this._key('ratelimit', `${timezone}:${windowStart}`);
    const count = await this.redis.get(key);
    return count ? parseInt(count, 10) : 0;
  }

  async incrementRateLimit(timezone, windowStart, ttl = CACHE_TTL.RATE_LIMIT) {
    const key = this._key('ratelimit', `${timezone}:${windowStart}`);
    const pipeline = this.redis.pipeline();
    pipeline.incr(key);
    pipeline.expire(key, ttl);
    const results = await pipeline.exec();
    return results[0][1]; // Returns new count
  }

  // ============================================
  // Dashboard Cache
  // ============================================
  async getDashboard(dateRange) {
    return this.get('dashboard', dateRange);
  }

  async setDashboard(dateRange, data) {
    return this.set('dashboard', dateRange, data, CACHE_TTL.DASHBOARD);
  }

  async invalidateDashboard() {
    return this.delPattern('dashboard:*');
  }

  // ============================================
  // Slot Availability Cache
  // ============================================
  async getSlotAvailability(timezone, date) {
    return this.get('slots', `${timezone}:${date}`);
  }

  async setSlotAvailability(timezone, date, slots, ttl = 60) {
    return this.set('slots', `${timezone}:${date}`, slots, ttl);
  }

  async invalidateSlots(timezone, date) {
    if (date) {
      return this.del('slots', `${timezone}:${date}`);
    }
    return this.delPattern(`slots:${timezone}:*`);
  }
}

const cache = new CacheService(redisConnection);

module.exports = { cache, CACHE_TTL };
