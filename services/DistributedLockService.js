// services/DistributedLockService.js
// Redis-based distributed locking for multi-instance scalability
// Replaces in-memory locks to work across multiple server instances

const redisConnection = require('../config/redis');

const DEFAULT_LOCK_TTL_MS = 30000; // 30 seconds
const LOCK_PREFIX = 'leadflow:lock:';

class DistributedLockService {
  
  /**
   * Acquire a distributed lock
   * @param {string} lockKey - Unique key identifying the resource to lock
   * @param {number} ttlMs - Lock TTL in milliseconds (auto-expires to prevent deadlocks)
   * @returns {Promise<{acquired: boolean, lockId: string|null}>}
   */
  async acquire(lockKey, ttlMs = DEFAULT_LOCK_TTL_MS) {
    const fullKey = `${LOCK_PREFIX}${lockKey}`;
    const lockId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    
    try {
      // SET key value NX PX ttl
      // NX = Only set if key doesn't exist
      // PX = Set expiry in milliseconds
      const result = await redisConnection.set(fullKey, lockId, 'NX', 'PX', ttlMs);
      
      if (result === 'OK') {
        console.log(`[DistributedLock] Acquired lock: ${lockKey}`);
        return { acquired: true, lockId };
      }
      
      console.log(`[DistributedLock] Lock already held: ${lockKey}`);
      return { acquired: false, lockId: null };
      
    } catch (error) {
      console.error(`[DistributedLock] Error acquiring lock ${lockKey}:`, error.message);
      return { acquired: false, lockId: null };
    }
  }
  
  /**
   * Release a distributed lock
   * Uses Lua script to ensure we only delete our own lock (prevents accidental release of another process's lock)
   * @param {string} lockKey - The lock key
   * @param {string} lockId - The lock ID returned from acquire()
   * @returns {Promise<boolean>}
   */
  async release(lockKey, lockId) {
    const fullKey = `${LOCK_PREFIX}${lockKey}`;
    
    // Lua script: Only delete if value matches (atomic operation)
    const luaScript = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end
    `;
    
    try {
      const result = await redisConnection.eval(luaScript, 1, fullKey, lockId);
      
      if (result === 1) {
        console.log(`[DistributedLock] Released lock: ${lockKey}`);
        return true;
      }
      
      console.log(`[DistributedLock] Lock not released (expired or not owned): ${lockKey}`);
      return false;
      
    } catch (error) {
      console.error(`[DistributedLock] Error releasing lock ${lockKey}:`, error.message);
      return false;
    }
  }
  
  /**
   * Extend lock TTL (useful for long-running operations)
   * @param {string} lockKey 
   * @param {string} lockId 
   * @param {number} additionalTtlMs 
   * @returns {Promise<boolean>}
   */
  async extend(lockKey, lockId, additionalTtlMs = DEFAULT_LOCK_TTL_MS) {
    const fullKey = `${LOCK_PREFIX}${lockKey}`;
    
    // Lua script: Only extend if we own the lock
    const luaScript = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("pexpire", KEYS[1], ARGV[2])
      else
        return 0
      end
    `;
    
    try {
      const result = await redisConnection.eval(luaScript, 1, fullKey, lockId, additionalTtlMs);
      return result === 1;
    } catch (error) {
      console.error(`[DistributedLock] Error extending lock ${lockKey}:`, error.message);
      return false;
    }
  }
  
  /**
   * Check if a lock is currently held
   * @param {string} lockKey 
   * @returns {Promise<boolean>}
   */
  async isLocked(lockKey) {
    const fullKey = `${LOCK_PREFIX}${lockKey}`;
    
    try {
      const result = await redisConnection.exists(fullKey);
      return result === 1;
    } catch (error) {
      console.error(`[DistributedLock] Error checking lock ${lockKey}:`, error.message);
      return false;
    }
  }
  
  /**
   * Execute a function while holding a lock
   * Automatically acquires and releases lock, with retry logic
   * @param {string} lockKey - The resource to lock
   * @param {Function} fn - Async function to execute
   * @param {Object} options - { ttlMs, maxRetries, retryDelayMs }
   * @returns {Promise<{success: boolean, result: any, error: Error|null}>}
   */
  async withLock(lockKey, fn, options = {}) {
    const {
      ttlMs = DEFAULT_LOCK_TTL_MS,
      maxRetries = 3,
      retryDelayMs = 100
    } = options;
    
    let lockId = null;
    let attempts = 0;
    
    // Try to acquire lock with retries
    while (attempts < maxRetries) {
      attempts++;
      const lockResult = await this.acquire(lockKey, ttlMs);
      
      if (lockResult.acquired) {
        lockId = lockResult.lockId;
        break;
      }
      
      if (attempts < maxRetries) {
        await this._sleep(retryDelayMs * attempts); // Exponential backoff
      }
    }
    
    if (!lockId) {
      return {
        success: false,
        result: null,
        error: new Error(`Failed to acquire lock after ${maxRetries} attempts`)
      };
    }
    
    try {
      const result = await fn();
      return { success: true, result, error: null };
    } catch (error) {
      return { success: false, result: null, error };
    } finally {
      await this.release(lockKey, lockId);
    }
  }
  
  /**
   * Generate a standard lock key for scheduling operations
   * @param {number} leadId 
   * @param {string} emailType 
   * @returns {string}
   */
  getSchedulingLockKey(leadId, emailType) {
    return `schedule:${leadId}:${emailType}`;
  }
  
  /**
   * Generate a lock key for lead-level operations
   * @param {number} leadId 
   * @returns {string}
   */
  getLeadLockKey(leadId) {
    return `lead:${leadId}`;
  }
  
  /**
   * Sleep helper
   */
  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = new DistributedLockService();
