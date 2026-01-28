// lib/ingest.js
// High-performance data ingestion layer with caching, batching, and deduplication
// Optimizes webhook processing and bulk operations

const { cache } = require('./cache');
const { prisma } = require('./prisma');

class IngestService {
  constructor() {
    // Deduplication window (prevents processing same event twice)
    this.dedupWindow = 60 * 1000; // 60 seconds
    this.dedupCache = new Map();
    
    // Batch processing configuration
    this.batchSize = 50;
    this.batchTimeout = 1000; // 1 second
    
    // Pending batches for different entity types
    this.pendingBatches = {
      webhookEvents: [],
      leadUpdates: [],
      jobUpdates: []
    };
    
    // Batch timers
    this.batchTimers = {};
    
    // Query cache TTLs
    this.cacheTTL = {
      lead: 300,        // 5 min for individual leads
      leadList: 60,     // 1 min for lead lists
      job: 120,         // 2 min for jobs
      settings: 600,    // 10 min for settings
      analytics: 300    // 5 min for analytics
    };
    
    // Start cleanup interval
    this._startCleanup();
  }

  // ==========================================
  // DEDUPLICATION
  // ==========================================
  
  /**
   * Check if event has been recently processed
   * @param {string} eventKey - Unique key for the event
   * @returns {boolean} - True if duplicate
   */
  isDuplicate(eventKey) {
    const lastProcessed = this.dedupCache.get(eventKey);
    if (lastProcessed && Date.now() - lastProcessed < this.dedupWindow) {
      return true;
    }
    return false;
  }
  
  /**
   * Mark event as processed
   * @param {string} eventKey - Unique key for the event
   */
  markProcessed(eventKey) {
    this.dedupCache.set(eventKey, Date.now());
  }
  
  /**
   * Generate dedup key for webhook event
   */
  getWebhookEventKey(messageId, eventType) {
    return `webhook:${messageId}:${eventType}`;
  }

  // ==========================================
  // CACHED QUERIES
  // ==========================================
  
  /**
   * Get lead with caching
   * Avoids repeated database queries for the same lead
   */
  async getLead(leadId, forceRefresh = false) {
    const cacheKey = `lead:${leadId}`;
    
    if (!forceRefresh) {
      const cached = await cache.get('ingest', cacheKey);
      if (cached) {
        return cached;
      }
    }
    
    const lead = await prisma.lead.findUnique({
      where: { id: parseInt(leadId) },
      include: { emailSchedule: true }
    });
    
    if (lead) {
      await cache.set('ingest', cacheKey, lead, this.cacheTTL.lead);
    }
    
    return lead;
  }
  
  /**
   * Get email job with caching
   */
  async getEmailJob(jobId, forceRefresh = false) {
    const cacheKey = `job:${jobId}`;
    
    if (!forceRefresh) {
      const cached = await cache.get('ingest', cacheKey);
      if (cached) {
        return cached;
      }
    }
    
    const job = await prisma.emailJob.findUnique({
      where: { id: parseInt(jobId) }
    });
    
    if (job) {
      await cache.set('ingest', cacheKey, job, this.cacheTTL.job);
    }
    
    return job;
  }
  
  /**
   * Find job by message ID with caching
   */
  async getJobByMessageId(messageId) {
    const cacheKey = `job:msgid:${messageId}`;
    
    const cached = await cache.get('ingest', cacheKey);
    if (cached) {
      return cached;
    }
    
    const job = await prisma.emailJob.findFirst({
      where: { messageId }
    });
    
    if (job) {
      // Cache by message ID
      await cache.set('ingest', cacheKey, job, this.cacheTTL.job);
      // Also cache by job ID for subsequent lookups
      await cache.set('ingest', `job:${job.id}`, job, this.cacheTTL.job);
    }
    
    return job;
  }
  
  /**
   * Invalidate lead cache (call after updates)
   */
  async invalidateLeadCache(leadId) {
    await cache.del('ingest', `lead:${leadId}`);
  }
  
  /**
   * Invalidate job cache (call after updates)
   */
  async invalidateJobCache(jobId, messageId = null) {
    await cache.del('ingest', `job:${jobId}`);
    if (messageId) {
      await cache.del('ingest', `job:msgid:${messageId}`);
    }
  }

  // ==========================================
  // BATCH PROCESSING
  // ==========================================
  
  /**
   * Add item to batch for processing
   * @param {string} batchType - Type of batch (webhookEvents, leadUpdates, etc.)
   * @param {Object} item - Item to add
   * @param {Function} processorFn - Function to process the batch
   */
  addToBatch(batchType, item, processorFn) {
    if (!this.pendingBatches[batchType]) {
      this.pendingBatches[batchType] = [];
    }
    
    this.pendingBatches[batchType].push(item);
    
    // Process immediately if batch is full
    if (this.pendingBatches[batchType].length >= this.batchSize) {
      this._processBatch(batchType, processorFn);
      return;
    }
    
    // Set timer to process batch after timeout
    if (!this.batchTimers[batchType]) {
      this.batchTimers[batchType] = setTimeout(() => {
        this._processBatch(batchType, processorFn);
      }, this.batchTimeout);
    }
  }
  
  /**
   * Process a batch
   * @private
   */
  async _processBatch(batchType, processorFn) {
    // Clear timer
    if (this.batchTimers[batchType]) {
      clearTimeout(this.batchTimers[batchType]);
      delete this.batchTimers[batchType];
    }
    
    // Get pending items
    const items = this.pendingBatches[batchType];
    this.pendingBatches[batchType] = [];
    
    if (items.length === 0) return;
    
    try {
      await processorFn(items);
      console.log(`[Ingest] Processed batch of ${items.length} ${batchType}`);
    } catch (error) {
      console.error(`[Ingest] Batch processing error for ${batchType}:`, error);
      // Re-add failed items to retry (with retry limit tracking if needed)
    }
  }

  // ==========================================
  // PARALLEL PROCESSING
  // ==========================================
  
  /**
   * Process items in parallel with concurrency limit
   * @param {Array} items - Items to process
   * @param {Function} processFn - Async function to process each item
   * @param {number} concurrency - Max concurrent operations
   */
  async parallelProcess(items, processFn, concurrency = 5) {
    const results = [];
    const executing = new Set();
    
    for (const item of items) {
      const promise = processFn(item).then(result => {
        executing.delete(promise);
        return result;
      }).catch(error => {
        executing.delete(promise);
        return { error, item };
      });
      
      executing.add(promise);
      results.push(promise);
      
      if (executing.size >= concurrency) {
        await Promise.race(executing);
      }
    }
    
    return Promise.all(results);
  }

  // ==========================================
  // WEBHOOK INGESTION
  // ==========================================
  
  /**
   * Ingest webhook event with deduplication and caching
   * @param {Object} webhookEvent - The webhook event data
   * @returns {Object} - { processed: boolean, job: EmailJob|null, reason?: string }
   */
  async ingestWebhookEvent(webhookEvent) {
    const { messageId, event: eventType, email } = webhookEvent;
    
    // Step 1: Check for duplicate
    const dedupKey = this.getWebhookEventKey(messageId, eventType);
    if (this.isDuplicate(dedupKey)) {
      return { processed: false, job: null, reason: 'duplicate' };
    }
    
    // Step 2: Find job by messageId (cached)
    const job = await this.getJobByMessageId(messageId);
    if (!job) {
      return { processed: false, job: null, reason: 'job_not_found' };
    }
    
    // Step 3: Get lead (cached)
    const lead = await this.getLead(job.leadId);
    if (!lead) {
      return { processed: false, job: null, reason: 'lead_not_found' };
    }
    
    // Step 4: Mark as processed
    this.markProcessed(dedupKey);
    
    // Step 5: Invalidate caches (job status will change)
    await this.invalidateJobCache(job.id, messageId);
    
    return { 
      processed: true, 
      job, 
      lead,
      reason: 'success' 
    };
  }

  // ==========================================
  // CLEANUP
  // ==========================================
  
  /**
   * Start periodic cleanup of dedup cache
   * @private
   */
  _startCleanup() {
    setInterval(() => {
      const now = Date.now();
      let cleaned = 0;
      
      for (const [key, timestamp] of this.dedupCache.entries()) {
        if (now - timestamp > this.dedupWindow * 2) {
          this.dedupCache.delete(key);
          cleaned++;
        }
      }
      
      if (cleaned > 0) {
        console.log(`[Ingest] Cleaned ${cleaned} expired dedup entries`);
      }
    }, 60000); // Every minute
  }

  /**
   * Get stats for monitoring
   */
  getStats() {
    return {
      dedupCacheSize: this.dedupCache.size,
      pendingBatches: {
        webhookEvents: this.pendingBatches.webhookEvents?.length || 0,
        leadUpdates: this.pendingBatches.leadUpdates?.length || 0,
        jobUpdates: this.pendingBatches.jobUpdates?.length || 0
      }
    };
  }
}

// Singleton instance
const ingestService = new IngestService();

module.exports = ingestService;
