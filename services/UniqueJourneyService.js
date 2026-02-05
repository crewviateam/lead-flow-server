// services/UniqueJourneyService.js
// Single Source of Truth for tracking unique email journeys and preventing duplicates
// This service provides atomic, thread-safe duplicate prevention across all scheduling paths
// UPDATED: Now uses Redis-based distributed locks for multi-instance scalability

const { prisma } = require('../lib/prisma');
const RulebookService = require('./RulebookService');
const DistributedLockService = require('./DistributedLockService');

const LOCK_TIMEOUT_MS = 30000; // 30 seconds

// Store lock IDs for release (key: leadId:emailType -> lockId)
const activeLocks = new Map();

class UniqueJourneyService {
  
  /**
   * Acquire a distributed lock for scheduling an email type to a lead
   * Prevents race conditions when multiple events trigger scheduling simultaneously
   * Works across multiple server instances via Redis
   * @param {number} leadId 
   * @param {string} emailType 
   * @returns {Promise<boolean>} - true if lock acquired, false if already locked
   */
  async acquireLock(leadId, emailType) {
    const lockKey = DistributedLockService.getSchedulingLockKey(leadId, emailType);
    const localKey = `${leadId}:${emailType}`;
    
    const result = await DistributedLockService.acquire(lockKey, LOCK_TIMEOUT_MS);
    
    if (result.acquired) {
      // Store lockId for later release
      activeLocks.set(localKey, result.lockId);
      console.log(`[UniqueJourney] Acquired distributed lock for ${localKey}`);
      return true;
    }
    
    console.log(`[UniqueJourney] Distributed lock exists for ${localKey}, cannot acquire`);
    return false;
  }
  
  /**
   * Release distributed lock after scheduling completes
   * @param {number} leadId 
   * @param {string} emailType 
   * @returns {Promise<void>}
   */
  async releaseLock(leadId, emailType) {
    const lockKey = DistributedLockService.getSchedulingLockKey(leadId, emailType);
    const localKey = `${leadId}:${emailType}`;
    const lockId = activeLocks.get(localKey);
    
    if (lockId) {
      await DistributedLockService.release(lockKey, lockId);
      activeLocks.delete(localKey);
      console.log(`[UniqueJourney] Released distributed lock for ${localKey}`);
    }
  }
  
  /**
   * Check if email type has already been successfully sent to lead
   * "Successfully sent" means status is in sent/delivered/opened/clicked
   * @param {number} leadId 
   * @param {string} emailType 
   * @returns {Promise<boolean>}
   */
  async hasBeenSent(leadId, emailType) {
    const sentStatuses = RulebookService.getSuccessfullySentStatuses();
    
    const existingJob = await prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        type: emailType,
        status: { in: sentStatuses }
      },
      select: { id: true, status: true }
    });
    
    if (existingJob) {
      console.log(`[UniqueJourney] ${emailType} already sent to lead ${leadId} (job ${existingJob.id}, status: ${existingJob.status})`);
      return true;
    }
    
    return false;
  }
  
  /**
   * Check if email type is currently pending/scheduled for lead
   * @param {number} leadId 
   * @param {string} emailType 
   * @returns {Promise<{isPending: boolean, job: Object|null}>}
   */
  async isPending(leadId, emailType) {
    const pendingStatuses = RulebookService.getActiveStatuses();
    
    const pendingJob = await prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        type: emailType,
        status: { in: pendingStatuses }
      },
      select: { id: true, status: true, scheduledFor: true }
    });
    
    return { 
      isPending: !!pendingJob, 
      job: pendingJob 
    };
  }
  
  /**
   * Check if any email is pending for this lead (regardless of type)
   * Used to prevent scheduling multiple emails simultaneously
   * @param {number} leadId 
   * @param {string[]} excludeTypes - Types to exclude from check (e.g., ['conditional:...'])
   * @returns {Promise<{hasPending: boolean, pendingType: string|null}>}
   */
  async hasAnyPending(leadId, excludeTypes = []) {
    const pendingStatuses = RulebookService.getActiveStatuses();
    
    const pendingJob = await prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        status: { in: pendingStatuses },
        type: {
          notIn: excludeTypes
        }
      },
      select: { id: true, type: true }
    });
    
    return {
      hasPending: !!pendingJob,
      pendingType: pendingJob?.type || null
    };
  }
  
  /**
   * MAIN ENTRY POINT: Validate if scheduling is allowed
   * Combines all checks into a single atomic validation
   * @param {number} leadId 
   * @param {string} emailType 
   * @returns {Promise<{allowed: boolean, reason: string|null}>}
   */
  async canSchedule(leadId, emailType) {
    // 1. Try to acquire distributed lock first
    const lockAcquired = await this.acquireLock(leadId, emailType);
    if (!lockAcquired) {
      return { 
        allowed: false, 
        reason: `Concurrent scheduling in progress for ${emailType}` 
      };
    }
    
    try {
      // 2. Check if already sent
      const alreadySent = await this.hasBeenSent(leadId, emailType);
      if (alreadySent) {
        return { 
          allowed: false, 
          reason: `${emailType} already sent to lead ${leadId}` 
        };
      }
      
      // 3. Check if already pending
      const { isPending, job } = await this.isPending(leadId, emailType);
      if (isPending) {
        return { 
          allowed: false, 
          reason: `${emailType} already pending (job ${job.id})` 
        };
      }
      
      // 4. All checks passed
      return { allowed: true, reason: null };
      
    } catch (error) {
      // Release lock on error
      await this.releaseLock(leadId, emailType);
      throw error;
    }
    // Note: Lock is NOT released here - caller must call releaseLock after scheduling
  }
  
  /**
   * Full scheduling guard: acquire lock, validate, and return lock release function
   * @param {number} leadId 
   * @param {string} emailType 
   * @returns {Promise<{allowed: boolean, reason: string|null, release: Function}>}
   */
  async guardScheduling(leadId, emailType) {
    const result = await this.canSchedule(leadId, emailType);
    
    return {
      ...result,
      release: async () => await this.releaseLock(leadId, emailType)
    };
  }
  
  /**
   * Get unique journey count for a lead
   * Counts distinct email types that were successfully sent
   * Excludes: retries (same type), failed that were never delivered
   * @param {number} leadId 
   * @returns {Promise<number>}
   */
  async getUniqueJourneyCount(leadId) {
    const sentStatuses = RulebookService.getSuccessfullySentStatuses();
    
    // Use groupBy to get distinct types with successful status
    const result = await prisma.emailJob.groupBy({
      by: ['type'],
      where: {
        leadId: parseInt(leadId),
        status: { in: sentStatuses }
      }
    });
    
    return result.length;
  }
  
  /**
   * Get complete journey details for a lead
   * @param {number} leadId 
   * @returns {Promise<Object[]>} Array of journey steps with status
   */
  async getJourneyDetails(leadId) {
    const jobs = await prisma.emailJob.findMany({
      where: {
        leadId: parseInt(leadId),
        // Exclude cancelled jobs that were duplicates
        NOT: {
          AND: [
            { status: 'cancelled' },
            { lastError: { contains: 'Duplicate' } }
          ]
        }
      },
      select: {
        id: true,
        type: true,
        status: true,
        scheduledFor: true,
        sentAt: true,
        deliveredAt: true,
        openedAt: true,
        clickedAt: true
      },
      orderBy: { createdAt: 'asc' }
    });
    
    // Group by type, keeping the most relevant job for each type
    const journeyMap = new Map();

    for (const job of jobs) {
      const existing = journeyMap.get(job.type);

      if (!existing) {
        journeyMap.set(job.type, job);
      } else {
        // Keep the one with higher priority status (use RulebookService for status priority)
        const existingPriority = RulebookService.getStatusPriority(
          existing.status,
        );
        const newPriority = RulebookService.getStatusPriority(job.status);

        if (newPriority > existingPriority) {
          journeyMap.set(job.type, job);
        }
      }
    }
    
    return Array.from(journeyMap.values());
  }
  
  /**
   * Record that an email was attempted to send (for worker-level deduplication)
   * Uses database to ensure atomicity across multiple workers
   * @param {number} emailJobId 
   * @returns {Promise<boolean>} true if this is first send attempt, false if duplicate
   */
  async markSendAttempt(emailJobId) {
    try {
      // First fetch the current job to get existing metadata
      const currentJob = await prisma.emailJob.findUnique({
        where: { id: parseInt(emailJobId) },
        select: { metadata: true, status: true },
      });

      if (!currentJob) {
        console.log(`[UniqueJourney] Job ${emailJobId} not found`);
        return false;
      }

      // If already in a processed status, skip
      if (!RulebookService.getActiveStatuses().includes(currentJob.status)) {
        console.log(
          `[UniqueJourney] Job ${emailJobId} already has status '${currentJob.status}', skipping`,
        );
        return false;
      }

      // Use atomic update with conditional check - CRITICAL for preventing race conditions
      // The WHERE clause ensures only one worker can successfully update the status
      const result = await prisma.emailJob.updateMany({
        where: {
          id: parseInt(emailJobId),
          status: { in: RulebookService.getActiveStatuses() },
        },
        data: {
          status: "sending",
          metadata: {
            ...(currentJob.metadata || {}),
            sendAttemptedAt: new Date().toISOString(),
          },
        },
      });

      // If count is 0, another worker already claimed this job
      if (result.count === 0) {
        console.log(
          `[UniqueJourney] Job ${emailJobId} already being processed by another worker`,
        );
        return false;
      }

      console.log(`[UniqueJourney] Marked job ${emailJobId} as sending`);
      return true;
    } catch (error) {
      console.error(`[UniqueJourney] Error marking send attempt for job ${emailJobId}:`, error);
      return false;
    }
  }
}

module.exports = new UniqueJourneyService();
