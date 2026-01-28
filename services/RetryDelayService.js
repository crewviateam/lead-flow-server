// services/RetryDelayService.js
// Single Source of Truth for retry and recovery delay configuration
// Handles soft bounce delays, retry attempts, and recovery logic

class RetryDelayService {
  
  /**
   * Get retry configuration from settings
   * @param {Object} settings 
   * @returns {Object} { maxAttempts, softBounceDelayHours, enabled }
   */
  getConfig(settings) {
    return {
      maxAttempts: settings?.retry?.maxAttempts ?? settings?.retryMaxAttempts ?? 5,
      softBounceDelayHours: settings?.retry?.softBounceDelayHours ?? settings?.retrySoftBounceDelayHrs ?? 2,
      enabled: true
    };
  }
  
  /**
   * Check if a job should be retried based on status and retry count
   * @param {Object} job - EmailJob object
   * @param {Object} settings 
   * @returns {Object} { shouldRetry: boolean, reason?: string, delayHours?: number }
   */
  shouldRetry(job, settings) {
    const config = this.getConfig(settings);
    const currentRetryCount = job.retryCount || 0;
    
    // Check max attempts
    if (currentRetryCount >= config.maxAttempts) {
      return { 
        shouldRetry: false, 
        reason: `Max retry attempts (${config.maxAttempts}) reached` 
      };
    }
    
    // Check if status is retriable
    const retriableStatuses = ['soft_bounce', 'deferred', 'failed'];
    if (!retriableStatuses.includes(job.status)) {
      return { 
        shouldRetry: false, 
        reason: `Status '${job.status}' is not retriable` 
      };
    }
    
    // Determine delay based on status
    let delayHours = config.softBounceDelayHours;
    
    if (job.status === 'deferred') {
      delayHours = 1; // Shorter delay for deferred
    } else if (job.status === 'failed') {
      delayHours = config.softBounceDelayHours * (currentRetryCount + 1); // Exponential backoff
    }
    
    return {
      shouldRetry: true,
      delayHours,
      nextRetryCount: currentRetryCount + 1,
      maxAttempts: config.maxAttempts
    };
  }
  
  /**
   * Get delay in hours for a specific retry type
   * @param {string} retryType - 'soft_bounce', 'deferred', 'failed'
   * @param {Object} settings 
   * @param {number} retryCount - Current retry count
   * @returns {number} Delay in hours
   */
  getDelayHours(retryType, settings, retryCount = 0) {
    const config = this.getConfig(settings);
    
    switch (retryType) {
      case 'soft_bounce':
        return config.softBounceDelayHours;
      case 'deferred':
        return 1;
      case 'failed':
        // Exponential backoff with cap
        return Math.min(config.softBounceDelayHours * Math.pow(2, retryCount), 48);
      default:
        return config.softBounceDelayHours;
    }
  }
  
  /**
   * Check if a status is retriable
   * @param {string} status 
   * @returns {boolean}
   */
  isRetriableStatus(status) {
    const retriableStatuses = ['soft_bounce', 'deferred', 'failed'];
    return retriableStatuses.includes(status);
  }
  
  /**
   * Check if a status is terminal (no retry possible)
   * @param {string} status 
   * @returns {boolean}
   */
  isTerminalStatus(status) {
    const terminalStatuses = ['hard_bounce', 'blocked', 'spam', 'unsubscribed'];
    return terminalStatuses.includes(status);
  }
}

module.exports = new RetryDelayService();
