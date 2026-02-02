/**
 * QueueWatcherService
 * 
 * Central service for managing email job queue priorities.
 * Handles pausing lower-priority jobs when higher-priority jobs are scheduled,
 * and auto-resuming paused jobs when high-priority jobs complete.
 * 
 * Key Concepts:
 * - PAUSED: Temporary hold due to priority conflict (Resume button, no retry increment)
 * - CANCELLED: Permanent failure/manual action (Retry button, increments retry count)
 */

const { prisma } = require("../lib/prisma");
const RulebookService = require('./RulebookService');

class QueueWatcherService {
  
  /**
   * Request permission to schedule a job.
   * This method should be called BEFORE scheduling any job.
   * It will pause any lower-priority pending jobs for the same lead.
   * 
   * @param {number} leadId - The lead ID
   * @param {string} mailType - The type of mail being scheduled
   * @param {Date} requestedTime - When the job is requested to be scheduled
   * @returns {Object} { allowed: boolean, pausedJobIds?: number[], reason?: string }
   */
  async requestSchedulePermission(leadId, mailType, requestedTime) {
    try {
      const priority = RulebookService.getMailTypePriority(mailType);
      
      // Find all pending jobs for this lead with LOWER priority
      const lowerPriorityJobs = await prisma.emailJob.findMany({
        where: {
          leadId: parseInt(leadId),
          status: {
            in: ['pending', 'queued', 'scheduled', 'rescheduled']
          }
        }
      });
      
      const pausedJobIds = [];
      
      for (const job of lowerPriorityJobs) {
        const jobPriority = RulebookService.getMailTypePriority(job.type);
        
        // If the existing job has LOWER priority, pause it
        if (jobPriority < priority) {
          await prisma.emailJob.update({
            where: { id: job.id },
            data: {
              status: 'paused',
              pausedReason: `Higher priority ${mailType} scheduled`,
              pausedAt: new Date(),
              pausedByJobType: mailType
            }
          });
          
          console.log(`[QueueWatcher] Paused job ${job.id} (${job.type}, priority ${jobPriority}) for higher priority ${mailType} (priority ${priority})`);
          pausedJobIds.push(job.id);
        }
      }
      
      return {
        allowed: true,
        pausedJobIds: pausedJobIds.length > 0 ? pausedJobIds : undefined
      };
      
    } catch (error) {
      console.error('[QueueWatcher] Error requesting schedule permission:', error);
      // Allow scheduling even if watcher fails - don't block the system
      return { allowed: true };
    }
  }
  
  /**
   * Resume paused jobs after a high-priority job completes or is cancelled.
   * Called when a job transitions to a terminal state (delivered, cancelled, failed, etc.)
   * 
   * @param {number} leadId - The lead ID
   * @param {string} completedMailType - The type of mail that just completed
   * @returns {Object} { resumedJobIds: number[] }
   */
  async resumePausedJobs(leadId, completedMailType) {
    try {
      // Find all paused jobs that were paused by this mail type
      const pausedJobs = await prisma.emailJob.findMany({
        where: {
          leadId: parseInt(leadId),
          status: 'paused',
          pausedByJobType: completedMailType
        }
      });
      
      if (pausedJobs.length === 0) {
        return { resumedJobIds: [] };
      }
      
      // Check if there are still any HIGH priority pending jobs
      const completedPriority = RulebookService.getMailTypePriority(completedMailType);
      
      const stillPendingHighPriority = await prisma.emailJob.findFirst({
        where: {
          leadId: parseInt(leadId),
          status: {
            in: ['pending', 'queued', 'scheduled', 'rescheduled']
          }
        }
      });
      
      // If there's still a high priority job pending, check its priority
      if (stillPendingHighPriority) {
        const pendingPriority = RulebookService.getMailTypePriority(stillPendingHighPriority.type);
        
        // For each paused job, only resume if no higher priority job exists
        const resumedJobIds = [];
        
        for (const pausedJob of pausedJobs) {
          const pausedJobPriority = RulebookService.getMailTypePriority(pausedJob.type);
          
          // Only resume if the paused job's priority >= all pending jobs
          if (pausedJobPriority >= pendingPriority) {
            await this._resumeJob(pausedJob);
            resumedJobIds.push(pausedJob.id);
          }
        }
        
        return { resumedJobIds };
      }
      
      // No high priority jobs pending, resume all paused jobs
      const resumedJobIds = [];
      
      for (const pausedJob of pausedJobs) {
        await this._resumeJob(pausedJob);
        resumedJobIds.push(pausedJob.id);
      }
      
      console.log(`[QueueWatcher] Auto-resumed ${resumedJobIds.length} jobs after ${completedMailType} completed for lead ${leadId}`);
      
      return { resumedJobIds };
      
    } catch (error) {
      console.error('[QueueWatcher] Error resuming paused jobs:', error);
      return { resumedJobIds: [] };
    }
  }
  
  /**
   * Internal: Resume a single paused job
   */
  async _resumeJob(job) {
    // Calculate new scheduled time if the original has passed
    let newScheduledFor = job.scheduledFor;
    const now = new Date();
    
    if (job.scheduledFor < now) {
      // Original time has passed, reschedule to next available slot
      // Add 30 minutes from now as a simple reschedule
      newScheduledFor = new Date(now.getTime() + 30 * 60 * 1000);
    }
    
    await prisma.emailJob.update({
      where: { id: job.id },
      data: {
        status: 'pending',
        pausedReason: null,
        pausedAt: null,
        pausedByJobType: null,
        scheduledFor: newScheduledFor,
        resumedAt: new Date()
      }
    });
    
    console.log(`[QueueWatcher] Resumed job ${job.id} (${job.type})`);
  }
  
  /**
   * Check if a lead has any higher priority mail pending/scheduled
   * 
   * @param {number} leadId - The lead ID
   * @param {string} comparisonType - The type to compare against
   * @returns {Object} { exists: boolean, type?: string, jobId?: number }
   */
  async hasHigherPriorityMail(leadId, comparisonType) {
    try {
      const comparisonPriority = RulebookService.getMailTypePriority(comparisonType);
      
      const pendingJobs = await prisma.emailJob.findMany({
        where: {
          leadId: parseInt(leadId),
          status: {
            in: ['pending', 'queued', 'scheduled', 'rescheduled']
          }
        }
      });
      
      for (const job of pendingJobs) {
        const jobPriority = RulebookService.getMailTypePriority(job.type);
        
        if (jobPriority > comparisonPriority) {
          return {
            exists: true,
            type: job.type,
            jobId: job.id
          };
        }
      }
      
      return { exists: false };
      
    } catch (error) {
      console.error('[QueueWatcher] Error checking higher priority mail:', error);
      return { exists: false };
    }
  }
  
  /**
   * Manually resume a paused job (user clicks Resume button)
   * This will check if high-priority mail still exists.
   * 
   * @param {number} jobId - The job ID to resume
   * @returns {Object} { success: boolean, error?: string, job?: Object }
   */
  async manualResumeJob(jobId) {
    try {
      const job = await prisma.emailJob.findUnique({
        where: { id: parseInt(jobId) },
        include: { lead: true }
      });
      
      if (!job) {
        return { success: false, error: 'Job not found' };
      }
      
      if (job.status !== 'paused') {
        return { success: false, error: `Job is not paused (status: ${job.status})` };
      }
      
      // Check if there's still a higher priority job pending
      const higherPriority = await this.hasHigherPriorityMail(job.leadId, job.type);
      
      if (higherPriority.exists) {
        return {
          success: false,
          error: `Cannot resume: ${higherPriority.type} is still scheduled`,
          blockedBy: {
            type: higherPriority.type,
            jobId: higherPriority.jobId
          }
        };
      }
      
      // Resume the job - DO NOT increment retry count
      await this._resumeJob(job);
      
      // Fetch the updated job
      const updatedJob = await prisma.emailJob.findUnique({
        where: { id: parseInt(jobId) },
        include: { lead: true }
      });
      
      return { success: true, job: updatedJob };
      
    } catch (error) {
      console.error('[QueueWatcher] Error manually resuming job:', error);
      return { success: false, error: error.message };
    }
  }
  
  /**
   * Get all paused jobs for a lead
   */
  async getPausedJobs(leadId) {
    return await prisma.emailJob.findMany({
      where: {
        leadId: parseInt(leadId),
        status: 'paused'
      },
      orderBy: { scheduledFor: 'asc' }
    });
  }
  
  /**
   * Get pause reason for a job
   */
  async getPauseReason(jobId) {
    const job = await prisma.emailJob.findUnique({
      where: { id: parseInt(jobId) },
      select: { pausedReason: true, pausedByJobType: true, pausedAt: true }
    });
    
    return job;
  }
}

module.exports = new QueueWatcherService();
