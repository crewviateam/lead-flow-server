// controllers/EmailJobController.js
// Email job controller using Prisma

const { EmailJobRepository, LeadRepository, SettingsRepository } = require('../repositories');
const { prisma } = require('../lib/prisma');
const { emailSendQueue } = require('../queues/emailQueues');
const moment = require('moment-timezone');
const { v4: uuidv4 } = require('uuid');
const SchedulingRulesService = require('../services/SchedulingRulesService');
const RulebookService = require('../services/RulebookService');
const EmailSchedulerService = require('../services/EmailSchedulerService');
const SmartDelayService = require('../services/SmartDelayService');

class EmailJobController {
  // Get all email jobs with pagination and filters
  async getEmailJobs(req, res) {
    try {
      const { 
        page = 1, 
        limit = 20, 
        status, 
        type,
        sortBy = 'scheduledFor',
        sortOrder = 'desc',
        view,
        startDate,
        endDate
      } = req.query;

      const where = {};
      if (status) where.status = status;
      if (type) where.type = type;

      // Date Range Filtering
      if (startDate && endDate) {
        where.scheduledFor = {
          gte: new Date(startDate),
          lte: new Date(endDate)
        };
      }

      // View filtering
      if (view === 'active') {
        where.status = { in: RulebookService.getPendingOnlyStatuses() };
      } else if (view === 'history') {
        where.status = { in: RulebookService.getCompletedHistoryStatuses() };
      }

      // Cap limit at 100 to prevent excessive data fetching
      const cappedLimit = Math.min(parseInt(limit) || 20, 100);

      const jobs = await prisma.emailJob.findMany({
        where,
        include: {
          lead: {
            select: { id: true, name: true, email: true, country: true, city: true, timezone: true }
          }
        },
        orderBy: { [sortBy]: sortOrder },
        skip: (parseInt(page) - 1) * cappedLimit,
        take: cappedLimit
      });

      const total = await prisma.emailJob.count({ where });

      // Calculate time until next scheduled and add display-friendly type/status
      const now = new Date();
      const enrichedJobs = await Promise.all(jobs.map(async job => {
        const displayType = RulebookService.getDisplayTypeName(job);
        // Use async version for conditional emails to lookup triggerEvent if missing
        const displayStatus = await RulebookService.formatJobStatusForDisplayAsync(job);
        // console.log("displayStatus==============", displayStatus);
        
        return {
          ...job,
          leadId: job.leadId,
          displayType,       // e.g., "conditional opened" or "First Followup"
          displayStatus,     // e.g., "condition opened:pending"
          timeUntilScheduled: job.scheduledFor > now 
            ? Math.round((job.scheduledFor - now) / 60000) 
            : null
        };
      }));

      res.status(200).json({
        jobs: enrichedJobs,
        pagination: {
          page: parseInt(page),
          limit: cappedLimit,
          total,
          pages: Math.ceil(total / cappedLimit)
        }
      });
    } catch (error) {
      console.error('Get email jobs error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Get single email job by ID
  async getEmailJob(req, res) {
    try {
      const { id } = req.params;
      
      const job = await prisma.emailJob.findUnique({
        where: { id: parseInt(id) },
        include: {
          lead: {
            select: { id: true, name: true, email: true, country: true, city: true, timezone: true, status: true }
          }
        }
      });

      if (!job) {
        return res.status(404).json({ error: 'Email job not found' });
      }

      res.status(200).json({ job });
    } catch (error) {
      console.error('Get email job error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Retry a failed/bounced job
  async retryJob(req, res) {
    try {
      const { id } = req.params;

      const oldJob = await EmailJobRepository.findById(parseInt(id));
      if (!oldJob) {
        return res.status(404).json({ error: "Email job not found" });
      }

      // Allow retry for all terminal failure states
      // IMPORTANT: Paused jobs should use 'resume', not 'retry'
      const retriableStatuses = RulebookService.getRetriableStatuses();
      if (!retriableStatuses.includes(oldJob.status)) {
        // Special check for paused - suggest using resume instead
        if (oldJob.status === "paused") {
          return res.status(400).json({
            error: `Paused jobs should be resumed, not retried. Use the Resume action instead.`,
            code: "USE_RESUME_INSTEAD",
            suggestResume: true,
          });
        }
        return res
          .status(400)
          .json({
            error: `Job status '${oldJob.status}' is not retriable. Allowed: ${retriableStatuses.join(", ")}`,
          });
      }

      // Get lead info
      const lead = await LeadRepository.findById(oldJob.leadId);
      if (!lead) {
        return res.status(404).json({ error: "Lead not found" });
      }

      // Calculate new retry count BEFORE checking limits
      const newRetryCount = (oldJob.retryCount || 0) + 1;

      // Check if retry would exceed max retry limit
      const maxRetries = await RulebookService.getMaxRetries(oldJob.type);
      if (newRetryCount > maxRetries) {
        console.log(
          `[RetryJob] Max retries exceeded (${newRetryCount}/${maxRetries}), marking lead ${lead.id} as dead`,
        );

        // Mark lead as dead
        await prisma.lead.update({
          where: { id: lead.id },
          data: {
            status: "dead",
            terminalState: "dead",
            terminalStateAt: new Date(),
            terminalReason: `Manual retry blocked: max retries exceeded (${newRetryCount}/${maxRetries})`,
            totalRetries: newRetryCount,
          },
        });

        // Cancel ALL pending/active jobs for this lead
        const activeStatuses = RulebookService.getActiveStatuses();
        const cancelledJobs = await prisma.emailJob.updateMany({
          where: {
            leadId: lead.id,
            status: { in: [...activeStatuses, "paused"] },
          },
          data: {
            status: "cancelled",
            lastError: "Lead marked as dead - max retries exceeded",
          },
        });
        console.log(
          `[RetryJob] Cancelled ${cancelledJobs.count} pending jobs for dead lead`,
        );

        // Mark the original job as dead too
        await prisma.emailJob.update({
          where: { id: parseInt(id) },
          data: {
            status: "dead",
            lastError: `Max retries exceeded (${newRetryCount}/${maxRetries})`,
          },
        });

        // Create notification
        await prisma.notification.create({
          data: {
            type: "warning",
            message: `Lead ${lead.name || lead.email} marked as dead`,
            details: `Manual retry blocked: max retries exceeded (${newRetryCount}/${maxRetries}). All pending emails cancelled.`,
            leadId: lead.id,
            emailJobId: parseInt(id),
            event: "dead",
          },
        });

        // Add event history
        await LeadRepository.addEvent(
          lead.id,
          "dead",
          {
            reason: `Manual retry blocked: max retries exceeded (${newRetryCount}/${maxRetries})`,
            jobId: oldJob.id,
            cancelledJobs: cancelledJobs.count,
          },
          oldJob.type,
          oldJob.id,
        );

        // Return success response (not error) since we handled the situation
        return res.status(200).json({
          success: true,
          markedAsDead: true,
          message: `Lead marked as dead - max retries exceeded (${newRetryCount}/${maxRetries}). All pending emails cancelled.`,
          maxRetries,
          currentRetries: newRetryCount,
          cancelledJobs: cancelledJobs.count,
        });
      }

      // 1. Mark OLD job metadata as 'rescheduled' but KEEP STATUS
      await prisma.emailJob.update({
        where: { id: parseInt(id) },
        data: {
          metadata: {
            ...oldJob.metadata,
            rescheduled: true,
            rescheduledAt: new Date(),
          },
        },
      });

      // 2. Calculate new scheduled time using SmartDelayService
      // This respects the configured delay hours (e.g., 2 hours) and working hours/days
      const retryDelayHours = await RulebookService.getRetryDelayHours();

      console.log(
        `[RetryJob] Calculating schedule with ${retryDelayHours}hr delay for job ${id}`,
      );

      const delayResult = await SmartDelayService.calculateNextValidTime(
        new Date(),
        retryDelayHours,
        lead.timezone || "UTC",
      );

      let newScheduledFor = delayResult.time;
      console.log(
        `[RetryJob] SmartDelayService result: ${newScheduledFor} (shifted: ${delayResult.wasShifted}, reason: ${delayResult.shiftReason || "none"})`,
      );

      // Validate the time
      if (!newScheduledFor || isNaN(newScheduledFor.getTime())) {
        // Fallback: use the configured delay from now
        newScheduledFor = new Date(
          Date.now() + retryDelayHours * 60 * 60 * 1000,
        );
        console.warn(
          "[RetryJob] SmartDelayService returned invalid time, using simple delay fallback",
        );
      }

      // 3. Create NEW job for the retry using EmailSchedulerService (Enforces Rate Limits!)
      const settings = await SettingsRepository.getSettings();
      const schedulerSettings = {
        businessHours: settings.businessHours,
        windowMinutes: settings.rateLimit?.windowMinutes || 15,
      };

      // Use scheduleEmailJob to respect rate limits and business hours
      // Pass skipDuplicateCheck: true because this is an explicit retry of a cancelled/failed job
      const newJob = await EmailSchedulerService.scheduleEmailJob(
        lead,
        oldJob.type,
        newScheduledFor, // Target time (calculated with SmartDelay)
        schedulerSettings,
        "rescheduled", // Use 'rescheduled' status to differentiate from original 'pending' jobs
        newRetryCount, // Properly incremented retry count
        oldJob.templateId,
        null, // condition
        { skipDuplicateCheck: true }, // Allow retry even if old job exists
      );

      if (!newJob) {
        return res
          .status(400)
          .json({
            error:
              "Failed to schedule retry: Rate limits or duplicate check prevented scheduling",
          });
      }

      // CLEAR FAILURE STATE: Retry scheduled successfully, clear the failure flag
      // This allows auto-resume to work again after manual intervention
      await prisma.lead.update({
        where: { id: lead.id },
        data: {
          isInFailure: false,
          // Keep lastFailureAt/lastFailureType for history
        },
      });
      console.log(
        `[RetryJob] Cleared isInFailure flag for lead ${lead.id} after successful retry schedule`,
      );

      // Update old job to mark as rescheduled with reference to new job
      await prisma.emailJob.update({
        where: { id: parseInt(id) },
        data: {
          status: "rescheduled",
          metadata: {
            ...oldJob.metadata,
            rescheduled: true,
            rescheduledAt: new Date(),
            rescheduledTo: newJob.id,
          },
        },
      });

      // PRIORITY-BASED PAUSING: Check if this job type has higher priority than existing pending jobs
      // If so, pause the lower priority jobs
      const retryJobPriority = RulebookService.getMailTypePriority(oldJob.type);
      const queueWatcherRules = RulebookService.getQueueWatcherRules();

      if (
        queueWatcherRules.priorityScheduling.enabled &&
        queueWatcherRules.priorityScheduling.canPauseLowerPriority
      ) {
        // Find all pending jobs for this lead with lower priority
        const activeStatuses = RulebookService.getActiveStatuses();
        const pendingJobs = await prisma.emailJob.findMany({
          where: {
            leadId: lead.id,
            status: { in: activeStatuses },
            id: { not: newJob.id }, // Exclude the job we just created
          },
        });

        for (const pendingJob of pendingJobs) {
          const pendingJobPriority = RulebookService.getMailTypePriority(
            pendingJob.type,
          );

          // If the retried job has HIGHER priority, pause the lower priority job
          if (retryJobPriority > pendingJobPriority) {
            console.log(
              `[RetryJob] Pausing lower priority job ${pendingJob.id} (${pendingJob.type}, priority=${pendingJobPriority}) for higher priority retry (${oldJob.type}, priority=${retryJobPriority})`,
            );

            // Remove from BullMQ queue
            if (pendingJob.metadata?.queueJobId) {
              try {
                const bullJob = await emailSendQueue.getJob(
                  pendingJob.metadata.queueJobId,
                );
                if (bullJob) await bullJob.remove();
              } catch (e) {
                console.warn(
                  `[RetryJob] Could not remove BullMQ job: ${e.message}`,
                );
              }
            }

            // Update job status to paused
            await prisma.emailJob.update({
              where: { id: pendingJob.id },
              data: {
                status: "paused",
                lastError: `Paused: Higher priority ${oldJob.type} was retried`,
                cancellationReason: "priority_paused",
              },
            });

            // Create event history
            await prisma.eventHistory.create({
              data: {
                leadId: lead.id,
                event: "job_paused_priority",
                timestamp: new Date(),
                details: {
                  reason: `Paused due to higher priority ${oldJob.type} retry`,
                  pausedJobId: pendingJob.id,
                  pausedJobType: pendingJob.type,
                  retryJobId: newJob.id,
                  retryJobType: oldJob.type,
                },
                emailType: pendingJob.type,
                emailJobId: pendingJob.id,
              },
            });
          }
        }
      }

      // Update metadata to link to old job and preserve conditional email info
      await prisma.emailJob.update({
        where: { id: newJob.id },
        data: {
          metadata: {
            ...newJob.metadata,
            ...oldJob.metadata, // Preserve original metadata including triggerEvent
            rescheduled: true,
            retryReason: `Retry from ${oldJob.status}`,
            originalJobId: oldJob.id,
          },
        },
      });

      // 4. Update Lead status and add event
      // For conditional emails, use "condition {triggerEvent}:rescheduled" format
      let newLeadStatus;
      if (RulebookService.isConditional(oldJob.type)) {
        let triggerEvent = oldJob.metadata?.triggerEvent;

        // Fallback: Try to get trigger event from conditional email settings
        if (!triggerEvent && oldJob.type.startsWith("conditional:")) {
          const conditionalName = oldJob.type.replace("conditional:", "");
          try {
            const conditionalSettings = await prisma.conditionalEmail.findFirst(
              {
                where: { name: conditionalName },
              },
            );
            if (conditionalSettings?.triggerEvent) {
              triggerEvent = conditionalSettings.triggerEvent;
            }
          } catch (e) {
            console.warn(
              `[RetryJob] Could not lookup conditional settings: ${e.message}`,
            );
          }
        }

        // If we found a trigger event, use proper format
        if (triggerEvent) {
          newLeadStatus = RulebookService.formatConditionalStatus(
            triggerEvent,
            "rescheduled",
          );
        } else {
          // Last resort: use a generic conditional format
          newLeadStatus = `conditional:rescheduled`;
        }
      } else {
        newLeadStatus = `${oldJob.type}:rescheduled`;
      }
      await LeadRepository.updateStatus(lead.id, newLeadStatus);
      await LeadRepository.addEvent(
        lead.id,
        "rescheduled",
        {
          reason: `Manual retry from ${oldJob.status}`,
          oldJobId: oldJob.id,
          newJobId: newJob.id,
          newScheduledFor,
        },
        oldJob.type,
        newJob.id,
      );

      // 4.5 SYNC emailSchedule status (for Sequence Progress card)
      const isInitial = oldJob.type?.toLowerCase().includes("initial");
      if (isInitial) {
        // Update initialStatus in emailSchedule
        await prisma.emailSchedule.updateMany({
          where: { leadId: lead.id },
          data: {
            initialStatus: "rescheduled",
            initialScheduledFor: newScheduledFor,
          },
        });
        console.log(
          `[RetryJob] Synced emailSchedule.initialStatus to 'rescheduled'`,
        );
      } else {
        // Update followup status in emailSchedule.followups JSON
        const leadWithSchedule = await prisma.lead.findUnique({
          where: { id: lead.id },
          include: { emailSchedule: true },
        });

        if (leadWithSchedule?.emailSchedule) {
          let followups = leadWithSchedule.emailSchedule.followups || [];
          if (!Array.isArray(followups)) {
            try {
              followups = JSON.parse(followups);
            } catch (e) {
              followups = [];
            }
          }

          const followupIndex = followups.findIndex(
            (f) => f.name === oldJob.type,
          );
          if (followupIndex >= 0) {
            followups[followupIndex].status = "rescheduled";
            followups[followupIndex].scheduledFor = newScheduledFor;
            await prisma.emailSchedule.update({
              where: { id: leadWithSchedule.emailSchedule.id },
              data: { followups },
            });
            console.log(
              `[RetryJob] Synced emailSchedule.followups[${oldJob.type}].status to 'rescheduled'`,
            );
          }
        }
      }

      // 5. Queue the new job
      const delay = Math.max(0, newScheduledFor.getTime() - Date.now());
      const queueJob = await emailSendQueue.add(
        "send-email",
        {
          emailJobId: newJob.id,
          leadId: lead.id,
          leadEmail: lead.email,
          emailType: newJob.type,
        },
        {
          delay,
          jobId: `email-${newJob.id}`,
          removeOnComplete: 1000,
          removeOnFail: 5000,
        },
      );

      // Update job with queue reference
      await prisma.emailJob.update({
        where: { id: newJob.id },
        data: {
          metadata: { ...newJob.metadata, queueJobId: queueJob.id },
        },
      });

      res.status(200).json({
        success: true,
        message: `Job retried successfully. New job scheduled for ${newScheduledFor}`,
        oldJob: { id: oldJob.id, status: oldJob.status },
        newJob: { id: newJob.id, scheduledFor: newScheduledFor },
        retryCount: newRetryCount,
      });
    } catch (error) {
      console.error('Retry job error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Resume a paused job
   * This is for jobs that were paused due to priority (e.g., higher priority mail scheduled)
   * IMPORTANT: Resume does NOT increment retry count - it's not a failure retry
   */
  async resumeJob(req, res) {
    try {
      const { id } = req.params;
      
      // Use QueueWatcherService for resume logic
      const QueueWatcherService = require('../services/QueueWatcherService');
      const result = await QueueWatcherService.manualResumeJob(parseInt(id));
      
      if (!result.success) {
        return res.status(400).json({ 
          error: result.error,
          blockedBy: result.blockedBy
        });
      }
      
      const job = result.job;
      
      // Add the job back to BullMQ queue
      const delay = Math.max(0, new Date(job.scheduledFor).getTime() - Date.now());
      const queueJob = await emailSendQueue.add('send-email', {
        emailJobId: job.id,
        leadId: job.leadId,
        leadEmail: job.email,
        emailType: job.type
      }, {
        delay,
        jobId: `email-${job.id}`,
        removeOnComplete: 1000,
        removeOnFail: 5000
      });
      
      // Update job with queue reference
      await prisma.emailJob.update({
        where: { id: job.id },
        data: {
          metadata: { ...job.metadata, queueJobId: queueJob.id }
        }
      });
      
      // Update lead status
      const newStatus = `${job.type}:pending`;
      await LeadRepository.updateStatus(job.leadId, newStatus);
      
      // Add event history
      await LeadRepository.addEvent(job.leadId, 'job_resumed', {
        reason: 'Manual resume after pause',
        jobId: job.id,
        scheduledFor: job.scheduledFor,
        pausedReason: job.pausedReason
      }, job.type, job.id);
      
      res.status(200).json({
        success: true,
        message: `Job resumed successfully. Scheduled for ${job.scheduledFor}`,
        job: {
          id: job.id,
          type: job.type,
          status: 'pending',
          scheduledFor: job.scheduledFor
        }
      });
      
    } catch (error) {
      console.error('Resume job error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Cancel a pending job
  async cancelJob(req, res) {
    try {
      const { id } = req.params;
      const { reason } = req.body;
      
      const job = await EmailJobRepository.findById(parseInt(id));
      if (!job) {
        return res.status(404).json({ error: 'Email job not found' });
      }

      // RULEBOOK VALIDATION: Check if cancel is allowed for this type and status
      const validation = RulebookService.validateAction('cancel', job.type, job.status);
      if (!validation.allowed) {
        const canSkip = RulebookService.canSkipType(job.type);
        return res.status(400).json({ 
          error: validation.reason,
          code: 'ACTION_NOT_ALLOWED',
          suggestSkip: canSkip
        });
      }

      // Remove from queue if present
      if (job.metadata?.queueJobId) {
        try {
          const queueJob = await emailSendQueue.getJob(job.metadata.queueJobId);
          if (queueJob) await queueJob.remove();
        } catch (e) { /* ignore */ }
      }

      // Update job status
      await prisma.emailJob.update({
        where: { id: parseInt(id) },
        data: {
          status: 'cancelled',
          lastError: reason || 'Manual cancellation',
          metadata: { ...job.metadata, cancelledAt: new Date(), cancelReason: reason }
        }
      });

      // Update lead with cancellation event
      const lead = await LeadRepository.findById(job.leadId);
      if (lead) {
        await LeadRepository.addEvent(lead.id, 'cancelled', {
          reason: reason || 'Manual cancellation',
          jobId: job.id,
          emailType: job.type
        }, job.type, job.id);

        // If this was a RETRY job (has originalJobId or retryReason), restore original failure status
        const originalJobId = job.metadata?.originalJobId;
        const retryReason = job.metadata?.retryReason;
        
        let newStatus = `${job.type}:cancelled`;
        
        if (originalJobId || retryReason) {
          // This was a retry - find original failure status
          if (originalJobId) {
            const originalJob = await prisma.emailJob.findUnique({ where: { id: originalJobId } });
            if (originalJob && ['blocked', 'failed', 'hard_bounce', 'soft_bounce', 'spam', 'bounced'].includes(originalJob.status)) {
              newStatus = `${job.type}:${originalJob.status}`;
              console.log(`[CancelJob] Restoring original failure status: ${newStatus}`);
            }
          } else if (retryReason) {
            // Extract status from retryReason like "Retry from blocked"
            const match = retryReason.match(/Retry from (\w+)/);
            if (match && ['blocked', 'failed', 'hard_bounce', 'soft_bounce', 'spam', 'bounced'].includes(match[1])) {
              newStatus = `${job.type}:${match[1]}`;
              console.log(`[CancelJob] Restoring failure status from reason: ${newStatus}`);
            }
          }
        }
        
        // Update lead status
        await LeadRepository.updateStatus(lead.id, newStatus);
        
        // SYNC emailSchedule status (for Sequence Progress card)
        const isInitial = job.type?.toLowerCase().includes('initial');
        const statusPart = newStatus.split(':')[1] || 'cancelled';
        
        if (isInitial) {
          await prisma.emailSchedule.updateMany({
            where: { leadId: lead.id },
            data: { initialStatus: statusPart }
          });
          console.log(`[CancelJob] Synced emailSchedule.initialStatus to '${statusPart}'`);
        } else if (job.type !== 'manual') {
          // Update followup status in emailSchedule.followups JSON
          const leadWithSchedule = await prisma.lead.findUnique({
            where: { id: lead.id },
            include: { emailSchedule: true }
          });
          
          if (leadWithSchedule?.emailSchedule) {
            let followups = leadWithSchedule.emailSchedule.followups || [];
            if (!Array.isArray(followups)) {
              try { followups = JSON.parse(followups); } catch(e) { followups = []; }
            }
            
            const followupIndex = followups.findIndex(f => f.name === job.type);
            if (followupIndex >= 0) {
              followups[followupIndex].status = statusPart;
              await prisma.emailSchedule.update({
                where: { id: leadWithSchedule.emailSchedule.id },
                data: { followups }
              });
              console.log(`[CancelJob] Synced emailSchedule.followups[${job.type}].status to '${statusPart}'`);
            }
          }
        }
        
        // If this is a manual mail, sync ManualMail.status to cancelled
        if (job.type === 'manual' || job.metadata?.manual) {
          const manualMailUpdate = await prisma.manualMail.updateMany({
            where: { emailJobId: job.id },
            data: { status: 'cancelled' }
          });
          console.log(`[CancelJob] Synced ManualMail.status to 'cancelled' for job ${job.id} (updated: ${manualMailUpdate.count})`);
          
          // Also update emailSchedule.followups for the manual entry
          const scheduleForManual = await prisma.lead.findUnique({
            where: { id: lead.id },
            include: { emailSchedule: true }
          });
          
          if (scheduleForManual?.emailSchedule) {
            let manualFollowups = scheduleForManual.emailSchedule.followups || [];
            if (!Array.isArray(manualFollowups)) {
              try { manualFollowups = JSON.parse(manualFollowups); } catch(e) { manualFollowups = []; }
            }
            
            // Find and update the manual mail entry in followups
            const manualIndex = manualFollowups.findIndex(f => 
              f.name?.toLowerCase() === 'manual' || 
              f.name?.toLowerCase().includes('testing') ||
              f.emailJobId === job.id
            );
            
            if (manualIndex >= 0) {
              manualFollowups[manualIndex].status = 'cancelled';
              await prisma.emailSchedule.update({
                where: { id: scheduleForManual.emailSchedule.id },
                data: { followups: manualFollowups }
              });
              console.log(`[CancelJob] Synced emailSchedule manual entry to 'cancelled'`);
            }
          }
          
          // Auto-resume paused followups when manual mail is cancelled
          if (lead.followupsPaused) {
            console.log(`[CancelJob] Auto-resuming followups for lead ${lead.id} after manual mail cancellation`);
            
            // Reset followupsPaused flag
            await prisma.lead.update({
              where: { id: lead.id },
              data: { followupsPaused: false }
            });
            
            // Resume paused followup jobs
            const { SettingsRepository } = require('../repositories');
            const settings = await SettingsRepository.getSettings();
            const followupNames = (settings?.followups || [])
              .filter(f => f.enabled && !f.name.toLowerCase().includes('initial'))
              .map(f => f.name);
            
            // Update paused jobs back to pending
            await prisma.emailJob.updateMany({
              where: {
                leadId: lead.id,
                status: 'paused',
                type: { in: followupNames }
              },
              data: {
                status: 'pending',
                lastError: 'Auto-resumed after manual mail cancellation'
              }
            });
            
            // Update emailSchedule followups status back to scheduled
            const resumeSchedule = await prisma.lead.findUnique({
              where: { id: lead.id },
              include: { emailSchedule: true }
            });
            
            if (resumeSchedule?.emailSchedule) {
              let resumeFollowups = resumeSchedule.emailSchedule.followups || [];
              if (!Array.isArray(resumeFollowups)) {
                try { resumeFollowups = JSON.parse(resumeFollowups); } catch(e) { resumeFollowups = []; }
              }
              
              for (const followup of resumeFollowups) {
                if (followup.status === 'paused') {
                  followup.status = 'scheduled';
                }
              }
              
              await prisma.emailSchedule.update({
                where: { id: resumeSchedule.emailSchedule.id },
                data: { followups: resumeFollowups }
              });
            }
            
            // Add event history for auto-resume
            await LeadRepository.addEvent(lead.id, 'resumed', {
              reason: 'Auto-resumed after manual mail cancellation'
            }, 'followups');
            
            console.log(`[CancelJob] Auto-resumed followups for lead ${lead.id}`);
          }
        }
        
        // AUTO-RESUME: Also check if cancelled job is a conditional email
        // If so, resume paused followups similar to manual mail
        const isConditional = job.type?.startsWith('conditional:');
        if (isConditional && lead.followupsPaused) {
          console.log(`[CancelJob] Auto-resuming followups for lead ${lead.id} after conditional mail cancellation`);
          
          // Reset followupsPaused flag
          await prisma.lead.update({
            where: { id: lead.id },
            data: { followupsPaused: false }
          });
          
          // Schedule next followup
          const EmailSchedulerService = require('../services/EmailSchedulerService');
          await EmailSchedulerService.scheduleNextEmail(lead.id);
          
          // Add event history for auto-resume
          await LeadRepository.addEvent(lead.id, 'resumed', {
            reason: `Auto-resumed after conditional mail (${job.type}) cancellation`
          }, 'followups');
          
          console.log(`[CancelJob] Auto-resumed followups for lead ${lead.id} after conditional cancellation`);
        }
        
        // PRIORITY-BASED AUTO-RESUME: Resume paused jobs when a high-priority job is cancelled
        // This works for conditional mail and any other high-priority cancellations
        const cancelledJobPriority = RulebookService.getMailTypePriority(job.type);
        const queueWatcherRules = RulebookService.getQueueWatcherRules();
        
        if (queueWatcherRules.priorityScheduling.enabled && queueWatcherRules.priorityScheduling.resumeOnComplete) {
          // Find paused jobs that were paused due to this higher priority job
          const pausedJobs = await prisma.emailJob.findMany({
            where: {
              leadId: lead.id,
              status: 'paused',
              cancellationReason: 'priority_paused'
            }
          });
          
          const { SettingsRepository } = require('../repositories');
          const settings = await SettingsRepository.getSettings();
          const schedulerSettings = {
            businessHours: settings.businessHours,
            windowMinutes: settings.rateLimit?.windowMinutes || 15
          };
          
          for (const pausedJob of pausedJobs) {
            const pausedJobPriority = RulebookService.getMailTypePriority(pausedJob.type);
            
            // Only resume if the cancelled job was higher priority than the paused job
            if (cancelledJobPriority > pausedJobPriority) {
              console.log(`[CancelJob] Auto-resuming paused job ${pausedJob.id} (${pausedJob.type}) after higher priority ${job.type} cancellation`);
              
              // Get a new scheduled time
              const EmailSchedulerService = require('../services/EmailSchedulerService');
              let newScheduledFor;
              try {
                const slots = await EmailSchedulerService.getAvailableSlots(lead.id, 168);
                if (slots?.length > 0 && slots[0].utc) {
                  newScheduledFor = new Date(slots[0].utc);
                }
              } catch (e) {
                console.warn(`[CancelJob] Could not get slots for resume: ${e.message}`);
              }
              
              if (!newScheduledFor || isNaN(newScheduledFor.getTime())) {
                newScheduledFor = new Date(Date.now() + 30 * 60 * 1000);
              }
              
              // Update job status to pending and reschedule
              await prisma.emailJob.update({
                where: { id: pausedJob.id },
                data: {
                  status: 'pending',
                  scheduledFor: newScheduledFor,
                  lastError: `Auto-resumed after ${job.type} cancellation`,
                  cancellationReason: null
                }
              });
              
              // Add to BullMQ queue
              const delay = Math.max(0, newScheduledFor.getTime() - Date.now());
              const queueJob = await emailSendQueue.add('send-email', {
                emailJobId: pausedJob.id,
                leadId: lead.id,
                leadEmail: lead.email,
                emailType: pausedJob.type
              }, {
                delay,
                jobId: `email-${pausedJob.id}-resumed`,
                removeOnComplete: 1000,
                removeOnFail: 5000
              });
              
              // Update metadata with queue job ID
              await prisma.emailJob.update({
                where: { id: pausedJob.id },
                data: {
                  metadata: { ...pausedJob.metadata, queueJobId: queueJob.id }
                }
              });
              
              // Create event history
              await prisma.eventHistory.create({
                data: {
                  leadId: lead.id,
                  event: 'job_auto_resumed',
                  timestamp: new Date(),
                  details: {
                    reason: `Auto-resumed after ${job.type} cancellation`,
                    resumedJobId: pausedJob.id,
                    resumedJobType: pausedJob.type,
                    cancelledJobType: job.type,
                    newScheduledFor
                  },
                  emailType: pausedJob.type,
                  emailJobId: pausedJob.id
                }
              });
              
              console.log(`[CancelJob] Resumed job ${pausedJob.id} scheduled for ${newScheduledFor}`);
            }
          }
        }
      }

      // CRITICAL: Use RulebookService to ensure lead status is 100% accurate
      // This looks for next scheduled job and sets lead status accordingly
      if (lead) {
        await RulebookService.syncLeadStatusAfterJobChange(lead.id, 'job_cancelled_manual');
      }

      res.status(200).json({
        success: true,
        message: 'Job cancelled successfully',
        job: await EmailJobRepository.findById(parseInt(id))
      });
    } catch (error) {
      console.error('Cancel job error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Reschedule a job (drag-and-drop)
  async rescheduleJob(req, res) {
    try {
      const { id } = req.params;
      const { newTime, newScheduledFor, scheduledFor } = req.body;
      
      const scheduledTime = newTime || newScheduledFor || scheduledFor;
      if (!scheduledTime) {
        return res.status(400).json({ error: 'New scheduled time is required' });
      }

      const newDate = new Date(scheduledTime);
      if (isNaN(newDate.getTime())) {
        return res.status(400).json({ error: 'Invalid date format' });
      }

      const job = await EmailJobRepository.findById(parseInt(id));
      if (!job) {
        return res.status(404).json({ error: 'Email job not found' });
      }

      // Only allow rescheduling pending/queued jobs
      const reschedulableStatuses = RulebookService.getReschedulableStatuses();
      if (!reschedulableStatuses.includes(job.status)) {
        return res.status(400).json({ error: `Cannot reschedule job with status '${job.status}'` });
      }

      const oldScheduledFor = job.scheduledFor;

      // ============================================
      // BUSINESS HOURS VALIDATION
      // Validate the new time against scheduling rules
      // ============================================
      const validation = await SchedulingRulesService.validateScheduleTime(job.leadId, newDate);
      const errors = validation.issues?.filter(i => i.severity === 'error') || [];
      
      if (errors.length > 0) {
        return res.status(400).json({ 
          error: errors[0].message,
          issues: validation.issues,
          localTime: errors[0].details?.localTime,
          timezone: validation.lead?.timezone,
          businessHours: errors[0].details?.businessHours
        });
      }

      // Remove old queue job
      if (job.metadata?.queueJobId) {
        try {
          const queueJob = await emailSendQueue.getJob(job.metadata.queueJobId);
          if (queueJob) await queueJob.remove();
        } catch (e) { /* ignore */ }
      }

      // CRITICAL: Determine the status to use after slot change
      // Preserve the original status if it's 'rescheduled', otherwise use 'pending'
      // This ensures retried jobs don't lose their tracking
      
      // CRITICAL: Determine the status to use after slot change
      // Preserve the original status if it's 'rescheduled', otherwise use 'pending'
      // This ensures retried jobs don't lose their tracking
      const statusToKeep = job.status === 'rescheduled' ? 'rescheduled' : 'pending';
      
      console.log(`[RescheduleJob] Changing slot for job ${id}: status ${job.status} -> ${statusToKeep}, retryCount: ${job.retryCount || 0}`);

      // Update job with new time while PRESERVING retryCount and appropriate status
      const updatedJob = await prisma.emailJob.update({
        where: { id: parseInt(id) },
        data: {
          scheduledFor: newDate,
          status: statusToKeep,  // Preserve 'rescheduled' status if applicable
          retryCount: job.retryCount || 0,  // CRITICAL: Preserve retryCount!
          metadata: {
            ...job.metadata,
            changedByUser: true,
            slotChangedAt: new Date(),
            previousScheduledFor: oldScheduledFor,
            previousStatus: job.status  // Keep track of what the status was
          }
        }
      });

      // Re-queue with new delay
      const delay = Math.max(0, newDate.getTime() - Date.now());
      const queueJob = await emailSendQueue.add('send-email', {
        emailJobId: updatedJob.id,
        leadId: job.leadId,
        leadEmail: job.email,
        emailType: job.type
      }, {
        delay,
        jobId: `email-${updatedJob.id}-${Date.now()}`,
        removeOnComplete: 1000,
        removeOnFail: 5000
      });

      // Update metadata with new queue job ID
      await prisma.emailJob.update({
        where: { id: updatedJob.id },
        data: {
          metadata: { ...updatedJob.metadata, queueJobId: queueJob.id }
        }
      });

      // Update lead schedule
      const lead = await LeadRepository.findById(job.leadId, { include: { emailSchedule: true } });
      if (lead) {
        await LeadRepository.addEvent(lead.id, 'scheduled', {
          oldDate: oldScheduledFor,
          newDate: newDate,
          reason: 'Manual Drag & Drop',
          changedByUser: true
        }, job.type, job.id);
        
        // await LeadRepository.updateStatus(lead.id, `${job.type}:scheduled`); // Disabled to prevent status change on manual slot adjustment
        
        // Sync emailSchedule with new scheduled time
        if (lead.emailSchedule) {
          const isInitial = job.type?.toLowerCase().includes('initial');
          if (isInitial) {
            await prisma.emailSchedule.update({
              where: { id: lead.emailSchedule.id },
              data: { initialScheduledFor: newDate }
            });
          } else {
            let followups = lead.emailSchedule.followups || [];
            if (!Array.isArray(followups)) {
              try { followups = JSON.parse(followups); } catch(e) { followups = []; }
            }
            const idx = followups.findIndex(f => f.name === job.type);
            if (idx >= 0) {
              followups[idx].scheduledFor = newDate;
              await prisma.emailSchedule.update({
                where: { id: lead.emailSchedule.id },
                data: { followups }
              });
            }
          }
        }
      }

      res.status(200).json({
        success: true,
        message: 'Job rescheduled successfully',
        job: {
          id: updatedJob.id,
          oldScheduledFor,
          newScheduledFor: newDate,
          status: updatedJob.status
        }
      });
    } catch (error) {
      console.error('Reschedule job error:', error);
      res.status(500).json({ error: error.message });
    }
  }
}

module.exports = new EmailJobController();
