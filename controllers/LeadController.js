// controllers/LeadController.js
const multer = require("multer");
const LeadImportService = require("../services/LeadImportService");
const EmailSchedulerService = require("../services/EmailSchedulerService");
const RulebookService = require("../services/RulebookService");
const { LeadRepository, EmailJobRepository } = require("../repositories");
const { prisma } = require("../lib/prisma");

// Configure multer for file upload
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB limit
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      "text/csv",
      "application/vnd.ms-excel",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ];
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error("Invalid file type. Only CSV and XLSX are allowed."));
    }
  },
});

class LeadController {
  async uploadLeads(req, res) {
    try {
      if (!req.file) {
        return res
          .status(400)
          .json({ error: 'No file uploaded. Ensure form-data key is "file".' });
      }

      const fileType = req.file.mimetype === "text/csv" ? "csv" : "xlsx";
      const results = await LeadImportService.importLeads(
        req.file.buffer,
        fileType,
      );

      // Auto-schedule emails for the newly imported leads
      let schedulingResults = { scheduled: 0, failed: 0, errors: [] };
      if (results.leads && results.leads.length > 0) {
        try {
          // Extract IDs from the lead objects returned by importLeads
          const leadIds = results.leads.map((l) => l.id.toString());
          schedulingResults =
            await EmailSchedulerService.scheduleEmailsForLeads(leadIds);
        } catch (scheduleError) {
          console.error("Auto-scheduling failed:", scheduleError);
          // We don't fail the whole request, but we report the error
          schedulingResults.errors.push({
            error: "Auto-scheduling failed",
            details: scheduleError.message,
          });
        }
      }

      res.status(200).json({
        message: "Lead import and scheduling completed",
        import: {
          success: results.success,
          failed: results.failed,
          errors: results.errors.slice(0, 10),
        },
        scheduling: schedulingResults,
      });
    } catch (error) {
      console.error("Upload leads error:", error);
      res.status(500).json({ error: error.message });
    }
  }

  async scheduleEmails(req, res) {
    try {
      const { leadIds, filter } = req.body;

      let leads;
      if (leadIds && leadIds.length > 0) {
        leads = await prisma.lead.findMany({
          where: {
            id: { in: leadIds.map((id) => parseInt(id)) },
            status: "pending",
          },
        });
      } else if (filter) {
        leads = await prisma.lead.findMany({
          where: { status: "pending", ...filter },
        });
      } else {
        leads = await prisma.lead.findMany({
          where: { status: "pending" },
        });
      }

      if (leads.length === 0) {
        return res.status(400).json({ error: "No pending leads found" });
      }

      const leadIdsToSchedule = leads.map((lead) => lead.id.toString());
      const results =
        await EmailSchedulerService.scheduleEmailsForLeads(leadIdsToSchedule);

      res.status(200).json({
        message: "Email scheduling completed",
        results: {
          scheduled: results.scheduled,
          failed: results.failed,
          errors: results.errors,
        },
      });
    } catch (error) {
      console.error("Schedule emails error:", error);
      res.status(500).json({ error: error.message });
    }
  }

  async getLeads(req, res) {
    try {
      const { status, tags, page = 1, limit = 50 } = req.query;
      const sortBy = req.query.sortBy || "createdAt";

      // Cap limit at 100 to prevent excessive data fetching
      const cappedLimit = Math.min(parseInt(limit) || 50, 100);

      // Use repository method which handles filtering internally
      const result = await LeadRepository.findMany({
        page: parseInt(page),
        limit: cappedLimit,
        status,
        tags,
        sortBy,
        sortOrder: sortBy === "name" ? "asc" : "desc",
      });

      res.status(200).json(result);
    } catch (error) {
      console.error("Get leads error:", error);
      res.status(500).json({ error: error.message });
    }
  }

  async freezeLead(req, res) {
    try {
      const { id } = req.params;
      const { hours, resumeAfter } = req.body;

      if (!id) return res.status(400).json({ error: "Lead ID is required" });

      const lead = await EmailSchedulerService.freezeLead(
        id,
        hours,
        resumeAfter,
      );
      res.status(200).json({ message: "Lead frozen successfully", lead });
    } catch (error) {
      console.error("Freeze lead error:", error);
      res.status(500).json({ error: error.message });
    }
  }

  async unfreezeLead(req, res) {
    try {
      const { id } = req.params;
      if (!id) return res.status(400).json({ error: "Lead ID is required" });

      const lead = await EmailSchedulerService.unfreezeLead(id);
      res.status(200).json({ message: "Lead unfrozen successfully", lead });
    } catch (error) {
      console.error("Unfreeze lead error:", error);
      res.status(500).json({ error: error.message });
    }
  }

  async convertLead(req, res) {
    try {
      const { id } = req.params;
      if (!id) return res.status(400).json({ error: "Lead ID is required" });

      const lead = await EmailSchedulerService.convertLead(id);
      res.status(200).json({ message: "Lead marked as converted", lead });
    } catch (error) {
      console.error("Convert lead error:", error);
      res.status(500).json({ error: error.message });
    }
  }

  async updateLead(req, res) {
    try {
      const { id } = req.params;
      const { name, email, country, city } = req.body;

      const lead = await LeadRepository.findById(id);
      if (!lead) return res.status(404).json({ error: "Lead not found" });

      const updates = {};
      if (name) updates.name = name;
      if (email) updates.email = email.toLowerCase().trim();

      let locationChanged = false;
      if (country && country !== lead.country) {
        updates.country = country;
        locationChanged = true;
      }
      if (city && city !== lead.city) {
        updates.city = city;
        locationChanged = true;
      }

      if (locationChanged) {
        const TimezoneService = require("../services/TimezoneService");
        const newTimezone = TimezoneService.getTimezone(
          updates.country || lead.country,
          updates.city || lead.city,
        );
        if (newTimezone) updates.timezone = newTimezone;
      }

      const updatedLead = await LeadRepository.update(id, updates);
      res
        .status(200)
        .json({ message: "Lead updated successfully", lead: updatedLead });
    } catch (error) {
      console.error("Update lead error:", error);
      res.status(500).json({ error: error.message });
    }
  }

  async getAvailableSlots(req, res) {
    try {
      const { id } = req.params;
      const slots = await EmailSchedulerService.getAvailableSlots(id);
      res.status(200).json(slots);
    } catch (error) {
      console.error("Get slots error:", error);
      res.status(500).json({ error: error.message });
    }
  }

  async scheduleManualSlot(req, res) {
    try {
      const { id } = req.params;
      const { time, emailType, title, templateId, emailBody } = req.body;
      const job = await EmailSchedulerService.scheduleManualSlot(
        id,
        time,
        emailType,
        title,
        templateId,
        emailBody,
      );
      res.status(200).json({ message: "Email scheduled manually", job });
    } catch (error) {
      console.error("Manual schedule error:", error);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Retry a failed lead - reset status and reschedule next email
   */
  async retryLead(req, res) {
    try {
      const { id } = req.params;

      const lead = await LeadRepository.findById(id);
      if (!lead) {
        return res.status(404).json({ error: "Lead not found" });
      }

      const retriableStatuses = ["failed", "hard_bounce", "blocked", "spam"];
      const isRetriable = retriableStatuses.some(
        (s) => lead.status === s || lead.status?.toLowerCase().includes(s),
      );
      if (!isRetriable) {
        return res
          .status(400)
          .json({ error: `Lead status '${lead.status}' is not retriable` });
      }

      const previousStatus = lead.status;
      // Extract email type from previous status (e.g., "Initial Email:blocked" -> "Initial Email")
      const emailType = previousStatus?.split(":")[0] || "Initial Email";
      await LeadRepository.updateStatus(id, `${emailType}:rescheduled`);
      await LeadRepository.addEvent(
        id,
        "rescheduled",
        {
          reason: `Manual retry from ${previousStatus}`,
          previousStatus,
        },
        emailType,
      );

      const job = await EmailSchedulerService.scheduleNextEmail(
        id,
        "rescheduled",
      );
      const updatedLead = await LeadRepository.findById(id);

      if (job) {
        res.status(200).json({
          message: "Lead retry scheduled successfully",
          lead: updatedLead,
          job,
        });
      } else {
        res.status(200).json({
          message: "Lead status reset, but no more emails to schedule",
          lead: updatedLead,
        });
      }
    } catch (error) {
      console.error("Retry lead error:", error);
      res.status(500).json({ error: error.message });
    }
  }

  async pauseFollowups(req, res) {
    try {
      const { id } = req.params;
      const lead = await LeadRepository.findById(id, {
        include: { emailSchedule: true },
      });
      if (!lead) return res.status(404).json({ error: "Lead not found" });

      if (lead.followupsPaused) {
        return res
          .status(200)
          .json({ message: "Followups already paused", lead });
      }

      const { SettingsRepository } = require("../repositories");
      const settings = await SettingsRepository.getSettings();
      const followupNames = (settings?.followups || [])
        .filter((f) => f.enabled && !f.name.toLowerCase().includes("initial"))
        .map((f) => f.name);

      // Parse current followups from emailSchedule
      let followups = lead.emailSchedule?.followups || [];
      if (!Array.isArray(followups)) {
        try {
          followups = JSON.parse(followups);
        } catch (e) {
          followups = [];
        }
      }

      // Update only followup entries to paused (not initial, not manual)
      const pausedFollowups = [];
      const newEvents = [];

      for (const followup of followups) {
        const isFollowupType = followupNames.some(
          (fn) =>
            followup.name?.toLowerCase().includes(fn.toLowerCase()) ||
            fn.toLowerCase().includes(followup.name?.toLowerCase()),
        );

        if (
          isFollowupType &&
          !RulebookService.getSuccessfullySentStatuses().includes(
            followup.status,
          )
        ) {
          followup.status = "paused";
          pausedFollowups.push(followup.name);
          newEvents.push({
            leadId: parseInt(id),
            event: "paused",
            timestamp: new Date(),
            details: { reason: "User paused followups" },
            emailType: followup.name,
          });
        }
      }

      // ONLY cancel FOLLOWUP type email jobs (not initial, not manual)
      const followupJobs = await prisma.emailJob.findMany({
        where: {
          leadId: parseInt(id),
          status: { in: RulebookService.getActiveStatuses() },
          type: { in: followupNames },
        },
      });

      // Cancel from BullMQ queue
      const { emailSendQueue } = require("../queues/emailQueues");
      for (const job of followupJobs) {
        try {
          if (job.metadata?.queueJobId) {
            const bullJob = await emailSendQueue.getJob(
              job.metadata.queueJobId,
            );
            if (bullJob) await bullJob.remove();
          }
        } catch (e) {
          console.log(`Could not remove BullMQ job: ${e.message}`);
        }
      }

      // Update followup jobs in database
      await prisma.emailJob.updateMany({
        where: {
          leadId: parseInt(id),
          status: { in: RulebookService.getActiveStatuses() },
          type: { in: followupNames },
        },
        data: {
          status: "paused",
          lastError: "Followups paused by user",
          cancellationReason: "user_paused",
        },
      });

      // Update lead and emailSchedule
      await prisma.lead.update({
        where: { id: parseInt(id) },
        data: {
          followupsPaused: true,
          emailSchedule: {
            upsert: {
              create: { followups },
              update: { followups },
            },
          },
        },
      });

      // Create event history
      if (newEvents.length > 0) {
        await prisma.eventHistory.createMany({ data: newEvents });
      }

      // Recalculate lead status
      const StatusUpdateService = require("../services/StatusUpdateService");
      await StatusUpdateService._recalculateStatus(id, "followups_paused");

      // CRITICAL: Use RulebookService to ensure lead status is 100% accurate
      await RulebookService.syncLeadStatusAfterJobChange(
        id,
        "followups_paused",
      );

      const updatedLead = await LeadRepository.findById(id, {
        include: { emailSchedule: true, eventHistory: true, emailJobs: true },
      });
      res
        .status(200)
        .json({
          message: `Paused ${pausedFollowups.length} followups`,
          lead: updatedLead,
        });
    } catch (err) {
      console.error("Pause followups error:", err);
      res.status(500).json({ error: err.message });
    }
  }

  async resumeFollowups(req, res) {
    try {
      const { id } = req.params;
      const lead = await LeadRepository.findById(id, {
        include: { emailSchedule: true, emailJobs: true },
      });
      if (!lead) return res.status(404).json({ error: "Lead not found" });

      const { SettingsRepository } = require("../repositories");
      const settings = await SettingsRepository.getSettings();
      const followupSettings = (settings?.followups || []).filter(
        (f) => f.enabled && !f.name.toLowerCase().includes("initial"),
      );
      const followupNames = followupSettings.map((f) => f.name);

      // Check for paused EmailJob records even if followupsPaused flag is false
      const pausedEmailJobs = await prisma.emailJob.findMany({
        where: {
          leadId: parseInt(id),
          status: "paused",
          type: { in: followupNames },
        },
      });

      // If neither the flag is set NOR there are paused jobs, nothing to resume
      if (!lead.followupsPaused && pausedEmailJobs.length === 0) {
        return res
          .status(200)
          .json({ message: "Followups are not paused", lead });
      }

      console.log(
        `[Resume] Found ${pausedEmailJobs.length} paused jobs, followupsPaused=${lead.followupsPaused}`,
      );

      // Parse current followups from emailSchedule
      let followups = lead.emailSchedule?.followups || [];
      if (!Array.isArray(followups)) {
        try {
          followups = JSON.parse(followups);
        } catch (e) {
          followups = [];
        }
      }

      const newEvents = [];

      // FIRST: Check if ANY initial email has been delivered
      // If so, we can proceed to schedule followups regardless of other pending initials
      const deliveredInitialJob = await prisma.emailJob.findFirst({
        where: {
          leadId: parseInt(id),
          type: { contains: "initial", mode: "insensitive" },
          status: { in: RulebookService.getSuccessfullySentStatuses() },
        },
      });

      // Only check for pending initial if NO initial has been delivered yet
      if (!deliveredInitialJob) {
        const pendingInitialJob = await prisma.emailJob.findFirst({
          where: {
            leadId: parseInt(id),
            type: { contains: "initial", mode: "insensitive" },
            status: { in: RulebookService.getActiveStatuses() },
          },
        });

        if (pendingInitialJob) {
          // Initial email is still pending - just mark followups as pending but don't schedule yet
          for (const followup of followups) {
            if (followup.status === "paused") {
              followup.status = "pending";
              newEvents.push({
                leadId: parseInt(id),
                event: "resumed",
                timestamp: new Date(),
                details: {
                  reason:
                    "Followups resumed - will schedule after initial delivers",
                },
                emailType: followup.name,
              });
            }
          }

          // Update lead without scheduling
          await prisma.lead.update({
            where: { id: parseInt(id) },
            data: {
              followupsPaused: false,
              emailSchedule: {
                upsert: { create: { followups }, update: { followups } },
              },
            },
          });

          if (newEvents.length > 0) {
            await prisma.eventHistory.createMany({ data: newEvents });
          }

          const updatedLead = await LeadRepository.findById(id, {
            include: {
              emailSchedule: true,
              eventHistory: true,
              emailJobs: true,
            },
          });
          return res.status(200).json({
            message:
              "Followups resumed - will schedule after initial email is delivered",
            lead: updatedLead,
          });
        }
      }

      console.log(
        `[Resume] Initial delivered (id: ${deliveredInitialJob?.id}), proceeding to schedule followups for lead ${id}`,
      );

      // CHECK FOR EXISTING CONDITIONAL EMAILS
      // If a conditional email is already scheduled, do NOT resume followups
      // Keep followupsPaused = true and return an informative error
      const existingConditionalJob = await prisma.emailJob.findFirst({
        where: {
          leadId: parseInt(id),
          type: { startsWith: "conditional:" },
          status: { in: RulebookService.getActiveStatuses() },
        },
      });

      if (existingConditionalJob) {
        console.log(
          `[Resume] Blocked by conditional email (${existingConditionalJob.type}), cannot resume followups`,
        );

        // DO NOT change followupsPaused flag - keep it as is
        // Return a conflict response so the frontend knows followups weren't resumed
        return res.status(200).json({
          message: `Cannot resume: ${existingConditionalJob.type.replace('conditional:', '')} is scheduled. Followups will auto-resume when it completes.`,
          blocked: true,
          blockingJob: {
            id: existingConditionalJob.id,
            type: existingConditionalJob.type,
            scheduledFor: existingConditionalJob.scheduledFor
          },
          lead: await LeadRepository.findById(id, {
            include: { emailSchedule: true, eventHistory: true, emailJobs: true },
          }),
        });
      }

      // Find last delivered email to calculate delay from
      const lastDeliveredJob = await prisma.emailJob.findFirst({
        where: {
          leadId: parseInt(id),
          status: { in: ["delivered", "opened", "clicked", "sent"] },
        },
        orderBy: { sentAt: "desc" },
      });

      const baseDate =
        lastDeliveredJob?.sentAt ||
        lastDeliveredJob?.scheduledFor ||
        new Date();

      // Resume paused followups and schedule them
      for (const followup of followups) {
        if (followup.status === "paused") {
          followup.status = "pending";
          newEvents.push({
            leadId: parseInt(id),
            event: "resumed",
            timestamp: new Date(),
            details: { reason: "User resumed followups" },
            emailType: followup.name,
          });
        }
      }

      // Update lead
      await prisma.lead.update({
        where: { id: parseInt(id) },
        data: {
          followupsPaused: false,
          emailSchedule: {
            upsert: { create: { followups }, update: { followups } },
          },
        },
      });

      // Create event history
      if (newEvents.length > 0) {
        await prisma.eventHistory.createMany({ data: newEvents });
      }

      // Delete paused EmailJob records so scheduleNextEmail can create fresh ones
      // This ensures proper timing calculation from the last delivered email
      const deletedPausedJobs = await prisma.emailJob.deleteMany({
        where: {
          leadId: parseInt(id),
          status: "paused",
          type: { in: followupNames },
        },
      });

      if (deletedPausedJobs.count > 0) {
        console.log(
          `[Resume] Deleted ${deletedPausedJobs.count} paused EmailJob records for lead ${id}`,
        );
      }

      // Now schedule the next followup using EmailSchedulerService
      // This will calculate proper delays based on configuration
      const scheduledJob = await EmailSchedulerService.scheduleNextEmail(id);

      if (scheduledJob) {
        console.log(`[Resume] Scheduled: ${scheduledJob.type} for lead ${id}`);
      }

      // Recalculate lead status
      const StatusUpdateService = require("../services/StatusUpdateService");
      await StatusUpdateService._recalculateStatus(id, "followups_resumed");

      // CRITICAL: Use RulebookService to ensure lead status is 100% accurate
      await RulebookService.syncLeadStatusAfterJobChange(
        id,
        "followups_resumed",
      );

      const updatedLead = await LeadRepository.findById(id, {
        include: { emailSchedule: true, eventHistory: true, emailJobs: true },
      });
      res
        .status(200)
        .json({
          message: "Followups resumed and scheduled",
          lead: updatedLead,
        });
    } catch (err) {
      console.error("Resume followups error:", err);
      res.status(500).json({ error: err.message });
    }
  }

  async skipFollowup(req, res) {
    try {
      const { id } = req.params;
      const { stepName } = req.body;

      const lead = await LeadRepository.findById(id, {
        include: { emailSchedule: true },
      });
      if (!lead) return res.status(404).json({ error: "Lead not found" });

      // RULEBOOK VALIDATION: Check if this mail type can be skipped
      const validation = RulebookService.validateAction('skip', stepName, 'pending');
      if (!validation.allowed) {
        return res.status(400).json({ 
          error: validation.reason,
          code: 'ACTION_NOT_ALLOWED'
        });
      }

      const skippedFollowups = lead.skippedFollowups || [];
      if (!skippedFollowups.includes(stepName)) {
        skippedFollowups.push(stepName);
      }

      // Get followups from emailSchedule
      let followups = lead.emailSchedule?.followups || [];
      if (!Array.isArray(followups)) {
        try {
          followups = JSON.parse(followups);
        } catch (e) {
          followups = [];
        }
      }

      // Update the skipped followup status
      if (Array.isArray(followups)) {
        const idx = followups.findIndex((f) => f.name === stepName);
        if (idx > -1) {
          followups[idx].status = "skipped";
        }
      }

      // Update lead with proper Prisma relation syntax
      await prisma.lead.update({
        where: { id: parseInt(id) },
        data: {
          skippedFollowups,
          emailSchedule: {
            upsert: {
              create: { followups },
              update: { followups },
            },
          },
        },
      });

      // Create event history entry
      await prisma.eventHistory.create({
        data: {
          leadId: parseInt(id),
          event: "skipped",
          timestamp: new Date(),
          details: { reason: "User skipped followup" },
          emailType: stepName,
        },
      });
      // Update ANY pending/paused job to 'skipped' - including jobs that were 'paused' previously
      await prisma.emailJob.updateMany({
        where: {
          leadId: parseInt(id),
          type: stepName,
          status: { in: RulebookService.getCancellableStatuses() },
        },
        data: { status: "skipped", lastError: "Skipped by user" },
      });

      await EmailSchedulerService.scheduleNextEmail(id);

      // CRITICAL: Use RulebookService to ensure lead status is 100% accurate
      await RulebookService.syncLeadStatusAfterJobChange(
        id,
        "followup_skipped",
      );

      const updatedLead = await LeadRepository.findById(id, {
        include: { emailSchedule: true, eventHistory: true },
      });
      res
        .status(200)
        .json({ message: `Skipped ${stepName}`, lead: updatedLead });
    } catch (err) {
      console.error("Skip followup error:", err);
      res.status(500).json({ error: err.message });
    }
  }

  async revertSkipFollowup(req, res) {
    try {
      const { id } = req.params;
      const { stepName } = req.body;

      const lead = await LeadRepository.findById(id, {
        include: { emailSchedule: true },
      });
      if (!lead) return res.status(404).json({ error: "Lead not found" });

      if (!lead.skippedFollowups)
        return res.status(200).json({ message: "No skipped followups", lead });

      const skippedFollowups = lead.skippedFollowups.filter(
        (s) => s !== stepName,
      );

      const { SettingsRepository: SR } = require("../repositories");
      const settings = await SR.getSettings();
      const allFollowups =
        settings?.followups
          ?.filter((f) => f.enabled)
          .sort((a, b) => a.order - b.order) || [];

      const revertedStepOrder =
        allFollowups.find((f) => f.name === stepName)?.order || 0;
      const followupsToCancel = allFollowups
        .filter((f) => f.order > revertedStepOrder)
        .map((f) => f.name);

      if (followupsToCancel.length > 0) {
        // DELETE all jobs for later steps (active, paused, pending) - not just active
        // This allows them to be naturally scheduled later in proper order
        const jobsToDelete = await prisma.emailJob.findMany({
          where: {
            leadId: parseInt(id),
            type: { in: followupsToCancel },
            status: { in: [...RulebookService.getActiveStatuses(), 'paused'] },
          },
        });

        // Remove from BullMQ queue first
        const { emailSendQueue } = require("../queues/emailQueues");
        for (const job of jobsToDelete) {
          if (job.metadata?.queueJobId) {
            try {
              const bullJob = await emailSendQueue.getJob(job.metadata.queueJobId);
              if (bullJob) await bullJob.remove();
              console.log(`[RevertSkip] Removed queue job for ${job.type}`);
            } catch (e) {
              console.log(`[RevertSkip] Could not remove queue job: ${e.message}`);
            }
          }
        }

        // Now delete from database
        await prisma.emailJob.deleteMany({
          where: {
            leadId: parseInt(id),
            type: { in: followupsToCancel },
            status: { in: [...RulebookService.getActiveStatuses(), 'paused'] },
          },
        });

        console.log(`[RevertSkip] Deleted ${jobsToDelete.length} jobs for later followups: ${followupsToCancel.join(', ')}`);
      }

      // Also delete ALL jobs for the reverted step itself (except completed ones)
      // This ensures a clean slate for re-scheduling
      const revertedStepJobs = await prisma.emailJob.deleteMany({
        where: {
          leadId: parseInt(id),
          type: stepName,
          status: { notIn: ["sent", "delivered", "opened", "clicked"] },
        },
      });
      
      if (revertedStepJobs.count > 0) {
        console.log(`[RevertSkip] Deleted ${revertedStepJobs.count} existing jobs for reverted step ${stepName}`);
      }

      // Get and update followups from emailSchedule
      let followups = lead.emailSchedule?.followups || [];
      if (!Array.isArray(followups)) {
        try {
          followups = JSON.parse(followups);
        } catch (e) {
          followups = [];
        }
      }

      if (Array.isArray(followups)) {
        // Remove entries for deleted jobs so they appear as upcoming projections
        for (const fname of followupsToCancel) {
          const fIdx = followups.findIndex((f) => f.name === fname);
          if (fIdx > -1) {
            followups.splice(fIdx, 1); // Remove entirely
          }
        }
        // Also remove the reverted step so it gets scheduled fresh
        const idx = followups.findIndex((f) => f.name === stepName);
        if (idx > -1) {
          followups.splice(idx, 1); // Remove entirely
        }
      }

      // PRIORITY CHECK: Check for higher priority blocking jobs (conditional, manual)
      // If a blocking job exists, don't schedule the reverted followup yet - pause it instead
      const blockingJob = await prisma.emailJob.findFirst({
        where: {
          leadId: parseInt(id),
          status: { in: RulebookService.getActiveStatuses() },
          OR: [
            { type: { contains: 'manual', mode: 'insensitive' } },
            { type: { startsWith: 'conditional:' } }
          ]
        }
      });

      let scheduledJob = null;
      let statusMessage = `Reverted skip for ${stepName}`;

      if (blockingJob) {
        // Higher priority job exists - mark followupsPaused so the reverted followup
        // gets scheduled when blocking job completes
        console.log(`[RevertSkip] Blocking job found: ${blockingJob.type}. Putting reverted followup in paused state.`);
        
        await prisma.lead.update({
          where: { id: parseInt(id) },
          data: {
            skippedFollowups,
            followupsPaused: true, // Mark as paused so it schedules when blocking job completes
            emailSchedule: {
              upsert: {
                create: { followups },
                update: { followups },
              },
            },
          },
        });

        statusMessage = `Reverted skip for ${stepName} - queued behind ${blockingJob.type}`;
      } else {
        // No blocking job - safe to schedule the reverted followup
        await prisma.lead.update({
          where: { id: parseInt(id) },
          data: {
            skippedFollowups,
            followupsPaused: false, // Ensure flag is cleared
            emailSchedule: {
              upsert: {
                create: { followups },
                update: { followups },
              },
            },
          },
        });

        scheduledJob = await EmailSchedulerService.scheduleNextEmail(id);
        if (scheduledJob) {
          console.log(`[RevertSkip] Scheduled: ${scheduledJob.type} for lead ${id}`);
        }
      }

      // Create event history entry
      await prisma.eventHistory.create({
        data: {
          leadId: parseInt(id),
          event: "revert_skipped",
          timestamp: new Date(),
          details: {
            reason: "User reverted skip",
            cancelledFollowups: followupsToCancel,
            blockingJob: blockingJob ? { id: blockingJob.id, type: blockingJob.type } : null,
            pausedUntilBlockingComplete: !!blockingJob,
          },
          emailType: stepName,
        },
      });

      // CRITICAL: Use RulebookService to ensure lead status is 100% accurate
      await RulebookService.syncLeadStatusAfterJobChange(
        id,
        "revert_skip",
      );

      const updatedLead = await LeadRepository.findById(id, {
        include: { emailSchedule: true, eventHistory: true },
      });
      res
        .status(200)
        .json({ message: statusMessage, lead: updatedLead });
    } catch (err) {
      console.error("Revert skip error:", err);
      res.status(500).json({ error: err.message });
    }
  }

  async deleteFollowupFromLead(req, res) {
    try {
      const { id, stepName } = req.params;

      const lead = await LeadRepository.findById(id, {
        include: { emailSchedule: true },
      });
      if (!lead) return res.status(404).json({ error: "Lead not found" });

      // Get followups and filter out the deleted one
      let followups = lead.emailSchedule?.followups || [];
      if (!Array.isArray(followups)) {
        try {
          followups = JSON.parse(followups);
        } catch (e) {
          followups = [];
        }
      }
      followups = followups.filter((f) => f.name !== stepName);

      const skippedFollowups = (lead.skippedFollowups || []).filter(
        (s) => s !== stepName,
      );

      // Update lead with proper Prisma relation syntax
      await prisma.lead.update({
        where: { id: parseInt(id) },
        data: {
          skippedFollowups,
          emailSchedule: {
            upsert: {
              create: { followups },
              update: { followups },
            },
          },
        },
      });

      // Create event history entry
      await prisma.eventHistory.create({
        data: {
          leadId: parseInt(id),
          event: "deleted_followup",
          timestamp: new Date(),
          details: {
            reason: "User permanently deleted followup from lead sequence",
          },
          emailType: stepName,
        },
      });

      await prisma.emailJob.updateMany({
        where: {
          leadId: parseInt(id),
          type: stepName,
          status: { in: RulebookService.getActiveStatuses() },
        },
        data: { status: "cancelled", lastError: "Followup deleted from lead" },
      });

      await EmailSchedulerService.scheduleNextEmail(id);

      const updatedLead = await LeadRepository.findById(id, {
        include: { emailSchedule: true, eventHistory: true },
      });
      res
        .status(200)
        .json({
          message: `Deleted ${stepName} from lead sequence`,
          lead: updatedLead,
        });
    } catch (err) {
      console.error("Delete followup from lead error:", err);
      res.status(500).json({ error: err.message });
    }
  }

  async deleteEmailJob(req, res) {
    try {
      const { id, jobId } = req.params;

      const lead = await LeadRepository.findById(id);
      if (!lead) return res.status(404).json({ error: "Lead not found" });

      await EmailSchedulerService.deleteEmailJob(id, jobId);

      const updatedLead = await LeadRepository.findById(id);
      res.status(200).json({ message: "Job deleted", lead: updatedLead });
    } catch (err) {
      console.error("Delete job error:", err);
      res.status(500).json({ error: err.message });
    }
  }
}

const leadController = new LeadController();

module.exports = {
  leadController,
  upload,
};
