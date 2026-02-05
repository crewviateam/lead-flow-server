// services/EmailSchedulerService.js
// Complete Prisma-based email scheduling service
// Uses RulebookService for centralized rules
// UPDATED: Now uses Redis-based distributed locks for multi-instance scalability

const moment = require("moment-timezone");
const { v4: uuidv4 } = require("uuid");
const { prisma } = require("../lib/prisma");
const {
  LeadRepository,
  EmailJobRepository,
  SettingsRepository,
} = require("../repositories");
const TimezoneService = require("./TimezoneService");
const RateLimitService = require("./RateLimitService");
const UniqueJourneyService = require("./UniqueJourneyService");
const RulebookService = require("./RulebookService");
const DistributedLockService = require("./DistributedLockService");
const { followupQueue } = require("../queues/emailQueues");
const EventBus = require("../events/EventBus");

// Store lock IDs for release (local cache for this instance)
const activeLocks = new Map();

class EmailSchedulerService {

  /**
   * Check if a date is a valid working day
   */
  isWorkingDay(dateTime, settings) {
    const dayOfWeek = dateTime.day();
    const weekendDays = settings.businessHours?.weekendDays || [0, 6];

    if (weekendDays.includes(dayOfWeek)) {
      return false;
    }

    const pausedDates = settings.pausedDates || [];
    if (pausedDates.length > 0) {
      const dateStr = dateTime.format("YYYY-MM-DD");
      const isPaused = pausedDates.some(
        (pd) => moment(pd).format("YYYY-MM-DD") === dateStr,
      );
      if (isPaused) return false;
    }

    return true;
  }

  /**
   * Get the next valid working day
   */
  getNextWorkingDay(dateTime, settings, startHour = 8) {
    let currentDate = dateTime.clone();
    let attempts = 0;

    while (attempts < 365) {
      if (this.isWorkingDay(currentDate, settings)) {
        return currentDate;
      }
      currentDate = currentDate.add(1, "day").startOf("day").hour(startHour);
      attempts++;
    }

    console.warn(
      "Scheduler: All days appear blocked. Returning original date.",
    );
    return dateTime;
  }

  /**
   * CORE FCFS SLOT ALLOCATION ALGORITHM
   * Finds the FIRST available slot that satisfies ALL constraints:
   * 1. On or after minTime (respects delays)
   * 2. Is a working day (not weekend, not paused date)
   * 3. Is within business hours
   * 4. Has available rate limit capacity
   *
   * @param {string} timezone - Lead's timezone
   * @param {Date} minTime - Minimum time (e.g., now + delay hours/days)
   * @param {Object} settings - Full settings object
   * @param {Object} options - Optional: { preferSmartSend: boolean }
   * @returns {Promise<{success: boolean, scheduledTime: Date, reason?: string}>}
   */
  async findNextAvailableSlot(timezone, minTime, settings, options = {}) {
    const businessHours = settings.businessHours || {
      startHour: 8,
      endHour: 18,
    };
    const windowMinutes = settings.rateLimit?.windowMinutes || 15;
    const maxAttempts = 200; // ~3 days worth of slots at 15min intervals

    // CRITICAL FIX: Never schedule in the past
    // Use the LATER of minTime or current time as the effective starting point
    const now = moment().tz(timezone);
    let effectiveMinTime = moment(minTime).tz(timezone);
    
    if (effectiveMinTime.isBefore(now)) {
      console.log(`[FCFS] minTime ${effectiveMinTime.format('HH:mm')} is in the past. Using current time ${now.format('HH:mm')} instead.`);
      effectiveMinTime = now.clone();
    }
    
    let currentTime = effectiveMinTime.clone();
    let attempts = 0;

    // Round to next window boundary
    const minutes = currentTime.minute();
    const roundedMinutes = Math.ceil(minutes / windowMinutes) * windowMinutes;
    currentTime.minute(roundedMinutes).second(0).millisecond(0);

    // If current time is before business hours start, jump to start
    if (currentTime.hour() < businessHours.startHour) {
      currentTime.hour(businessHours.startHour).minute(0);
    }

    // If current time is after business hours end, jump to next day
    if (currentTime.hour() >= businessHours.endHour) {
      currentTime.add(1, "day").hour(businessHours.startHour).minute(0);
    }

    while (attempts < maxAttempts) {
      attempts++;

      // CHECK 1: Working Day
      if (!this.isWorkingDay(currentTime, settings)) {
        // Move to next day at business start
        currentTime.add(1, "day").hour(businessHours.startHour).minute(0);
        continue;
      }

      // CHECK 2: Business Hours
      const hour = currentTime.hour();
      if (hour < businessHours.startHour) {
        currentTime.hour(businessHours.startHour).minute(0);
        continue;
      }
      if (hour >= businessHours.endHour) {
        currentTime.add(1, "day").hour(businessHours.startHour).minute(0);
        continue;
      }

      // CHECK 3: Rate Limit Capacity (DB-backed)
      const slotTime = currentTime.toDate();
      const capacity = await RateLimitService.getSlotCapacity(
        slotTime.getTime(),
      );

      if (capacity.available > 0) {
        // FOUND A VALID SLOT!
        console.log(
          `[FCFS] Found slot: ${currentTime.format("YYYY-MM-DD HH:mm")} (${capacity.available}/${capacity.total} available)`,
        );
        return {
          success: true,
          scheduledTime: slotTime,
          slotInfo: {
            available: capacity.available,
            total: capacity.total,
            windowStart: capacity.windowStart,
          },
        };
      }

      // Slot full, move to next window
      currentTime.add(windowMinutes, "minutes");

      // If we've passed business hours, jump to next day
      if (currentTime.hour() >= businessHours.endHour) {
        currentTime.add(1, "day").hour(businessHours.startHour).minute(0);
      }
    }

    // Exhausted attempts
    console.error(
      `[FCFS] Failed to find slot after ${maxAttempts} attempts from ${moment(minTime).format()}`,
    );
    return {
      success: false,
      reason: "No available slot found within search window",
    };
  }

  /**
   * Optimize send time to preferred engagement windows
   * PRIORITY ORDER:
   * 1. Keep scheduling on the SAME DAY within business hours (highest priority)
   * 2. Try to fit within smart send time windows if possible
   * 3. Only shift to next day when BUSINESS HOURS are exhausted, not just smart windows
   */
  getSmartSendTime(baseTime, leadTimezone, settings) {
    const smartSettings = settings.smartSendTime;
    const businessHours = settings.businessHours || {
      startHour: 8,
      endHour: 18,
    };

    // If smart send is disabled, return baseTime as-is
    if (!smartSettings || smartSettings.enabled !== true) {
      return baseTime;
    }

    const localMoment = moment(baseTime).tz(leadTimezone);
    const currentHour = localMoment.hour();

    const morning = smartSettings.morningWindow || {
      startHour: 9,
      endHour: 11,
    };
    const afternoon = smartSettings.afternoonWindow || {
      startHour: 14,
      endHour: 16,
    };
    const priority = smartSettings.priority || "morning";

    // If already within a smart send window, use baseTime
    if (currentHour >= morning.startHour && currentHour < morning.endHour)
      return baseTime;
    if (currentHour >= afternoon.startHour && currentHour < afternoon.endHour)
      return baseTime;

    // Determine smart window target based on priority and current time
    let smartWindowTarget = null;

    if (priority === "morning") {
      if (currentHour < morning.startHour) {
        smartWindowTarget = morning.startHour;
      } else if (
        currentHour >= morning.endHour &&
        currentHour < afternoon.startHour
      ) {
        smartWindowTarget = afternoon.startHour;
      }
      // If past afternoon.endHour, smartWindowTarget stays null (use fallback)
    } else if (priority === "afternoon") {
      if (currentHour < afternoon.startHour) {
        smartWindowTarget = afternoon.startHour;
      }
      // If past afternoon.endHour, smartWindowTarget stays null (use fallback)
    } else {
      // Default: try morning first, then afternoon
      if (currentHour < morning.startHour) {
        smartWindowTarget = morning.startHour;
      } else if (
        currentHour >= morning.endHour &&
        currentHour < afternoon.startHour
      ) {
        smartWindowTarget = afternoon.startHour;
      }
    }

    // If we found a smart window target on the same day, use it
    if (smartWindowTarget !== null) {
      return localMoment.hour(smartWindowTarget).minute(0).second(0).toDate();
    }

    // FALLBACK: Smart windows are passed, but we're still within business hours
    // PRIORITY 1: Stay on the same day within business hours (don't shift to next day)
    if (currentHour < businessHours.endHour) {
      // Use current time - let rate limiting handle the exact slot
      return baseTime;
    }

    // ONLY shift to next day when business hours are exhausted
    // Return next day at priority smart window start
    const nextDayHour =
      priority === "afternoon" ? afternoon.startHour : morning.startHour;
    return localMoment.add(1, "day").startOf("day").hour(nextDayHour).toDate();
  }

  /**
   * Schedule emails for a list of leads
   */
  async scheduleEmailsForLeads(leadIds) {
    console.log(`Scheduler: Processing ${leadIds.length} leads...`);
    const results = { scheduled: 0, failed: 0, errors: [] };

    for (const leadId of leadIds) {
      try {
        const job = await this.scheduleNextEmail(leadId);
        if (job) results.scheduled++;
      } catch (error) {
        console.error(`Scheduler: Error for lead ${leadId}:`, error);
        results.failed++;
        results.errors.push({ leadId, error: error.message });
      }
    }

    console.log(
      `Scheduler: Completed. Scheduled: ${results.scheduled}, Failed: ${results.failed}`,
    );
    return results;
  }

  /**
   * Determine and schedule the next email for a lead
   * PRIORITY-BASED CONDITIONAL SCHEDULING:
   * - Conditional followups (if_opened, if_clicked) wait until condition is met
   * - When condition is met, schedule with priority and cancel lower-priority pending jobs
   * - "Always send" followups are scheduled immediately after delivery
   */
  async scheduleNextEmail(leadId, customStatus = "pending") {
    // Use distributed lock for multi-instance scalability
    const lockKey = DistributedLockService.getLeadLockKey(leadId);
    const lockResult = await DistributedLockService.acquire(lockKey, 30000); // 30 second TTL
    
    if (!lockResult.acquired) {
      console.log(`Scheduler: Distributed lock held for ${leadId}. Skipping.`);
      return null;
    }
    
    // Store lock ID for release
    activeLocks.set(leadId.toString(), lockResult.lockId);

    try {
      const lead = await LeadRepository.findById(leadId);
      if (!lead) throw new Error(`Lead ${leadId} not found`);

      // CRITICAL: Ensure lead has a valid timezone - derive from country if missing
      if (!lead.timezone && lead.country) {
        const derivedTz = TimezoneService.getTimezone(lead.country, lead.city);
        lead.timezone = derivedTz || "UTC";
        console.log(`[Scheduler] Derived timezone ${lead.timezone} for ${lead.email} from country ${lead.country}`);
      } else if (!lead.timezone) {
        lead.timezone = "UTC";
        console.log(`[Scheduler] No timezone for ${lead.email}, defaulting to UTC`);
      }

      // ==========================================
      // CRITICAL SAFETY CHECK: Terminal State Guard (PRIMARY)
      // This is the FIRST and MOST IMPORTANT check
      // No scheduling should EVER proceed if lead is in terminal state
      // ==========================================
      if (lead.terminalState) {
        console.log(`Scheduler: ⛔ Lead ${lead.email} is in terminal state (${lead.terminalState}). BLOCKING.`);
        return null;
      }
      
      // Block if lead is in failure state (requires manual intervention)
      if (lead.isInFailure) {
        console.log(`Scheduler: ⛔ Lead ${lead.email} is in failure state. Manual retry required. BLOCKING.`);
        return null;
      }
      // ==========================================

      // Safety guard for terminal statuses (SECONDARY - backup check via status string)
      const terminalStates = [
        "failed",
        "hard_bounce",
        "blocked",
        "spam",
        "unsubscribed",
      ];
      const currentStatus = lead.status?.includes(":")
        ? lead.status.split(":")[1]
        : lead.status;
      if (
        terminalStates.includes(currentStatus) &&
        customStatus === "pending"
      ) {
        console.log(
          `Scheduler: Lead ${lead.email} has terminal status. Skipping.`,
        );
        return null;
      }

      // Check for active manual mails
      const hasActiveManualMail = lead.manualMails?.some((m) =>
        ["pending", "queued"].includes(m.status),
      );
      if (hasActiveManualMail) {
        console.log(
          `Scheduler: Lead ${lead.email} has active MANUAL mail. Pausing.`,
        );
        return null;
      }

      if (lead.followupsPaused) {
        console.log(`Scheduler: Followups PAUSED for ${lead.email}. Skipping.`);
        return null;
      }

      const settings = await SettingsRepository.getSettings();
      if (!settings.followups || settings.followups.length === 0) {
        throw new Error("No email sequence configured");
      }

      // Get sequence (exclude skipped)
      const sequence = settings.followups
        .filter(
          (f) =>
            f.enabled &&
            !f.globallySkipped &&
            (!lead.skippedFollowups || !lead.skippedFollowups.includes(f.name)),
        )
        .sort((a, b) => a.order - b.order);

      if (sequence.length === 0) {
        console.log(`Scheduler: No enabled emails for ${lead.email}`);
        return null;
      }

      // OPTIMIZATION: Batch fetch all jobs for this lead in ONE query
      const allJobsForLead = await prisma.emailJob.findMany({
        where: { leadId: parseInt(leadId) },
        select: { id: true, type: true, status: true, condition: true },
      });

      // Create lookup maps for O(1) access
      const completedStatuses = ["sent", "delivered", "opened", "clicked"];
      const completedTypes = new Set();
      const pendingJobs = new Map(); // type -> job info
      const jobStatuses = new Map(); // type -> status (for condition checking)
      const skippedTypes = new Set();

      // Track Initial Email specifically
      let initialEmailExists = false;
      let initialEmailStatus = null;

      for (const job of allJobsForLead) {
        // Check for any Initial Email variant
        const isInitial = job.type?.toLowerCase().includes("initial");

        if (completedStatuses.includes(job.status)) {
          completedTypes.add(job.type);
          jobStatuses.set(job.type, job.status);
          if (isInitial) {
            initialEmailExists = true;
            initialEmailStatus = job.status;
          }
        } else if (
          ["pending", "queued", "rescheduled", "scheduled"].includes(job.status)
        ) {
          pendingJobs.set(job.type, job);
          if (isInitial) {
            initialEmailExists = true;
            initialEmailStatus = job.status;
          }
        } else if (job.status === "skipped") {
          skippedTypes.add(job.type);
        }
        // Always store latest status for condition checking
        if (
          !jobStatuses.has(job.type) ||
          completedStatuses.includes(job.status)
        ) {
          jobStatuses.set(job.type, job.status);
        }
      }

      // GUARD 1: INITIAL EMAIL UNIQUENESS
      // If Initial Email already exists (any status except cancelled/failed), mark it as completed
      if (initialEmailExists && initialEmailStatus) {
        console.log(
          `[Scheduler] Initial Email already exists with status '${initialEmailStatus}' for ${lead.email}`,
        );
        // Add to completedTypes so algorithm skips it
        const initialStep = sequence.find((s) =>
          s.name?.toLowerCase().includes("initial"),
        );
        if (initialStep) {
          completedTypes.add(initialStep.name);
        }
      }

      // GUARD 2: Only schedule ONE followup at a time
      // If there's already ANY pending followup (not Initial Email), don't schedule another
      const pendingFollowups = Array.from(pendingJobs.entries()).filter(
        ([type, job]) =>
          !type.toLowerCase().includes("initial") &&
          !type.toLowerCase().startsWith("conditional:") &&
          !type.toLowerCase().startsWith("manual"),
      );

      if (pendingFollowups.length > 0) {
        console.log(
          `[Scheduler] ⏸ Already has pending followup: ${pendingFollowups[0][0]} for ${lead.email}. Not scheduling another.`,
        );
        return null;
      }

      // PRIORITY-BASED CONDITIONAL SCHEDULING ALGORITHM
      // 1. Find all steps that are not completed
      // 2. For each step, evaluate if condition is met RIGHT NOW
      // 3. Schedule the FIRST step whose condition is met (giving priority to conditional followups)
      // 4. If a conditional followup is scheduled, cancel any pending "always" followups that come after it

      let stepToSchedule = null;
      let stepCondition = null;
      let pendingAlwaysJobsToCancel = [];

      for (let i = 0; i < sequence.length; i++) {
        const step = sequence[i];

        // Skip if already completed or skipped
        if (completedTypes.has(step.name) || skippedTypes.has(step.name)) {
          continue;
        }

        // Resolve condition checkStep to actual step name
        let condition = step.condition ? { ...step.condition } : null;
        if (
          condition &&
          (condition.checkStep === "previous" || !condition.checkStep)
        ) {
          if (i > 0) {
            condition.checkStep = sequence[i - 1].name;
          }
        }

        // Check if this step already has a pending job
        if (pendingJobs.has(step.name)) {
          const pendingJob = pendingJobs.get(step.name);
          const jobCondition = pendingJob.condition;

          // Track "always" jobs that might need to be cancelled if a conditional takes priority
          if (
            !jobCondition ||
            !jobCondition.type ||
            jobCondition.type === "always"
          ) {
            pendingAlwaysJobsToCancel.push({ step, job: pendingJob, index: i });
          }

          console.log(
            `[Scheduler] ${step.name} already pending for ${lead.email}`,
          );
          continue;
        }

        // Evaluate if this step's condition is met
        const conditionResult = await this._evaluateCondition(
          condition,
          jobStatuses,
          leadId,
        );

        if (conditionResult === "met" || conditionResult === "always") {
          // Condition is met - this step can be scheduled!
          stepToSchedule = step;
          stepCondition = condition;

          console.log(
            `[Scheduler] ✓ ${step.name} condition '${condition?.type || "always"}' is MET for ${lead.email}`,
          );
          break;
        } else if (conditionResult === "failed") {
          // Condition explicitly FAILED (e.g., if_not_opened but email WAS opened)
          // Mark as skipped and continue to next step
          console.log(
            `[Scheduler] ✗ ${step.name} condition '${condition?.type}' FAILED for ${lead.email} - marking as skipped`,
          );

          // Determine category for skipped job
          let category = 'followup';
          const typeLower = step.name.toLowerCase();
          if (typeLower.includes('initial')) {
            category = 'initial';
          } else if (typeLower.startsWith('conditional:') || typeLower.startsWith('conditional ')) {
            category = 'conditional';
          }

          // Create a skipped job record so we don't re-evaluate
          await prisma.emailJob.create({
            data: {
              leadId: parseInt(leadId),
              email: lead.email,
              type: step.name,
              category,
              status: "skipped",
              lastError: `Condition ${condition?.type} on ${condition?.checkStep} not met`,
              scheduledFor: new Date(),
              templateId: step.templateId || null,
              condition: condition,
            },
          });
          continue;
        } else if (conditionResult === "waiting") {
          // Condition not yet determinable (waiting for event like opened/clicked)
          // Don't schedule yet, wait for the condition to be triggered by an event
          console.log(
            `[Scheduler] ⏳ ${step.name} waiting for '${condition?.type}' on ${condition?.checkStep} for ${lead.email}`,
          );
          continue;
        }
      }

      if (!stepToSchedule) {
        // Check if all steps are completed/skipped/pending
        const allHandled = sequence.every(
          (step) =>
            completedTypes.has(step.name) ||
            skippedTypes.has(step.name) ||
            pendingJobs.has(step.name),
        );

        if (allHandled && pendingJobs.size === 0) {
          console.log(`Scheduler: All emails completed for ${lead.email}`);
          await LeadRepository.updateStatus(leadId, "sequence_complete");
        } else if (pendingJobs.size > 0) {
          console.log(
            `Scheduler: Steps pending, no new scheduling needed for ${lead.email}`,
          );
        } else {
          console.log(
            `Scheduler: Waiting for conditions to be met for ${lead.email}`,
          );
        }
        return null;
      }

      // PRIORITY HANDLING: If we're scheduling a conditional followup,
      // cancel any pending "always send" followups that come AFTER this step
      const stepIndex = sequence.findIndex(
        (s) => s.name === stepToSchedule.name,
      );

      if (
        stepCondition &&
        stepCondition.type &&
        stepCondition.type !== "always"
      ) {
        for (const pending of pendingAlwaysJobsToCancel) {
          if (pending.index > stepIndex) {
            // This is an "always" job that comes after our conditional job
            // Cancel it - the conditional job takes priority
            console.log(
              `[Scheduler] 🚫 Cancelling lower-priority job ${pending.job.id} (${pending.step.name}) - ${stepToSchedule.name} takes priority`,
            );

            await prisma.emailJob.update({
              where: { id: pending.job.id },
              data: {
                status: "cancelled",
                lastError: `Cancelled: conditional followup ${stepToSchedule.name} takes priority`,
              },
            });
          }
        }
      }

      // Find latest delivered mail for timing
      const latestDelivered = await prisma.emailJob.findFirst({
        where: {
          leadId: parseInt(leadId),
          status: { in: ["sent", "delivered", "opened", "clicked"] },
        },
        orderBy: [{ sentAt: "desc" }, { createdAt: "desc" }],
      });

      let baseTime;
      if (latestDelivered) {
        baseTime =
          latestDelivered.sentAt ||
          latestDelivered.deliveredAt ||
          latestDelivered.scheduledFor ||
          new Date();
      } else {
        baseTime = new Date();
      }

      // FCFS SLOT ALLOCATION
      // Calculate the minimum time (baseTime + delay), then find first available slot
      // CRITICAL: All time calculations must be in the LEAD'S TIMEZONE
      const delayDays = stepToSchedule.delayDays || 0;
      const leadTimezone = lead.timezone || "UTC";

      // Calculate minTime in the LEAD'S timezone, not server timezone
      let minTime = moment(baseTime).tz(leadTimezone).add(delayDays, "days");

      // If delay results in past time (in lead's timezone), start from now in lead's timezone
      const nowInLeadTz = moment().tz(leadTimezone);
      if (minTime.isBefore(nowInLeadTz)) {
        minTime = nowInLeadTz.clone();
      }

      // Start at beginning of business hours in LEAD'S timezone (FCFS)
      const startHour = settings.businessHours?.startHour || 8;
      minTime.hour(startHour).minute(0).second(0);

      console.log(`[Scheduler] Lead ${lead.email} (${leadTimezone}): min time = ${minTime.format('YYYY-MM-DD HH:mm')} local`);

      // Use unified FCFS slot finder
      const slotResult = await this.findNextAvailableSlot(
        leadTimezone,
        minTime.toDate(),
        settings,
      );

      if (!slotResult.success) {
        console.error(
          `Scheduler: Failed to find slot for ${stepToSchedule.name}: ${slotResult.reason}`,
        );
        throw new Error(`Failed to find scheduling slot: ${slotResult.reason}`);
      }

      const targetTime = slotResult.scheduledTime;

      const schedulerSettings = {
        businessHours: settings.businessHours,
        windowMinutes: settings.rateLimit?.windowMinutes || 15,
      };

      console.log(
        `Scheduler: 📧 Scheduling '${stepToSchedule.name}' for ${lead.email} at ${moment(targetTime).tz(lead.timezone).format("YYYY-MM-DD HH:mm")} ${lead.timezone}`,
      );
      return await this.scheduleEmailJob(
        lead,
        stepToSchedule.name,
        targetTime,
        schedulerSettings,
        customStatus,
        0,
        stepToSchedule.templateId,
        stepCondition,
      );
    } finally {
      // Release distributed lock
      const lockId = activeLocks.get(leadId.toString());
      if (lockId) {
        await DistributedLockService.release(lockKey, lockId);
        activeLocks.delete(leadId.toString());
      }
    }
  }

  /**
   * Evaluate if a condition is met for a lead
   * Returns: 'met', 'failed', 'waiting', or 'always'
   */
  async _evaluateCondition(condition, jobStatuses, leadId) {
    // No condition or "always" - always met
    if (!condition || !condition.type || condition.type === "always") {
      return "always";
    }

    const checkStepName = condition.checkStep;
    if (!checkStepName) {
      return "always"; // No step to check, treat as always
    }

    // Get the status of the step we're checking
    let checkStepStatus = jobStatuses.get(checkStepName);

    // If not in our cached map, query the database
    if (!checkStepStatus) {
      const checkJob = await prisma.emailJob.findFirst({
        where: { leadId: parseInt(leadId), type: checkStepName },
        orderBy: { createdAt: "desc" },
      });
      checkStepStatus = checkJob?.status;
    }

    if (!checkStepStatus) {
      // The step we're checking hasn't been scheduled yet
      return "waiting";
    }

    // Evaluate based on condition type
    switch (condition.type) {
      case "if_opened":
        if (["opened", "clicked"].includes(checkStepStatus)) {
          return "met";
        }
        // If it's delivered but not opened, we're still waiting
        if (["sent", "delivered"].includes(checkStepStatus)) {
          return "waiting";
        }
        // If it failed/bounced, condition can never be met - skip with skipIfNotMet
        if (condition.skipIfNotMet) {
          return "failed";
        }
        return "waiting";

      case "if_clicked":
        if (checkStepStatus === "clicked") {
          return "met";
        }
        // If it's opened but not clicked, we're still waiting
        if (["sent", "delivered", "opened"].includes(checkStepStatus)) {
          return "waiting";
        }
        if (condition.skipIfNotMet) {
          return "failed";
        }
        return "waiting";

      case "if_not_opened":
        // This condition is MET when the email is delivered but NOT opened
        if (["opened", "clicked"].includes(checkStepStatus)) {
          // Email was opened - condition FAILED
          return "failed";
        }
        if (["sent", "delivered"].includes(checkStepStatus)) {
          // Email was delivered but not opened - condition MET
          return "met";
        }
        // Email not sent yet
        return "waiting";

      case "if_not_clicked":
        if (checkStepStatus === "clicked") {
          return "failed";
        }
        if (["sent", "delivered", "opened"].includes(checkStepStatus)) {
          return "met";
        }
        return "waiting";

      default:
        console.warn(`[Scheduler] Unknown condition type: ${condition.type}`);
        return "always";
    }
  }

  /**
   * Prevent slot conflicts
   */
  async preventSlotConflict(leadId, targetTime) {
    const conflictWindow = 60;

    const existingJob = await prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        scheduledFor: {
          gte: moment(targetTime)
            .subtract(conflictWindow / 2, "minutes")
            .toDate(),
          lte: moment(targetTime)
            .add(conflictWindow / 2, "minutes")
            .toDate(),
        },
        status: { in: ["pending", "queued", "rescheduled"] },
      },
    });

    if (existingJob) {
      const newTime = moment(existingJob.scheduledFor).add(1, "hour").toDate();
      console.log(`Scheduler: Slot conflict detected. Moving to ${newTime}`);
      return newTime;
    }

    return targetTime;
  }

  /**
   * Helper to resolve template ID from name or ID
   */
  async resolveTemplateId(templateIdOrName) {
    if (!templateIdOrName) return null;

    // If it's a number (or string number), return it
    const asInt = parseInt(templateIdOrName);
    if (!isNaN(asInt) && asInt.toString() === templateIdOrName.toString()) {
      return asInt;
    }

    // If it's a string name, try to find in DB
    try {
      const template = await prisma.emailTemplate.findFirst({
        where: {
          name: { equals: templateIdOrName.toString(), mode: "insensitive" },
        },
      });

      if (template) {
        console.log(
          `[Scheduler] Resolved template name "${templateIdOrName}" to ID ${template.id}`,
        );
        return template.id;
      } else {
        console.warn(
          `[Scheduler] Template name "${templateIdOrName}" not found in database`,
        );
      }
    } catch (err) {
      console.warn(
        `[Scheduler] Failed to resolve template "${templateIdOrName}":`,
        err.message,
      );
    }

    return null;
  }

  /**
   * Schedule a specific email job with rate limiting
   * @param {Object} options - { skipDuplicateCheck: boolean } - Set to true for retries
   */
  async scheduleEmailJob(
    lead,
    type,
    distinctTime,
    { businessHours, windowMinutes },
    customStatus = "pending",
    retryCount = 0,
    templateId = null,
    condition = null,
    options = {}
  ) {
    const { skipDuplicateCheck = false } = options;
    
    // ============================================
    // STEP 1: ATOMIC DUPLICATE PREVENTION (FIRST!)
    // Use UniqueJourneyService for thread-safe duplicate checking
    // Skip for explicit retries to allow rescheduling cancelled/failed jobs
    // ============================================
    let scheduleGuard = { allowed: true, release: () => {} };
    
    if (!skipDuplicateCheck) {
      scheduleGuard = await UniqueJourneyService.guardScheduling(
        lead.id,
        type,
      );

      if (!scheduleGuard.allowed) {
        console.warn(`[Scheduler] DUPLICATE BLOCKED: ${scheduleGuard.reason}`);
        return null;
      }
    } else {
      console.log(`[Scheduler] Skipping duplicate check for retry of ${type}`);
    }

    try {
      const settings = await SettingsRepository.getSettings();

      // Resolve template ID if provided (might be name from bad settings)
      const resolvedTemplateId = await this.resolveTemplateId(templateId);

      let targetTime = TimezoneService.getNextBusinessHourSlot(
        lead.timezone,
        distinctTime,
        businessHours,
        windowMinutes,
      );
      let finalScheduleTime;

      let attempts = 0;
      while (attempts < 100) {
        attempts++;

        const targetMoment = moment(targetTime).tz(lead.timezone);
        if (!this.isWorkingDay(targetMoment, settings)) {
          const startHour = businessHours?.startHour || 8;
          const nextWorkingDay = this.getNextWorkingDay(
            targetMoment.add(1, "day").startOf("day").hour(startHour),
            settings,
            startHour,
          );
          targetTime = nextWorkingDay.toDate();
          continue;
        }

        if (
          !TimezoneService.isBusinessHours(
            targetTime,
            lead.timezone,
            businessHours,
          )
        ) {
          targetTime = TimezoneService.getNextBusinessHourSlot(
            lead.timezone,
            targetTime,
            businessHours,
            windowMinutes,
          );
          continue;
        }

        const reservation = await RateLimitService.reserveSlot(
          lead.timezone,
          targetTime,
        );

        if (reservation.success) {
          finalScheduleTime = reservation.reservedTime;
          break;
        } else {
          targetTime = reservation.nextWindow;
        }
      }

      if (!finalScheduleTime) {
        console.error(`Scheduler: Failed to find slot for ${lead.email}`);
        throw new Error("Failed to find scheduling slot");
      }

      // SECONDARY CHECK: Double-verify no job was created during slot finding
      // This handles edge cases where another process created a job after our initial check
      // Skip for explicit retries
      if (!skipDuplicateCheck) {
        const doubleCheck = await prisma.emailJob.findFirst({
          where: {
            leadId: parseInt(lead.id),
            type: type,
            status: {
              in: [
                "pending",
                "queued",
                "scheduled",
                "rescheduled",
                "sent",
                "delivered",
                "opened",
                "clicked",
              ],
            },
          },
        });

        if (doubleCheck) {
          console.warn(
            `[Scheduler] RACE CONDITION CAUGHT: ${type} created by another process for lead ${lead.id}`,
          );
          return null;
        }
      }

      // Determine category from type name for efficient analytics queries
      // Categories: 'initial', 'followup', 'manual', 'conditional'
      let category = 'followup'; // default for sequence emails
      const typeLower = type.toLowerCase();
      if (typeLower.includes('initial')) {
        category = 'initial';
      } else if (typeLower.startsWith('conditional:') || typeLower.startsWith('conditional ')) {
        category = 'conditional';
      }
      // Note: 'manual' category is set by scheduleManualSlot, not here

      // Create email job using Prisma
      const emailJob = await prisma.emailJob.create({
        data: {
          leadId: parseInt(lead.id),
          email: lead.email,
          type,
          category,
          scheduledFor: finalScheduleTime,
          status: customStatus === "rescheduled" ? "pending" : customStatus,
          idempotencyKey: uuidv4(),
          retryCount: retryCount,
          lastError: null,
          queueName: "followupQueue",
          templateId: resolvedTemplateId,
          condition: condition || undefined,
          metadata: {
            timezone: lead.timezone,
            localScheduledTime: moment(finalScheduleTime)
              .tz(lead.timezone)
              .format("YYYY-MM-DD HH:mm:ss z"),
            rescheduled: customStatus === "rescheduled",
            originalJobId: null,
            conditionalTemplate: resolvedTemplateId ? true : false,
            conditionType: condition?.type,
          },
        },
      });

      const visibleStatus =
        customStatus === "rescheduled" ? "rescheduled" : "scheduled";

      // Build emailSchedule relation data using proper Prisma relation syntax
      const isInitial = type.toLowerCase().includes("initial");

      // Get existing followups from lead's emailSchedule relation
      const existingSchedule = lead.emailSchedule;
      let followupsData = existingSchedule?.followups || [];

      if (!isInitial) {
        // Update or add followup entry
        if (Array.isArray(followupsData)) {
          const existingIndex = followupsData.findIndex((f) => f.name === type);
          if (existingIndex >= 0) {
            followupsData[existingIndex].scheduledFor = finalScheduleTime;
            followupsData[existingIndex].status = visibleStatus;
          } else {
            followupsData.push({
              name: type,
              scheduledFor: finalScheduleTime,
              status: visibleStatus,
              order: followupsData.length + 1,
            });
          }
        } else {
          followupsData = [
            {
              name: type,
              scheduledFor: finalScheduleTime,
              status: visibleStatus,
              order: 1,
            },
          ];
        }
      }

      // Update lead status
      await prisma.lead.update({
        where: { id: parseInt(lead.id) },
        data: {
          status: `${type}:${visibleStatus}`,
          emailSchedule: {
            upsert: {
              create: {
                nextScheduledEmail: finalScheduleTime,
                initialScheduledFor: isInitial ? finalScheduleTime : null,
                initialStatus: isInitial ? visibleStatus : "pending",
                followups: followupsData,
              },
              update: {
                nextScheduledEmail: finalScheduleTime,
                ...(isInitial && {
                  initialScheduledFor: finalScheduleTime,
                  initialStatus: visibleStatus,
                }),
                ...(!isInitial && {
                  followups: followupsData,
                }),
              },
            },
          },
        },
      });

      await EventBus.emit("EmailScheduled", {
        emailJobId: emailJob.id.toString(),
        leadId: lead.id.toString(),
        type,
        scheduledFor: finalScheduleTime,
      });

      console.log(
        `Scheduler: Scheduled '${type}' for ${lead.email} at ${finalScheduleTime}`,
      );
      return emailJob;
    } finally {
      // ALWAYS release the scheduling lock
      scheduleGuard.release();
    }
  }

  /**
   * Reschedule a job after soft bounce
   * Uses SmartDelayService to respect working hours, weekends, and paused dates
   */
  async rescheduleEmailJob(
    jobId,
    delayHours = null,
    customStatus = "rescheduled",
  ) {
    const emailJob = await EmailJobRepository.findById(jobId);
    if (!emailJob) throw new Error("Job not found");

    const lead = await LeadRepository.findById(emailJob.leadId);
    if (!lead) throw new Error("Lead not found");

    const settings = await SettingsRepository.getSettings();
    const actualDelay =
      delayHours || settings.retrySoftBounceDelayHrs || settings.retry?.softBounceDelayHours || 2;

    // Use SmartDelayService for proper time calculation respecting:
    // 1. Working hours (business hours from settings)
    // 2. Working days (excludes weekends and paused dates)
    // 3. Slot availability (rate limits)
    const leadTimezone = lead.timezone || TimezoneService.getTimezone(lead.country, lead.city) || "UTC";
    
    const SmartDelayService = require('./SmartDelayService');
    const delayResult = await SmartDelayService.calculateNextValidTime(
      new Date(),
      actualDelay,
      leadTimezone
    );
    
    const baseTime = delayResult.time;
    console.log(`[Scheduler] Rescheduling ${emailJob.type} for ${lead.email} with ${actualDelay}hr delay. ` +
      `Result: ${moment(baseTime).tz(leadTimezone).format('YYYY-MM-DD HH:mm')} ` +
      `(shifted: ${delayResult.wasShifted}, reason: ${delayResult.shiftReason || 'none'})`);

    const schedulerSettings = {
      businessHours: settings.businessHours,
      windowMinutes: settings.rateLimit?.windowMinutes || 15,
    };

    const nextRetryCount = (emailJob.retryCount || 0) + 1;
    const newJob = await this.scheduleEmailJob(
      lead,
      emailJob.type,
      baseTime,
      schedulerSettings,
      customStatus,
      nextRetryCount,
      emailJob.templateId,  // Preserve template
      null,                 // No condition
      { skipDuplicateCheck: true }  // CRITICAL: Skip duplicate check for reschedule operations
    );
    
    // Handle case where scheduleEmailJob returns null (shouldn't happen with skipDuplicateCheck)
    if (!newJob) {
      console.error(`[Scheduler] rescheduleEmailJob failed to create new job for ${emailJob.type}`);
      throw new Error("Failed to create rescheduled job - scheduleEmailJob returned null");
    }

    await prisma.emailJob.update({
      where: { id: newJob.id },
      data: {
        metadata: { ...newJob.metadata, originalJobId: jobId },
      },
    });

    // CRITICAL: Mark old job as 'rescheduled' to exclude from analytics counts
    // This prevents double-counting when failed emails are retried
    await prisma.emailJob.update({
      where: { id: parseInt(jobId) },
      data: {
        status: "rescheduled",
        metadata: {
          ...emailJob.metadata,
          rescheduledTo: newJob.id,
          rescheduledAt: new Date().toISOString(),
        },
      },
    });

    console.log(
      `[Scheduler] Rescheduled job ${jobId} -> ${newJob.id}, old job marked as 'rescheduled'`,
    );

    return newJob;
  }

  /**
   * Move a job to the next working day
   * CRITICAL: Must cancel the old job BEFORE creating new one to avoid duplicate prevention in scheduleEmailJob
   */
  async moveJobToNextWorkingDay(jobId, reason = "Date paused") {
    const emailJob = await EmailJobRepository.findById(jobId);
    if (!emailJob) throw new Error("Job not found");

    if (
      !["pending", "queued", "scheduled", "rescheduled"].includes(
        emailJob.status,
      )
    ) {
      console.log(
        `[MoveJob] Job ${jobId} not movable (${emailJob.status}). Skipping.`,
      );
      return null;
    }

    const lead = await LeadRepository.findById(emailJob.leadId);
    if (!lead) throw new Error("Lead not found");

    const settings = await SettingsRepository.getSettings();

    // CRITICAL: Use lead's timezone for all calculations
    const leadTimezone = lead.timezone || TimezoneService.getTimezone(lead.country, lead.city) || "UTC";
    const originalTime = moment(emailJob.scheduledFor).tz(leadTimezone);
    const startHour = settings.businessHours?.startHour || 8;
    const baseTime = originalTime
      .clone()
      .add(1, "day")
      .startOf("day")
      .hour(startHour);

    const nextWorkingDay = this.getNextWorkingDay(
      baseTime,
      settings,
      startHour,
    );

    console.log(
      `[MoveJob] Moving job ${jobId} (${emailJob.type}) from ${originalTime.format("YYYY-MM-DD HH:mm")} to ${nextWorkingDay.format("YYYY-MM-DD HH:mm")} ${leadTimezone}`,
    );

    // CRITICAL FIX: Cancel the old job FIRST before creating new one
    // This prevents duplicate prevention in scheduleEmailJob from blocking the new job
    await prisma.emailJob.update({
      where: { id: parseInt(jobId) },
      data: {
        status: "cancelled",
        lastError: reason,
        metadata: {
          ...emailJob.metadata,
          cancelledAt: new Date().toISOString(),
          cancelReason: reason,
        },
      },
    });

    // Remove from BullMQ if exists
    if (emailJob.metadata?.queueJobId) {
      try {
        const { followupQueue } = require("../queues/emailQueues");
        const queueJob = await followupQueue.getJob(
          emailJob.metadata.queueJobId,
        );
        if (queueJob) await queueJob.remove();
      } catch (err) {
        console.warn(`[MoveJob] Could not remove BullMQ job:`, err.message);
      }
    }

    // NOW create the new job - duplicate prevention won't block since old job is cancelled
    const schedulerSettings = {
      businessHours: settings.businessHours,
      windowMinutes: settings.rateLimit?.windowMinutes || 15,
    };

    const newJob = await this.scheduleEmailJob(
      lead,
      emailJob.type,
      nextWorkingDay.toDate(),
      schedulerSettings,
      "pending",
      emailJob.retryCount || 0,
      emailJob.templateId || null,
      emailJob.condition || null,
    );

    if (!newJob) {
      // Failed to create new job - restore the old one
      console.error(
        `[MoveJob] Failed to create new job for ${jobId}. Restoring old job.`,
      );
      await prisma.emailJob.update({
        where: { id: parseInt(jobId) },
        data: {
          status: emailJob.status,
          lastError: null,
        },
      });
      throw new Error("Failed to create new job after cancelling old one");
    }

    // Update new job metadata with move info
    await prisma.emailJob.update({
      where: { id: newJob.id },
      data: {
        metadata: {
          ...newJob.metadata,
          movedFrom: jobId,
          moveReason: reason,
          originalScheduledFor: emailJob.scheduledFor.toISOString(),
        },
      },
    });

    console.log(
      `[MoveJob] ✓ Successfully moved job ${jobId} -> ${newJob.id} to ${nextWorkingDay.format("YYYY-MM-DD HH:mm")} (${reason})`,
    );
    return newJob;
  }

  /**
   * Freeze a lead
   */
  async freezeLead(leadId, hours = null, resumeAfter = null) {
    const lead = await LeadRepository.findById(leadId);
    if (!lead) throw new Error("Lead not found");

    const isIndefinite = hours === -1 || (!hours && !resumeAfter);
    const resumeTime = isIndefinite
      ? null
      : resumeAfter || moment().add(hours, "hours").toDate();

    await LeadRepository.updateStatus(leadId, "frozen");
    await prisma.lead.update({
      where: { id: parseInt(leadId) },
      data: { frozenUntil: resumeTime },
    });
    await LeadRepository.addEvent(leadId, "frozen", {
      durationHours: isIndefinite ? "Indefinite" : hours,
      resumeAt: resumeTime,
    });

    const cancelledJobs = await prisma.emailJob.updateMany({
      where: {
        leadId: parseInt(leadId),
        status: { in: ["pending", "queued", "rescheduled", "deferred"] },
      },
      data: {
        status: "cancelled",
        lastError: isIndefinite
          ? "Manual permanent freeze"
          : `Manual freeze until ${moment(resumeTime).format("LLL")}`,
      },
    });

    console.log(
      `[Freeze] Lead ${lead.email} frozen. Cancelled ${cancelledJobs.count} jobs.`,
    );
    return await LeadRepository.findById(leadId);
  }

  /**
   * Unfreeze a lead
   */
  async unfreezeLead(leadId) {
    const lead = await LeadRepository.findById(leadId);
    if (!lead) throw new Error("Lead not found");

    if (lead.status !== "frozen") {
      console.warn(
        `[Unfreeze] Lead ${lead.email} is not frozen (status: ${lead.status})`,
      );
      return lead;
    }

    // Don't set status here - scheduleNextEmail will set proper "EmailType:scheduled" status
    await prisma.lead.update({
      where: { id: parseInt(leadId) },
      data: { frozenUntil: null },
    });
    await LeadRepository.addEvent(leadId, "unfrozen", {
      reason: "Lead unfrozen",
    });

    console.log(`[Unfreeze] Resuming outreach for ${lead.email}...`);
    await this.scheduleNextEmail(leadId, "rescheduled");

    return await LeadRepository.findById(leadId);
  }

  /**
   * Convert a lead
   */
  async convertLead(leadId) {
    const lead = await LeadRepository.findById(leadId);
    if (!lead) throw new Error("Lead not found");

    await LeadRepository.updateStatus(leadId, "converted");
    await LeadRepository.addEvent(leadId, "converted", {
      timestamp: new Date(),
    });

    const cancelledJobs = await prisma.emailJob.updateMany({
      where: {
        leadId: parseInt(leadId),
        status: { in: ["pending", "queued", "rescheduled", "deferred"] },
      },
      data: {
        status: "cancelled",
        lastError: "Lead converted to client",
      },
    });

    console.log(
      `[Converter] Cancelled ${cancelledJobs.count} jobs for lead ${lead.email}`,
    );
    return await LeadRepository.findById(leadId);
  }

  /**
   * Get available slots
   */
  async getAvailableSlots(leadId, hoursAhead = 8760) {
    const lead = await LeadRepository.findById(leadId);
    if (!lead) throw new Error("Lead not found");

    const settings = await SettingsRepository.getSettings();
    const { businessHours, rateLimit } = settings;
    const windowMinutes = rateLimit?.windowMinutes || 15;

    const slots = [];
    let currentTime = moment().tz(lead.timezone).startOf("minute");

    const minutes = currentTime.minute();
    const roundedMinutes = Math.ceil(minutes / windowMinutes) * windowMinutes;
    currentTime.minute(roundedMinutes).second(0);

    const endTime = moment().add(hoursAhead, "hours");

    while (currentTime.isBefore(endTime) && slots.length < 1000) {
      if (this.isWorkingDay(currentTime, settings)) {
        if (
          TimezoneService.isBusinessHours(
            currentTime.toDate(),
            lead.timezone,
            businessHours,
          )
        ) {
          const capacity = await RateLimitService.getSlotCapacity(
            currentTime.toDate(),
          );
          if (capacity.available > 0) {
            slots.push({
              time: currentTime.toDate(),
              localTime: currentTime.format("YYYY-MM-DD HH:mm:ss"),
              available: capacity.available,
            });
          }
        }
      }
      currentTime.add(windowMinutes, "minutes");

      if (currentTime.hour() >= businessHours.endHour) {
        currentTime.add(1, "day").hour(businessHours.startHour).minute(0);
      }
    }

    return slots;
  }

  /**
   * Manually schedule an email slot
   */
  async scheduleManualSlot(
    leadId,
    targetTime,
    emailType = null,
    title = null,
    templateId = null,
    emailBody = null,
  ) {
    console.log(
      `[ManualSlot] Called with leadId=${leadId}, templateId=${templateId}, title=${title}, emailType=${emailType}`,
    );

    const lead = await LeadRepository.findById(leadId);
    if (!lead) throw new Error("Lead not found");

    // ==========================================
    // CRITICAL SAFETY CHECK: Terminal State Guard
    // Cannot schedule ANY mail on leads in terminal state
    // ==========================================
    if (lead.terminalState) {
      console.log(`[ManualSlot] ⛔ Lead ${lead.email} is in terminal state (${lead.terminalState}), BLOCKING schedule`);
      throw new Error(`Cannot schedule mail: lead is in terminal state (${lead.terminalState}). Resurrect lead first.`);
    }
    
    // Block if lead is in failure state (requires clearing failure first)
    if (lead.isInFailure) {
      console.log(`[ManualSlot] ⛔ Lead ${lead.email} is in failure state, BLOCKING schedule - clear failure first`);
      throw new Error(`Cannot schedule mail: lead has unresolved failure. Clear failure state first.`);
    }
    // ==========================================

    // Resolve template ID (might be name from frontend)
    const resolvedTemplateId = await this.resolveTemplateId(templateId);
    console.log(
      `[ManualSlot] Resolved templateId: ${templateId} -> ${resolvedTemplateId}`,
    );

    let typeToSend = emailType || title;
    if (!typeToSend) {
      const settings = await SettingsRepository.getSettings();
      const sequence = settings.followups
        .filter((f) => f.enabled)
        .sort((a, b) => a.order - b.order);

      const failedJob = await prisma.emailJob.findFirst({
        where: {
          leadId: parseInt(leadId),
          status: {
            in: [
              "failed",
              "hard_bounce",
              "blocked",
              "spam",
              "soft_bounce",
              "deferred",
            ],
          },
        },
        orderBy: { createdAt: "desc" },
      });

      if (failedJob) {
        typeToSend = failedJob.type;
      } else {
        for (const step of sequence) {
          const alreadySent = await prisma.emailJob.findFirst({
            where: {
              leadId: parseInt(leadId),
              type: step.name,
              status: { in: ["sent", "delivered", "opened", "clicked"] },
            },
          });

          if (!alreadySent) {
            typeToSend = step.name;
            break;
          }
        }
      }

      if (!typeToSend && sequence.length > 0) {
        typeToSend = sequence[0].name;
      }
    }

    if (!typeToSend) throw new Error("No email steps configured");

    // Check for duplicates
    const duplicateWindow = new Date(Date.now() - 60000);
    const existingJob = await prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        type: typeToSend,
        createdAt: { gte: duplicateWindow },
        status: { in: ["pending", "queued", "scheduled"] },
        ...(title && { metadata: { path: ["manualTitle"], equals: title } }),
      },
    });

    if (existingJob) {
      console.warn(`[Manual] Filtered duplicate for ${lead.email}`);
      return existingJob;
    }

    // PAUSE lower-priority pending jobs (followups) - NOT cancel!
    // This uses RulebookService to correctly handle priority-based pausing
    const RulebookService = require('./RulebookService');
    const pauseResult = await RulebookService.pauseLowerPriorityJobs(leadId, 'manual');
    
    if (pauseResult.pausedCount > 0) {
      console.log(
        `[Manual] Paused ${pauseResult.pausedCount} lower-priority jobs for lead ${leadId}`,
      );
    }

    // Get retry count from paused job if any
    const preservedRetryCount = pauseResult.pausedJobs?.[0]?.retryCount || 0;


    // VALIDATE AND RESERVE SLOT
    // Manual scheduling must also respect business hours, working days, and rate limits
    const settings = await SettingsRepository.getSettings();
    
    // CRITICAL: Ensure lead has a valid timezone
    const leadTimezone = lead.timezone || TimezoneService.getTimezone(lead.country, lead.city) || "UTC";
    const requestedTime = moment(targetTime).tz(leadTimezone);

    // CHECK 1: Working Day
    const isWorkingDay = this.isWorkingDay(requestedTime, settings);
    // CHECK 2: Business Hours
    const businessHours = settings.businessHours || {
      startHour: 8,
      endHour: 18,
    };
    const isInBusinessHours =
      requestedTime.hour() >= businessHours.startHour &&
      requestedTime.hour() < businessHours.endHour;

    let finalScheduleTime;

    if (!isWorkingDay || !isInBusinessHours) {
      // User selected an invalid time - find the next valid slot
      console.log(
        `[ManualSlot] Invalid time selected (workingDay=${isWorkingDay}, businessHours=${isInBusinessHours}). Finding next valid slot.`,
      );
      const slotResult = await this.findNextAvailableSlot(
        lead.timezone || "UTC",
        new Date(targetTime),
        settings,
      );

      if (!slotResult.success) {
        throw new Error(
          `Cannot schedule: ${slotResult.reason}. Please select a time during business hours on a working day.`,
        );
      }

      finalScheduleTime = slotResult.scheduledTime;
      console.log(
        `[ManualSlot] Shifted to valid slot: ${moment(finalScheduleTime).format("YYYY-MM-DD HH:mm")}`,
      );
    } else {
      // Time is valid, try to reserve exactly that slot
      const reservation = await RateLimitService.reserveSlot(
        lead.timezone,
        new Date(targetTime),
      );

      if (!reservation.success) {
        // Slot is full - find next available
        console.log(
          `[ManualSlot] Requested slot is full. Finding next available.`,
        );
        const slotResult = await this.findNextAvailableSlot(
          lead.timezone || "UTC",
          new Date(targetTime),
          settings,
        );

        if (!slotResult.success) {
          throw new Error(
            "Slot is full and no nearby slots are available. Please pick another time.",
          );
        }

        finalScheduleTime = slotResult.scheduledTime;
      } else {
        finalScheduleTime = reservation.reservedTime;
      }
    }

    console.log(
      `[ManualSlot] Final scheduled time: ${moment(finalScheduleTime).format("YYYY-MM-DD HH:mm")}`,
    );

    const isReschedule = !title && emailType;

    // Determine category for analytics
    // For reschedules, derive category from the original email type
    // For true manual emails, use 'manual'
    let category = 'manual';
    if (isReschedule && emailType) {
      const typeLower = emailType.toLowerCase();
      if (typeLower.includes('initial')) {
        category = 'initial';
      } else if (typeLower.startsWith('conditional:') || typeLower.startsWith('conditional ')) {
        category = 'conditional';
      } else {
        category = 'followup';
      }
    }

    // Create job
    const emailJob = await prisma.emailJob.create({
      data: {
        leadId: parseInt(leadId),
        email: lead.email,
        type: typeToSend || emailType || "manual",
        category,
        scheduledFor: finalScheduleTime,
        status: "pending",
        retryCount: preservedRetryCount,
        idempotencyKey: uuidv4(),
        templateId: resolvedTemplateId,
        metadata: {
          timezone: lead.timezone,
          manual: !isReschedule,
          manualTitle: title,
          templateId: resolvedTemplateId,
          emailBody,
          changedByUser: true,
          localScheduledTime: moment(finalScheduleTime)
            .tz(lead.timezone)
            .format("YYYY-MM-DD HH:mm:ss z"),
        },
      },
    });

    // Update lead with proper Prisma relation syntax
    const leadUpdateData = {
      status: isReschedule ? "scheduled" : "manual:scheduled",
      followupsPaused: isTrueManualMail ? true : lead.followupsPaused,
      emailSchedule: {
        upsert: {
          create: {
            nextScheduledEmail: finalScheduleTime,
          },
          update: {
            nextScheduledEmail: finalScheduleTime,
          },
        },
      },
    };

    // Add manual mail to manualMails relation if not reschedule
    if (!isReschedule) {
      leadUpdateData.manualMails = {
        create: {
          title: title || typeToSend || "Manual Mail",
          scheduledFor: finalScheduleTime,
          templateId: resolvedTemplateId,
          emailJobId: emailJob.id, // Link to email job for status sync
          status: "pending",
        },
      };
    }

    await prisma.lead.update({
      where: { id: parseInt(leadId) },
      data: leadUpdateData,
    });

    console.log(
      `[Manual] Scheduled ${typeToSend} for ${lead.email} at ${finalScheduleTime}`,
    );
    return emailJob;
  }

  /**
   * Delete an email job
   */
  async deleteEmailJob(leadId, jobId) {
    const emailJob = await EmailJobRepository.findById(jobId);
    if (emailJob) {
      if (
        ["pending", "queued", "rescheduled"].includes(emailJob.status) &&
        emailJob.metadata?.queueJobId
      ) {
        try {
          const { emailSendQueue } = require("../queues/emailQueues");
          const queueJob = await emailSendQueue.getJob(
            emailJob.metadata.queueJobId,
          );
          if (queueJob) await queueJob.remove();
        } catch (e) {
          /* ignore */
        }
      }
      await prisma.emailJob.delete({ where: { id: parseInt(jobId) } });
    }

    const lead = await LeadRepository.findById(leadId);
    if (lead && lead.manualMails && lead.manualMails.length > 0) {
      const filteredManualMails = lead.manualMails.filter(
        (m) => !m.emailJobId || m.emailJobId.toString() !== jobId.toString(),
      );
      await prisma.lead.update({
        where: { id: parseInt(leadId) },
        data: { manualMails: filteredManualMails },
      });
    }
    return true;
  }

  /**
   * Move all pending jobs scheduled on paused dates to the next working day
   * Called by cron to ensure jobs don't get stuck on paused dates
   */
  async moveJobsOnPausedDates() {
    const settings = await SettingsRepository.getSettings();
    const pausedDates = settings.pausedDates || [];

    if (pausedDates.length === 0) {
      return { moved: 0, checked: 0 };
    }

    // Format paused dates for comparison
    const pausedDateStrings = pausedDates.map((pd) =>
      moment(pd).format("YYYY-MM-DD"),
    );

    // Find pending/queued jobs scheduled for future dates
    const now = new Date();
    const jobs = await prisma.emailJob.findMany({
      where: {
        status: { in: ["pending", "queued"] },
        scheduledFor: { gte: now },
      },
    });

    let movedCount = 0;
    for (const job of jobs) {
      const jobDate = moment(job.scheduledFor).format("YYYY-MM-DD");
      const isPausedDate = pausedDateStrings.includes(jobDate);

      if (isPausedDate) {
        try {
          await this.moveJobToNextWorkingDay(job.id, "Date is paused");
          movedCount++;
        } catch (err) {
          console.error(
            `[PausedDates] Failed to move job ${job.id}:`,
            err.message,
          );
        }
      }
    }

    if (movedCount > 0) {
      console.log(`[PausedDates] Moved ${movedCount} jobs from paused dates`);
    }

    return { moved: movedCount, checked: jobs.length };
  }

  /**
   * Schedule a newly added followup for leads that have completed their sequence
   * Called when a new followup is added in settings
   * OPTIMIZED: Uses cursor pagination and batch queries for 10K+ scale
   */
  async scheduleNewFollowupForCompletedLeads(newFollowupName) {
    console.log(
      `[Scheduler] Checking for completed/idle leads to schedule new followup: ${newFollowupName}`,
    );

    const settings = await SettingsRepository.getSettings();
    const followup = settings.followups?.find(
      (f) => f.name === newFollowupName && f.enabled,
    );

    if (!followup) {
      console.log(
        `[Scheduler] Followup ${newFollowupName} not found or not enabled`,
      );
      return { scheduled: 0, checked: 0 };
    }

    // Build the sequence for validation
    const sequence = settings.followups
      .filter((f) => f.enabled)
      .sort((a, b) => a.order - b.order);

    const newFollowupIndex = sequence.findIndex(
      (f) => f.name === newFollowupName,
    );
    if (newFollowupIndex === -1) {
      return { scheduled: 0, checked: 0 };
    }

    // Get required previous step names for validation
    const requiredPreviousSteps = sequence
      .slice(0, newFollowupIndex)
      .map((s) => s.name);

    let totalScheduled = 0;
    let totalChecked = 0;
    let cursor = null;
    const BATCH_SIZE = 100;

    // Process leads in batches using cursor pagination
    while (true) {
      // Fetch a batch of leads with cursor pagination
      const leads = await prisma.lead.findMany({
        take: BATCH_SIZE,
        ...(cursor && { skip: 1, cursor: { id: cursor } }),
        where: {
          OR: [
            { status: "sequence_complete" },
            { status: { startsWith: "idle" } },
            { status: { endsWith: ":clicked" } },
            { status: { endsWith: ":opened" } },
            { status: { endsWith: ":delivered" } },
            { status: { endsWith: ":sent" } },
          ],
          followupsPaused: { not: true },
          frozenUntil: null,
          NOT: {
            status: { in: ["frozen", "converted"] },
          },
        },
        orderBy: { id: "asc" },
        select: { id: true, email: true, skippedFollowups: true },
      });

      if (leads.length === 0) break;

      cursor = leads[leads.length - 1].id;
      totalChecked += leads.length;

      // Batch fetch all jobs for these leads in ONE query
      const leadIds = leads.map((l) => l.id);
      const allJobs = await prisma.emailJob.findMany({
        where: { leadId: { in: leadIds } },
        select: { leadId: true, type: true, status: true },
      });

      // Build job lookup map: leadId -> { completedTypes, pendingTypes, hasFollowup }
      const jobMap = new Map();
      for (const job of allJobs) {
        if (!jobMap.has(job.leadId)) {
          jobMap.set(job.leadId, {
            completedTypes: new Set(),
            pendingTypes: new Set(),
            hasNewFollowup: false,
          });
        }
        const data = jobMap.get(job.leadId);

        if (["sent", "delivered", "opened", "clicked"].includes(job.status)) {
          data.completedTypes.add(job.type);
        }
        if (["pending", "queued"].includes(job.status)) {
          data.pendingTypes.add(job.type);
        }
        if (
          job.type === newFollowupName &&
          [
            "pending",
            "queued",
            "sent",
            "delivered",
            "opened",
            "clicked",
          ].includes(job.status)
        ) {
          data.hasNewFollowup = true;
        }
      }

      // Process each lead with O(1) lookups
      for (const lead of leads) {
        try {
          // Skip if in skipped list
          if (lead.skippedFollowups?.includes(newFollowupName)) {
            continue;
          }

          const jobData = jobMap.get(lead.id) || {
            completedTypes: new Set(),
            pendingTypes: new Set(),
            hasNewFollowup: false,
          };

          // Skip if already has this followup
          if (jobData.hasNewFollowup) {
            continue;
          }

          // Skip if has pending jobs (normal flow will handle)
          if (jobData.pendingTypes.size > 0) {
            continue;
          }

          // Verify all previous steps are completed using the pre-fetched data
          let allPreviousComplete = true;
          for (const stepName of requiredPreviousSteps) {
            if (!jobData.completedTypes.has(stepName)) {
              allPreviousComplete = false;
              break;
            }
          }

          if (!allPreviousComplete) {
            continue;
          }

          // Schedule the new followup
          console.log(
            `[Scheduler] Scheduling new followup ${newFollowupName} for ${lead.email}`,
          );
          const result = await this.scheduleNextEmail(lead.id, "pending");
          if (result) {
            totalScheduled++;
          }
        } catch (err) {
          console.error(
            `[Scheduler] Error scheduling ${newFollowupName} for lead ${lead.id}:`,
            err.message,
          );
        }
      }

      console.log(
        `[Scheduler] Batch progress: ${totalScheduled} scheduled, ${totalChecked} checked`,
      );
    }

    console.log(
      `[Scheduler] Completed: ${totalScheduled} new followups scheduled for ${totalChecked} leads`,
    );
    return { scheduled: totalScheduled, checked: totalChecked };
  }
}

module.exports = new EmailSchedulerService();
