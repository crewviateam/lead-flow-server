// services/AnalyticsService.js
// Analytics service using Prisma with PostgreSQL

const { prisma } = require("../lib/prisma");
const {
  LeadRepository,
  EmailJobRepository,
  EventStoreRepository,
} = require("../repositories");
const { cache } = require("../lib/cache");
const EventBus = require("../events/EventBus");
const moment = require("moment");

class AnalyticsService {
  /**
   * UNIFIED ANALYTICS - Single source of truth for all analytics data
   * All other methods should use this to ensure consistency across pages
   * Uses DISTINCT lead+type for unique journey counting
   * CACHED: 5 minutes TTL to reduce database load
   */
  async getUnifiedAnalytics(startDate, endDate) {
    // Create cache key from date range
    const cacheKey = `${startDate.toISOString()}_${endDate.toISOString()}`;
    
    // Try to get from cache first
    const cached = await cache.getAnalytics(cacheKey);
    if (cached) {
      console.log('[AnalyticsService] Cache HIT for unified analytics');
      return cached;
    }
    
    console.log('[AnalyticsService] Cache MISS - querying database');
    
    // CRITICAL: Use DISTINCT ON (lead_id, type) to count unique email journeys
    // This prevents double-counting when emails are retried
    const result = await prisma.$queryRaw`
      WITH unique_journeys AS (
        -- For each lead+type, select the "best" job (most positive outcome)
        SELECT DISTINCT ON (lead_id, type)
          lead_id,
          type,
          category,
          status,
          sent_at,
          delivered_at,
          opened_at,
          clicked_at,
          bounced_at,
          failed_at,
          deferred_at,
          metadata
        FROM email_jobs
        WHERE sent_at >= ${startDate} AND sent_at <= ${endDate}
          AND status != 'rescheduled'
          AND sent_at IS NOT NULL
        ORDER BY lead_id, type,
          -- Priority: best outcomes first
          CASE 
            WHEN clicked_at IS NOT NULL THEN 1
            WHEN opened_at IS NOT NULL THEN 2
            WHEN delivered_at IS NOT NULL THEN 3
            WHEN status IN ('sent', 'pending', 'queued') THEN 4
            WHEN status IN ('soft_bounce', 'deferred') THEN 5
            ELSE 6
          END ASC,
          sent_at DESC
      )
      SELECT 
        -- TOTAL SENT: All emails that were sent (sent_at IS NOT NULL already filtered above)
        COUNT(*) as total_all,
        
        -- DELIVERED: Emails successfully delivered (status indicates success or has delivered_at)
        -- These are mutually exclusive from failed
        COUNT(*) FILTER (WHERE delivered_at IS NOT NULL AND status NOT IN ('hard_bounce', 'blocked', 'spam', 'error', 'invalid', 'unsubscribed', 'complaint', 'dead')) as total_delivered,
        COUNT(*) FILTER (WHERE opened_at IS NOT NULL) as total_opened,
        COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) as total_clicked,
        
        -- FAILED: Emails that failed delivery (hard_bounce, blocked, spam, error, invalid)
        -- Mutually exclusive from delivered
        COUNT(*) FILTER (WHERE status IN ('hard_bounce', 'blocked', 'spam', 'error', 'invalid')) as total_failed,
        COUNT(*) FILTER (WHERE status = 'hard_bounce') as hard_bounce,
        COUNT(*) FILTER (WHERE status = 'blocked') as blocked,
        COUNT(*) FILTER (WHERE status = 'spam') as spam,
        COUNT(*) FILTER (WHERE status = 'error') as error,
        COUNT(*) FILTER (WHERE status = 'invalid') as invalid,
        
        -- TERMINAL STATES: Lead is dead/unsubscribed/complaint (shown separately)
        COUNT(*) FILTER (WHERE status IN ('unsubscribed', 'complaint', 'dead')) as total_terminal,
        COUNT(*) FILTER (WHERE status = 'unsubscribed') as unsubscribed,
        COUNT(*) FILTER (WHERE status = 'complaint') as complaint,
        COUNT(*) FILTER (WHERE status = 'dead') as dead,
        
        -- RESCHEDULED: Emails pending retry (soft_bounce, deferred)
        COUNT(*) FILTER (WHERE status IN ('soft_bounce', 'deferred')) as total_rescheduled,
        COUNT(*) FILTER (WHERE status = 'soft_bounce') as soft_bounce,
        COUNT(*) FILTER (WHERE status = 'deferred') as deferred,
        
        -- BOUNCED (for rate calculation)
        COUNT(*) FILTER (WHERE bounced_at IS NOT NULL) as total_bounced,
        
        -- NOTE: pending count comes from separate query below (not from sent jobs)
        0 as pending,
        
        -- By category (using indexed column for fast queries!)
        -- Initial emails (EXCLUDE terminal states from sent)
        COUNT(*) FILTER (WHERE category = 'initial' AND status NOT IN ('unsubscribed', 'complaint', 'dead', 'cancelled', 'skipped')) as initial_sent,
        COUNT(*) FILTER (WHERE category = 'initial' AND delivered_at IS NOT NULL) as initial_delivered,
        COUNT(*) FILTER (WHERE category = 'initial' AND opened_at IS NOT NULL) as initial_opened,
        COUNT(*) FILTER (WHERE category = 'initial' AND clicked_at IS NOT NULL) as initial_clicked,
        COUNT(*) FILTER (WHERE category = 'initial' AND ((metadata->>'rescheduled')::boolean = true OR metadata->>'retryReason' IS NOT NULL)) as initial_rescheduled,
        COUNT(*) FILTER (WHERE category = 'initial' AND failed_at IS NOT NULL) as initial_failed,
        COUNT(*) FILTER (WHERE category = 'initial' AND status = 'soft_bounce') as initial_soft_bounce,
        COUNT(*) FILTER (WHERE category = 'initial' AND status = 'hard_bounce') as initial_hard_bounce,
        COUNT(*) FILTER (WHERE category = 'initial' AND status = 'blocked') as initial_blocked,
        COUNT(*) FILTER (WHERE category = 'initial' AND status = 'deferred') as initial_deferred,
        COUNT(*) FILTER (WHERE category = 'initial' AND status = 'spam') as initial_spam,
        COUNT(*) FILTER (WHERE category = 'initial' AND status = 'error') as initial_error,
        COUNT(*) FILTER (WHERE category = 'initial' AND status = 'invalid') as initial_invalid,
        COUNT(*) FILTER (WHERE category = 'initial' AND status = 'unsubscribed') as initial_unsubscribed,
        COUNT(*) FILTER (WHERE category = 'initial' AND status = 'complaint') as initial_complaint,
        COUNT(*) FILTER (WHERE category = 'initial' AND status = 'dead') as initial_dead,
        
        -- Followup emails (EXCLUDE terminal states from sent)
        COUNT(*) FILTER (WHERE category = 'followup' AND status NOT IN ('unsubscribed', 'complaint', 'dead', 'cancelled', 'skipped')) as followup_sent,
        COUNT(*) FILTER (WHERE category = 'followup' AND delivered_at IS NOT NULL) as followup_delivered,
        COUNT(*) FILTER (WHERE category = 'followup' AND opened_at IS NOT NULL) as followup_opened,
        COUNT(*) FILTER (WHERE category = 'followup' AND clicked_at IS NOT NULL) as followup_clicked,
        COUNT(*) FILTER (WHERE category = 'followup' AND ((metadata->>'rescheduled')::boolean = true OR metadata->>'retryReason' IS NOT NULL)) as followup_rescheduled,
        COUNT(*) FILTER (WHERE category = 'followup' AND failed_at IS NOT NULL) as followup_failed,
        COUNT(*) FILTER (WHERE category = 'followup' AND status = 'soft_bounce') as followup_soft_bounce,
        COUNT(*) FILTER (WHERE category = 'followup' AND status = 'hard_bounce') as followup_hard_bounce,
        COUNT(*) FILTER (WHERE category = 'followup' AND status = 'blocked') as followup_blocked,
        COUNT(*) FILTER (WHERE category = 'followup' AND status = 'deferred') as followup_deferred,
        COUNT(*) FILTER (WHERE category = 'followup' AND status = 'spam') as followup_spam,
        COUNT(*) FILTER (WHERE category = 'followup' AND status = 'error') as followup_error,
        COUNT(*) FILTER (WHERE category = 'followup' AND status = 'invalid') as followup_invalid,
        COUNT(*) FILTER (WHERE category = 'followup' AND status = 'unsubscribed') as followup_unsubscribed,
        COUNT(*) FILTER (WHERE category = 'followup' AND status = 'complaint') as followup_complaint,
        COUNT(*) FILTER (WHERE category = 'followup' AND status = 'dead') as followup_dead,
        
        -- Manual emails (EXCLUDE terminal states from sent)
        COUNT(*) FILTER (WHERE category = 'manual' AND status NOT IN ('unsubscribed', 'complaint', 'dead', 'cancelled', 'skipped')) as manual_sent,
        COUNT(*) FILTER (WHERE category = 'manual' AND delivered_at IS NOT NULL) as manual_delivered,
        COUNT(*) FILTER (WHERE category = 'manual' AND opened_at IS NOT NULL) as manual_opened,
        COUNT(*) FILTER (WHERE category = 'manual' AND clicked_at IS NOT NULL) as manual_clicked,
        COUNT(*) FILTER (WHERE category = 'manual' AND ((metadata->>'rescheduled')::boolean = true OR metadata->>'retryReason' IS NOT NULL)) as manual_rescheduled,
        COUNT(*) FILTER (WHERE category = 'manual' AND failed_at IS NOT NULL) as manual_failed,
        COUNT(*) FILTER (WHERE category = 'manual' AND status = 'soft_bounce') as manual_soft_bounce,
        COUNT(*) FILTER (WHERE category = 'manual' AND status = 'hard_bounce') as manual_hard_bounce,
        COUNT(*) FILTER (WHERE category = 'manual' AND status = 'blocked') as manual_blocked,
        COUNT(*) FILTER (WHERE category = 'manual' AND status = 'deferred') as manual_deferred,
        COUNT(*) FILTER (WHERE category = 'manual' AND status = 'spam') as manual_spam,
        COUNT(*) FILTER (WHERE category = 'manual' AND status = 'error') as manual_error,
        COUNT(*) FILTER (WHERE category = 'manual' AND status = 'invalid') as manual_invalid,
        COUNT(*) FILTER (WHERE category = 'manual' AND status = 'unsubscribed') as manual_unsubscribed,
        COUNT(*) FILTER (WHERE category = 'manual' AND status = 'complaint') as manual_complaint,
        COUNT(*) FILTER (WHERE category = 'manual' AND status = 'dead') as manual_dead,
        
        -- Conditional emails (EXCLUDE terminal states from sent)
        COUNT(*) FILTER (WHERE category = 'conditional' AND status NOT IN ('unsubscribed', 'complaint', 'dead', 'cancelled', 'skipped')) as conditional_sent,
        COUNT(*) FILTER (WHERE category = 'conditional' AND delivered_at IS NOT NULL) as conditional_delivered,
        COUNT(*) FILTER (WHERE category = 'conditional' AND opened_at IS NOT NULL) as conditional_opened,
        COUNT(*) FILTER (WHERE category = 'conditional' AND clicked_at IS NOT NULL) as conditional_clicked,
        COUNT(*) FILTER (WHERE category = 'conditional' AND ((metadata->>'rescheduled')::boolean = true OR metadata->>'retryReason' IS NOT NULL)) as conditional_rescheduled,
        COUNT(*) FILTER (WHERE category = 'conditional' AND failed_at IS NOT NULL) as conditional_failed,
        COUNT(*) FILTER (WHERE category = 'conditional' AND status = 'soft_bounce') as conditional_soft_bounce,
        COUNT(*) FILTER (WHERE category = 'conditional' AND status = 'hard_bounce') as conditional_hard_bounce,
        COUNT(*) FILTER (WHERE category = 'conditional' AND status = 'blocked') as conditional_blocked,
        COUNT(*) FILTER (WHERE category = 'conditional' AND status = 'deferred') as conditional_deferred,
        COUNT(*) FILTER (WHERE category = 'conditional' AND status = 'spam') as conditional_spam,
        COUNT(*) FILTER (WHERE category = 'conditional' AND status = 'error') as conditional_error,
        COUNT(*) FILTER (WHERE category = 'conditional' AND status = 'invalid') as conditional_invalid,
        COUNT(*) FILTER (WHERE category = 'conditional' AND status = 'unsubscribed') as conditional_unsubscribed,
        COUNT(*) FILTER (WHERE category = 'conditional' AND status = 'complaint') as conditional_complaint,
        COUNT(*) FILTER (WHERE category = 'conditional' AND status = 'dead') as conditional_dead
      FROM unique_journeys
    `;

    // SEPARATE QUERY FOR PENDING JOBS (not yet sent)
    // NOTE: Pending jobs are scheduled for the FUTURE, so we do NOT filter by date
    // This shows ALL pending jobs regardless of the selected date range
    // Uses COALESCE to check category field first, then fallback to type field parsing
    const pendingResult = await prisma.$queryRaw`
      SELECT 
        COUNT(*) as total_pending,
        COUNT(*) FILTER (WHERE 
          category = 'initial' OR 
          (category IS NULL AND LOWER(type) LIKE '%initial%')
        ) as initial_pending,
        COUNT(*) FILTER (WHERE 
          category = 'followup' OR 
          (category IS NULL AND LOWER(type) LIKE '%followup%' AND LOWER(type) NOT LIKE '%initial%' AND LOWER(type) NOT LIKE 'conditional%')
        ) as followup_pending,
        COUNT(*) FILTER (WHERE 
          category = 'manual' OR 
          (category IS NULL AND LOWER(type) LIKE '%manual%')
        ) as manual_pending,
        COUNT(*) FILTER (WHERE 
          category = 'conditional' OR 
          (category IS NULL AND LOWER(type) LIKE 'conditional%')
        ) as conditional_pending
      FROM email_jobs
      WHERE status IN ('pending', 'queued', 'scheduled')
        AND sent_at IS NULL
    `;
    console.log("result", result[0]);

    const r = result[0] || {};
    const p = pendingResult[0] || {};
    const toNum = (v) => Number(v) || 0;

    // Calculate rates - OPEN RATE IS FROM DELIVERED, NOT SENT
    const sent = toNum(r.total_all); // All emails that were sent
    const delivered = toNum(r.total_delivered);
    const opened = toNum(r.total_opened);
    const clicked = toNum(r.total_clicked);
    const bounced = toNum(r.total_bounced);
    const pending = toNum(p.total_pending);
    const rescheduled = toNum(r.total_rescheduled);
    const failed = toNum(r.total_failed);
    const terminal = toNum(r.total_terminal);

    const analyticsResult = {
      totals: {
        // SENT: All emails that were attempted
        sent,
        // DELIVERED: Successfully delivered (mutually exclusive from failed/terminal/rescheduled)
        delivered,
        opened,
        clicked,
        bounced,
        // FAILED: Delivery failed (hard_bounce, blocked, spam, error, invalid)
        failed,
        hardBounce: toNum(r.hard_bounce),
        blocked: toNum(r.blocked),
        spam: toNum(r.spam),
        error: toNum(r.error),
        invalid: toNum(r.invalid),
        // RESCHEDULED: Pending retry (soft_bounce, deferred)
        rescheduled,
        softBounce: toNum(r.soft_bounce),
        deferred: toNum(r.deferred),
        // PENDING: Not yet sent
        pending,
        // TERMINAL: Lead marked as terminal (unsubscribed, complaint, dead)
        terminal,
        unsubscribed: toNum(r.unsubscribed),
        complaint: toNum(r.complaint),
        dead: toNum(r.dead),
      },
      byType: {
        Initial: {
          sent: toNum(r.initial_sent),
          delivered: toNum(r.initial_delivered),
          opened: toNum(r.initial_opened),
          clicked: toNum(r.initial_clicked),
          rescheduled: toNum(r.initial_rescheduled),
          failed: toNum(r.initial_failed),
          softBounce: toNum(r.initial_soft_bounce),
          hardBounce: toNum(r.initial_hard_bounce),
          blocked: toNum(r.initial_blocked),
          deferred: toNum(r.initial_deferred),
          spam: toNum(r.initial_spam),
          error: toNum(r.initial_error),
          invalid: toNum(r.initial_invalid),
          unsubscribed: toNum(r.initial_unsubscribed),
          complaint: toNum(r.initial_complaint),
          dead: toNum(r.initial_dead),
          pending: toNum(p.initial_pending),
        },
        Followup: {
          sent: toNum(r.followup_sent),
          delivered: toNum(r.followup_delivered),
          opened: toNum(r.followup_opened),
          clicked: toNum(r.followup_clicked),
          rescheduled: toNum(r.followup_rescheduled),
          failed: toNum(r.followup_failed),
          softBounce: toNum(r.followup_soft_bounce),
          hardBounce: toNum(r.followup_hard_bounce),
          blocked: toNum(r.followup_blocked),
          deferred: toNum(r.followup_deferred),
          spam: toNum(r.followup_spam),
          error: toNum(r.followup_error),
          invalid: toNum(r.followup_invalid),
          unsubscribed: toNum(r.followup_unsubscribed),
          complaint: toNum(r.followup_complaint),
          dead: toNum(r.followup_dead),
          pending: toNum(p.followup_pending),
        },
        Manual: {
          sent: toNum(r.manual_sent),
          delivered: toNum(r.manual_delivered),
          opened: toNum(r.manual_opened),
          clicked: toNum(r.manual_clicked),
          rescheduled: toNum(r.manual_rescheduled),
          failed: toNum(r.manual_failed),
          softBounce: toNum(r.manual_soft_bounce),
          hardBounce: toNum(r.manual_hard_bounce),
          blocked: toNum(r.manual_blocked),
          deferred: toNum(r.manual_deferred),
          spam: toNum(r.manual_spam),
          error: toNum(r.manual_error),
          invalid: toNum(r.manual_invalid),
          unsubscribed: toNum(r.manual_unsubscribed),
          complaint: toNum(r.manual_complaint),
          dead: toNum(r.manual_dead),
          pending: toNum(p.manual_pending),
        },
        Conditional: {
          sent: toNum(r.conditional_sent),
          delivered: toNum(r.conditional_delivered),
          opened: toNum(r.conditional_opened),
          clicked: toNum(r.conditional_clicked),
          rescheduled: toNum(r.conditional_rescheduled),
          failed: toNum(r.conditional_failed),
          softBounce: toNum(r.conditional_soft_bounce),
          hardBounce: toNum(r.conditional_hard_bounce),
          blocked: toNum(r.conditional_blocked),
          deferred: toNum(r.conditional_deferred),
          spam: toNum(r.conditional_spam),
          error: toNum(r.conditional_error),
          invalid: toNum(r.conditional_invalid),
          unsubscribed: toNum(r.conditional_unsubscribed),
          complaint: toNum(r.conditional_complaint),
          dead: toNum(r.conditional_dead),
          pending: toNum(p.conditional_pending),
        },
      },
      rates: {
        // CRITICAL: Open rate is from DELIVERED, not sent
        deliveryRate: sent > 0 ? ((delivered / sent) * 100).toFixed(1) : "0.0",
        openRate:
          delivered > 0 ? ((opened / delivered) * 100).toFixed(1) : "0.0",
        clickRate:
          delivered > 0 ? ((clicked / delivered) * 100).toFixed(1) : "0.0",
        bounceRate: sent > 0 ? ((bounced / sent) * 100).toFixed(1) : "0.0",
      },
    };
    
    // Cache the result for 5 minutes
    await cache.setAnalytics(cacheKey, analyticsResult);
    
    return analyticsResult;
  }

  /**
   * Universal event handler for both webhooks and polling
   * Ensures idempotency and updates all required models
   */
  async handleEvent(rawEvent, source = "unknown") {
    const email = rawEvent.email;
    const messageId = rawEvent.messageId;
    const eventDate = rawEvent.date ? new Date(rawEvent.date) : new Date();

    // 1. Normalize Event Type
    const rawType = (
      rawEvent.event ||
      rawEvent.fetchedEventType ||
      ""
    ).toLowerCase();
    let eventType = rawType;

    const mapping = {
      requests: "sent",
      request: "sent",
      delivered: "delivered",
      opened: "opened",
      unique_opened: "unique_opened",
      uniqueopened: "unique_opened",
      first_opening: "unique_opened",
      clicks: "clicked",
      click: "clicked",
      softbounces: "soft_bounce",
      softbounce: "soft_bounce",
      soft_bounce: "soft_bounce",
      hardbounces: "hard_bounce",
      hardbounce: "hard_bounce",
      hard_bounce: "hard_bounce",
      spam: "spam",
      complaint: "spam",
      invalid_email: "failed",
      invalidemail: "failed",
      invalid: "failed",
      blocked: "blocked",
      deferred: "deferred",
      unsubscribed: "unsubscribed",
    };

    eventType = mapping[rawType] || rawType;

    if (!messageId || !eventType) {
      console.warn(
        `[Analytics] Skipping invalid event: msgId=${messageId}, type=${rawType}`,
      );
      return false;
    }

    // 2. Idempotency Check using Prisma
    const wasProcessed = await EventStoreRepository.wasProcessed(
      messageId,
      eventType,
    );
    if (wasProcessed) {
      return false;
    }

    const marked = await EventStoreRepository.markProcessed(
      messageId,
      eventType,
      eventDate,
    );
    if (!marked) {
      return false; // Duplicate
    }

    // 3. Find the Source Email Job
    let emailJob = await prisma.emailJob.findFirst({
      where: { brevoMessageId: messageId },
    });

    if (!emailJob) {
      // Fallback: match by email and timing
      emailJob = await prisma.emailJob.findFirst({
        where: {
          email: email,
          scheduledFor: { lte: new Date() },
        },
        orderBy: { scheduledFor: "desc" },
      });
    }

    if (!emailJob) {
      console.warn(`[Analytics] No matching Job for ${email} | ${messageId}`);
      return false;
    }

    // 4. Update EmailJob status
    const terminalStatuses = [
      "delivered",
      "opened",
      "clicked",
      "soft_bounce",
      "hard_bounce",
      "failed",
      "blocked",
      "spam",
      "unsubscribed",
    ];
    if (
      terminalStatuses.includes(eventType) ||
      !emailJob.status ||
      emailJob.status === "sent" ||
      emailJob.status === "queued"
    ) {
      const updateData = { status: eventType };
      if (rawEvent.reason) updateData.lastError = rawEvent.reason;

      // Map eventType to timestamp fields
      const timestampMap = {
        sent: "sentAt",
        delivered: "deliveredAt",
        opened: "openedAt",
        unique_opened: "openedAt",
        clicked: "clickedAt",
        soft_bounce: "bouncedAt",
        hard_bounce: "failedAt",
        failed: "failedAt",
        spam: "failedAt",
        blocked: "failedAt",
        deferred: "deferredAt",
      };

      const tsField = timestampMap[eventType];
      if (tsField && !emailJob[tsField]) {
        updateData[tsField] = eventDate;
      }

      // For blocked/failed/bounced events: also set sentAt if empty (since email was sent to Brevo)
      const failureEvents = [
        "blocked",
        "failed",
        "hard_bounce",
        "soft_bounce",
        "spam",
      ];
      if (failureEvents.includes(eventType) && !emailJob.sentAt) {
        updateData.sentAt = eventDate;
      }

      // AUTO-RESCHEDULE LOGIC: For soft_bounce and deferred - mark metadata
      // The actual EmailBounced event will be emitted by the dynamic routing at line ~671
      if (["soft_bounce", "deferred"].includes(eventType)) {
        const currentMetadata = emailJob.metadata || {};
        updateData.metadata = { ...currentMetadata, rescheduled: true };
      }

      // Don't downgrade status
      const statusHierarchy = {
        sent: 1,
        delivered: 2,
        opened: 3,
        unique_opened: 4,
        clicked: 5,
      };
      const currentRank = statusHierarchy[emailJob.status] || 0;
      const newRank = statusHierarchy[eventType] || 0;

      if (newRank >= currentRank || !statusHierarchy[eventType]) {
        await prisma.emailJob.update({
          where: { id: emailJob.id },
          data: updateData,
        });

        // SYNC emailSchedule status with email job status
        const lead = await prisma.lead.findUnique({
          where: { id: emailJob.leadId },
          include: { emailSchedule: true },
        });

        if (lead?.emailSchedule) {
          const isInitial = emailJob.type?.toLowerCase().includes("initial");

          if (isInitial) {
            // Update initialStatus in emailSchedule
            await prisma.emailSchedule.update({
              where: { id: lead.emailSchedule.id },
              data: { initialStatus: eventType },
            });
          } else if (emailJob.type?.startsWith("conditional:")) {
            // Update ConditionalEmailJob status when events occur
            // ConditionalEmailJob is linked by emailJobId
            const condUpdateResult =
              await prisma.conditionalEmailJob.updateMany({
                where: { emailJobId: emailJob.id },
                data: {
                  status: eventType === "delivered" ? "completed" : eventType,
                },
              });

            if (condUpdateResult.count > 0) {
              console.log(
                `[AnalyticsService] Updated ConditionalEmailJob status to ${eventType} for emailJobId ${emailJob.id}`,
              );
            } else {
              // Try fallback by metadata
              if (emailJob.metadata?.conditionalJobId) {
                await prisma.conditionalEmailJob.update({
                  where: { id: emailJob.metadata.conditionalJobId },
                  data: {
                    status: eventType === "delivered" ? "completed" : eventType,
                  },
                });
                console.log(
                  `[AnalyticsService] Fallback: Updated ConditionalEmailJob ${emailJob.metadata.conditionalJobId} to ${eventType}`,
                );
              }
            }

            // Also update followups JSON entry for conditional emails
            let followups = lead.emailSchedule.followups || [];
            if (!Array.isArray(followups)) {
              try {
                followups = JSON.parse(followups);
              } catch (e) {
                followups = [];
              }
            }

            const condFollowupIndex = followups.findIndex(
              (f) =>
                f.name === emailJob.type ||
                f.name ===
                  `Conditional: ${emailJob.type.replace("conditional:", "")}`,
            );
            if (condFollowupIndex >= 0) {
              followups[condFollowupIndex].status = eventType;
              await prisma.emailSchedule.update({
                where: { id: lead.emailSchedule.id },
                data: { followups },
              });
            }
          } else if (emailJob.type === "manual" || emailJob.metadata?.manual) {
            // Update ManualMail status using Prisma model (not JSON)
            // ManualMail is a separate table linked by emailJobId
            const updateResult = await prisma.manualMail.updateMany({
              where: { emailJobId: emailJob.id },
              data: { status: eventType },
            });

            if (updateResult.count > 0) {
              console.log(
                `[AnalyticsService] Updated ManualMail status to ${eventType} for emailJobId ${emailJob.id}`,
              );
            } else {
              // Fallback: try matching by leadId and similar scheduledFor time
              console.warn(
                `[AnalyticsService] No ManualMail found with emailJobId ${emailJob.id}, trying fallback`,
              );
              const fallbackResult = await prisma.manualMail.updateMany({
                where: {
                  leadId: emailJob.leadId,
                  status: "pending",
                },
                data: { status: eventType },
              });
              if (fallbackResult.count > 0) {
                console.log(
                  `[AnalyticsService] Fallback: Updated ${fallbackResult.count} ManualMail(s) to ${eventType}`,
                );
              }
            }
          } else {
            // Update followup status in emailSchedule.followups JSON
            let followups = lead.emailSchedule.followups || [];
            if (!Array.isArray(followups)) {
              try {
                followups = JSON.parse(followups);
              } catch (e) {
                followups = [];
              }
            }

            const followupIndex = followups.findIndex(
              (f) => f.name === emailJob.type,
            );
            if (followupIndex >= 0) {
              followups[followupIndex].status = eventType;
              await prisma.emailSchedule.update({
                where: { id: lead.emailSchedule.id },
                data: { followups },
              });
            }
          }
        }

        // TRIGGER FOLLOWUPS: When delivered, schedule next followup
        // Followups are now a simple "no response" chain - conditions removed
        if (["delivered"].includes(eventType)) {
          console.log(
            `[AnalyticsService] 📋 Triggering followup check for lead ${emailJob.leadId} after ${eventType}`,
          );
          try {
            const EmailSchedulerService = require("./EmailSchedulerService");
            const scheduledJob = await EmailSchedulerService.scheduleNextEmail(
              emailJob.leadId,
            );
            if (scheduledJob) {
              console.log(
                `[AnalyticsService] ✓ Followup scheduled after ${eventType}: ${scheduledJob.type}`,
              );
            }
          } catch (schedError) {
            console.error(
              `[AnalyticsService] Error scheduling followup:`,
              schedError,
            );
          }
        }

        // TRIGGER CONDITIONAL EMAILS FIRST: When opened/clicked events occur,
        // evaluate if any conditional emails should be triggered
        // IMPORTANT: This MUST happen BEFORE recalculating status so the new
        // conditional email job exists when we check for pending jobs
        if (
          ["opened", "unique_opened", "clicked", "delivered"].includes(
            eventType,
          )
        ) {
          console.log(
            `[AnalyticsService] 🎯 Evaluating conditional email triggers for lead ${emailJob.leadId} after ${eventType}`,
          );
          try {
            const ConditionalEmailService = require("./ConditionalEmailService");
            const triggeredJobs =
              await ConditionalEmailService.evaluateTriggers(
                emailJob.leadId,
                eventType === "unique_opened" ? "opened" : eventType, // Normalize unique_opened to opened
                emailJob.type,
                emailJob.id,
              );
            if (triggeredJobs.length > 0) {
              console.log(
                `[AnalyticsService] 🎯 Triggered ${triggeredJobs.length} conditional email(s)`,
              );
            }
          } catch (condError) {
            console.error(
              `[AnalyticsService] Error evaluating conditional emails:`,
              condError,
            );
          }
        }

        // Update lead.status - use StatusUpdateService to properly check for pending jobs
        // IMPORTANT: This runs AFTER conditional emails are created, so it will find them
        const StatusUpdateService = require("./StatusUpdateService");
        console.log(
          `[AnalyticsService] Recalculating lead ${emailJob.leadId} status after event: ${eventType}`,
        );
        const newStatus = await StatusUpdateService._recalculateStatus(
          emailJob.leadId,
          eventType,
          emailJob.type,
        );
        console.log(`[AnalyticsService] Lead status updated to: ${newStatus}`);
      }
    }

    // 5. Update Lead History & Scoring
    const lead = await LeadRepository.findById(emailJob.leadId);
    if (lead) {
      await LeadRepository.addEvent(
        emailJob.leadId,
        eventType,
        {
          ...rawEvent,
          source,
          timestamp: eventDate,
        },
        emailJob.type,
        emailJob.id,
      );

      // Update counters based on event
      const counterMap = { opened: "emailsOpened", clicked: "emailsClicked" };
      if (counterMap[eventType]) {
        await LeadRepository.incrementCounter(
          emailJob.leadId,
          counterMap[eventType],
          1,
        );
      }

      // Update score
      const scoreMap = { opened: 5, clicked: 15, hard_bounce: -20, spam: -30 };
      if (scoreMap[eventType]) {
        await LeadRepository.updateScore(emailJob.leadId, scoreMap[eventType]);
      }
    }

    // 6. Log to Audit Store
    await EventStoreRepository.create({
      eventType: `Email${eventType.charAt(0).toUpperCase() + eventType.slice(1)}`,
      aggregateId: messageId,
      aggregateType: "EmailJob",
      payload: rawEvent,
      metadata: { source },
    });

    // 7. Trigger System Side-Effects
    // Route events to correct handlers based on RulebookService event categories
    let handlerName = `Email${eventType.charAt(0).toUpperCase() + eventType.slice(1)}`;
    
    // Bounce events go to EmailBounced (soft_bounce and hard_bounce)
    // IMPORTANT: deferred has its own handler EmailDeferred for auto-reschedule
    if (eventType.includes("bounce")) handlerName = "EmailBounced";
    if (eventType === "deferred") handlerName = "EmailDeferred";
    
    // Complaint/Unsubscribed get their own dedicated handlers
    if (eventType === "complaint") handlerName = "EmailComplaint";
    if (eventType === "unsubscribed") handlerName = "EmailUnsubscribed";
    
    // Failed, blocked, spam, error, invalid go to EmailFailed
    if (["failed", "blocked", "spam", "error", "invalid"].includes(eventType)) {
      handlerName = "EmailFailed";
    }

    await EventBus.emit(handlerName, {
      emailJobId: emailJob.id,
      leadId: emailJob.leadId,
      email: emailJob.email,
      brevoMessageId: messageId,
      eventData: rawEvent,
      type: emailJob.type,
      timestamp: eventDate,
    });

    // Invalidate analytics cache
    await cache.invalidateAnalytics();

    return true;
  }

  async processWebhookEvent(eventType, eventData) {
    return await this.handleEvent(eventData, "webhook");
  }

  async getSummary(startDate, endDate) {
    // Check cache first
    const cacheKey = `${startDate.toISOString()}_${endDate.toISOString()}`;
    const cached = await cache.getAnalytics(cacheKey);
    if (cached) return cached;

    // Exclude rescheduled jobs from all counts for accurate unique journey counts
    const excludeRescheduled = { NOT: { status: "rescheduled" } };

    // CRITICAL FIX: Use DISTINCT lead+type for unique journey counts
    // This ensures retried emails are counted once per unique lead+type combination
    const uniqueCountsResult = await prisma.$queryRaw`
      WITH unique_journeys AS (
        SELECT DISTINCT ON (lead_id, type)
          lead_id, type, status, sent_at, delivered_at, opened_at, clicked_at, bounced_at, failed_at
        FROM email_jobs
        WHERE sent_at >= ${startDate} AND sent_at <= ${endDate}
          AND status != 'rescheduled'
          AND sent_at IS NOT NULL
        ORDER BY lead_id, type,
          CASE 
            WHEN clicked_at IS NOT NULL THEN 1
            WHEN opened_at IS NOT NULL THEN 2
            WHEN delivered_at IS NOT NULL THEN 3
            ELSE 4
          END ASC,
          sent_at DESC
      )
      SELECT 
        COUNT(*) as sent,
        COUNT(*) FILTER (WHERE delivered_at IS NOT NULL) as delivered,
        COUNT(*) FILTER (WHERE opened_at IS NOT NULL) as opened,
        COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) as clicked,
        COUNT(*) FILTER (WHERE bounced_at IS NOT NULL) as bounced,
        COUNT(*) FILTER (WHERE failed_at IS NOT NULL) as failed
      FROM unique_journeys
    `;

    const counts = uniqueCountsResult[0] || {
      sent: 0,
      delivered: 0,
      opened: 0,
      clicked: 0,
      bounced: 0,
      failed: 0,
    };
    const sent = Number(counts.sent);
    const delivered = Number(counts.delivered);
    const opened = Number(counts.opened);
    const clicked = Number(counts.clicked);
    const bounced = Number(counts.bounced);
    const failed = Number(counts.failed);

    const totalLeads = await prisma.lead.count();

    // Get daily breakdown using DISTINCT counts per day
    const dailyBreakdown = await prisma.$queryRaw`
      WITH unique_daily AS (
        SELECT DISTINCT ON (lead_id, type)
          lead_id, type, 
          DATE(sent_at AT TIME ZONE 'Asia/Kolkata') as date,
          sent_at, delivered_at, opened_at, clicked_at, bounced_at, failed_at
        FROM email_jobs
        WHERE sent_at >= ${startDate} AND sent_at <= ${endDate}
          AND status != 'rescheduled'
          AND sent_at IS NOT NULL
        ORDER BY lead_id, type,
          CASE 
            WHEN clicked_at IS NOT NULL THEN 1
            WHEN opened_at IS NOT NULL THEN 2
            WHEN delivered_at IS NOT NULL THEN 3
            ELSE 4
          END ASC,
          sent_at DESC
      )
      SELECT 
        date,
        COUNT(*) FILTER (WHERE sent_at IS NOT NULL) as sent,
        COUNT(*) FILTER (WHERE delivered_at IS NOT NULL) as delivered,
        COUNT(*) FILTER (WHERE opened_at IS NOT NULL) as opened,
        COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) as clicked,
        COUNT(*) FILTER (WHERE bounced_at IS NOT NULL) as bounced,
        COUNT(*) FILTER (WHERE failed_at IS NOT NULL) as failed
      FROM unique_daily
      GROUP BY date
      ORDER BY date DESC
    `;

    const result = {
      emailsSent: Number(sent),
      emailsDelivered: Number(delivered),
      emailsOpened: Number(opened),
      emailsClicked: Number(clicked),
      emailsBounced: Number(bounced),
      emailsFailed: Number(failed),
      totalLeads: Number(totalLeads),
      // FIXED: Open rate from delivered, not sent
      deliveryRate: sent > 0 ? ((delivered / sent) * 100).toFixed(1) : 0,
      openRate: delivered > 0 ? ((opened / delivered) * 100).toFixed(1) : 0,
      clickRate: delivered > 0 ? ((clicked / delivered) * 100).toFixed(1) : 0,
      bounceRate: sent > 0 ? ((bounced / sent) * 100).toFixed(1) : 0,
      dailyBreakdown: dailyBreakdown.map((day) => ({
        date: day.date,
        emailsSent: Number(day.sent) || 0,
        emailsDelivered: Number(day.delivered) || 0,
        emailsOpened: Number(day.opened) || 0,
        emailsClicked: Number(day.clicked) || 0,
        emailsBounced: Number(day.bounced) || 0,
        emailsFailed: Number(day.failed) || 0,
      })),
    };

    // Cache the result
    await cache.setAnalytics(cacheKey, result);
    return result;
  }

  async getDashboardData(startDate, endDate) {
    // Check cache first
    const cacheKey = `dashboard_${startDate?.toISOString() || "all"}_${endDate?.toISOString() || "now"}`;
    const cached = await cache.getDashboard(cacheKey);
    if (cached) return cached;

    const dateFilter = {};
    if (startDate) dateFilter.gte = startDate;
    if (endDate) dateFilter.lte = endDate;
    const hasDateFilter = startDate || endDate;

    // Parallel queries for all dashboard data
    const [jobStats, leadStats, convertedCount, frozenCount, recentActivity] =
      await Promise.all([
        // Job stats by status
        prisma.emailJob.groupBy({
          by: ["status"],
          where: hasDateFilter ? { updatedAt: dateFilter } : undefined,
          _count: { status: true },
        }),
        // Lead stats by status
        prisma.lead.groupBy({
          by: ["status"],
          where: hasDateFilter ? { updatedAt: dateFilter } : undefined,
          _count: { status: true },
        }),
        // Converted leads
        prisma.lead.count({
          where: { status: { contains: "converted" } },
        }),
        // Frozen leads
        prisma.lead.count({
          where: { frozenUntil: { not: null } },
        }),
        // Recent activity
        EmailJobRepository.getRecentActivity(20),
      ]);

    // Transform stats to object format
    const emailJobs = {};
    jobStats.forEach((j) => (emailJobs[j.status] = j._count.status));

    const leads = {};
    leadStats.forEach((l) => (leads[l.status] = l._count.status));

    const result = {
      emailJobs,
      leads,
      total: Object.values(leads).reduce((a, b) => a + b, 0),
      convertedCount,
      frozenCount,
      recentActivity: recentActivity.map((job) => ({
        leadName: job.lead?.name || "Unknown",
        status: job.status,
        type: job.type,
        timestamp: job.updatedAt,
      })),
    };

    // Cache the result
    await cache.setDashboard(cacheKey, result);
    return result;
  }
}

module.exports = new AnalyticsService();
