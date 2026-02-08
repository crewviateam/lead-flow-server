// services/RulebookService.js
// Single Source of Truth for Email System Rules
// User-configurable with defaults and reset capability
// THIS IS THE HEART OF THE SOFTWARE - ALL SYSTEM BEHAVIOR DERIVES FROM THESE RULES

const { prisma } = require('../lib/prisma');

// ========================================
// DEFAULT RULEBOOK CONFIGURATION
// ========================================

const DEFAULT_RULEBOOK = {
  version: "1.0.0",
  lastUpdated: new Date().toISOString(),

  // ============================================================
  // SECTION 1: MAIL TYPE DEFINITIONS
  // Defines all types of emails in the system and their properties
  // ============================================================
  mailTypes: {
    initial: {
      id: "initial",
      displayName: "Initial Mail",
      internalTypes: ["Initial Email", "initial", "initial_email"],
      category: "automated",
      priority: 100, // Highest priority - sends first
      canPause: false,
      canSkip: false,
      canReschedule: true,
      canCancel: true,
      canRetry: true,
      maxRetries: 3,
      triggeredBy: "lead_creation",
      description: "First email sent to a new lead",
    },
    followup: {
      id: "followup",
      displayName: "Followup Mail",
      internalTypes: [
        "First Followup",
        "Second Followup",
        "Third Followup",
        "followup",
      ],
      category: "automated",
      priority: 80,
      canPause: true,
      canSkip: true,
      canReschedule: true,
      canCancel: false, // Skippable jobs should use skip, not cancel
      canRetry: true,
      maxRetries: 3,
      triggeredBy: "previous_email_delivered",
      description: "Automated followup emails in sequence",
    },
    conditional: {
      id: "conditional",
      displayName: "Conditional Mail",
      internalTypes: ["conditional:", "conditional"],
      category: "triggered",
      priority: 95, // Higher than followups - takes precedence
      canPause: false,
      canSkip: false,
      canReschedule: true,
      canCancel: true,
      canRetry: true,
      maxRetries: 3,
      triggeredBy: "engagement_event",
      description: "Emails triggered by recipient actions (open, click)",
    },
    manual: {
      id: "manual",
      displayName: "Manual Mail",
      internalTypes: ["manual", "Manual"],
      category: "manual",
      priority: 85,
      canPause: false,
      canSkip: false,
      canReschedule: true,
      canCancel: true,
      canRetry: true,
      maxRetries: 3,
      triggeredBy: "user_action",
      description: "User-created manual emails",
    },
  },

  // ============================================================
  // SECTION 2: STATUS DEFINITIONS
  // All possible statuses for email jobs and their properties
  // ============================================================
  statuses: {
    // ACTIVE/PENDING STATES
    pending: {
      display: "Scheduled",
      color: "#eab308",
      bgColor: "rgba(234, 179, 8, 0.1)",
      icon: "clock",
      isTerminal: false,
      isActive: true,
      showInQueue: true,
      allowedTransitions: ["sent", "cancelled", "paused", "rescheduled"],
    },
    queued: {
      display: "Queued",
      color: "#f97316",
      bgColor: "rgba(249, 115, 22, 0.1)",
      icon: "queue",
      isTerminal: false,
      isActive: true,
      showInQueue: true,
      allowedTransitions: ["sent", "failed", "cancelled"],
    },
    scheduled: {
      display: "Scheduled",
      color: "#eab308",
      bgColor: "rgba(234, 179, 8, 0.1)",
      icon: "calendar",
      isTerminal: false,
      isActive: true,
      showInQueue: true,
      allowedTransitions: ["pending", "cancelled", "rescheduled"],
    },
    rescheduled: {
      display: "Rescheduled",
      color: "#06b6d4",
      bgColor: "rgba(6, 182, 212, 0.1)",
      icon: "refresh",
      isTerminal: false,
      isActive: true,
      showInQueue: true,
      allowedTransitions: ["pending", "sent", "cancelled"],
    },
    paused: {
      display: "Paused",
      color: "#f59e0b",
      bgColor: "rgba(245, 158, 11, 0.1)",
      icon: "pause",
      isTerminal: false,
      isActive: false,
      showInQueue: false,
      allowedTransitions: ["pending", "cancelled"],
    },

    // SENT/DELIVERY STATES
    sent: {
      display: "Sent",
      color: "#8b5cf6",
      bgColor: "rgba(139, 92, 246, 0.1)",
      icon: "send",
      isTerminal: false,
      isActive: false,
      showInQueue: false,
      allowedTransitions: [
        "delivered",
        "soft_bounce",
        "hard_bounce",
        "blocked",
        "failed",
      ],
    },
    delivered: {
      display: "Delivered",
      color: "#22c55e",
      bgColor: "rgba(34, 197, 94, 0.1)",
      icon: "check",
      isTerminal: false,
      isActive: false,
      showInQueue: false,
      allowedTransitions: ["opened", "clicked", "spam"],
    },

    // ENGAGEMENT STATES
    opened: {
      display: "Opened",
      color: "#3b82f6",
      bgColor: "rgba(59, 130, 246, 0.1)",
      icon: "eye",
      isTerminal: false,
      isActive: false,
      showInQueue: false,
      allowedTransitions: ["clicked"],
    },
    unique_opened: {
      display: "Unique Open",
      color: "#3b82f6",
      bgColor: "rgba(59, 130, 246, 0.1)",
      icon: "eye",
      isTerminal: false,
      isActive: false,
      showInQueue: false,
      allowedTransitions: ["clicked"],
    },
    clicked: {
      display: "Clicked",
      color: "#a855f7",
      bgColor: "rgba(168, 85, 247, 0.1)",
      icon: "cursor",
      isTerminal: false,
      isActive: false,
      showInQueue: false,
      allowedTransitions: [],
    },

    // TEMPORARY FAILURE STATES (can retry)
    soft_bounce: {
      display: "Soft Bounce",
      color: "#eab308",
      bgColor: "rgba(234, 179, 8, 0.1)",
      icon: "alert",
      isTerminal: false,
      isActive: false,
      showInQueue: false,
      canRetry: true,
      allowedTransitions: ["pending", "rescheduled", "failed"],
    },
    deferred: {
      display: "Deferred",
      color: "#eab308",
      bgColor: "rgba(234, 179, 8, 0.1)",
      icon: "clock",
      isTerminal: false,
      isActive: false,
      showInQueue: false,
      canRetry: true,
      allowedTransitions: ["pending", "rescheduled", "failed"],
    },

    // TERMINAL FAILURE STATES (cannot retry)
    hard_bounce: {
      display: "Hard Bounce",
      color: "#ef4444",
      bgColor: "rgba(239, 68, 68, 0.1)",
      icon: "x-circle",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: false,
      allowedTransitions: [],
    },
    failed: {
      display: "Failed",
      color: "#ef4444",
      bgColor: "rgba(239, 68, 68, 0.1)",
      icon: "x-circle",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: true,
      allowedTransitions: ["pending"],
    },
    blocked: {
      display: "Blocked",
      color: "#ef4444",
      bgColor: "rgba(239, 68, 68, 0.1)",
      icon: "ban",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: false,
      allowedTransitions: [],
    },
    spam: {
      display: "Spam",
      color: "#ef4444",
      bgColor: "rgba(239, 68, 68, 0.1)",
      icon: "alert-triangle",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: false,
      allowedTransitions: [],
    },

    // USER ACTION STATES
    cancelled: {
      display: "Cancelled",
      color: "#6b7280",
      bgColor: "rgba(107, 114, 128, 0.1)",
      icon: "x",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: true,
      allowedTransitions: ["pending"],
    },
    skipped: {
      display: "Skipped",
      color: "#6b7280",
      bgColor: "rgba(107, 114, 128, 0.1)",
      icon: "skip-forward",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: false,
      allowedTransitions: [],
    },

    // ERROR STATES
    invalid: {
      display: "Invalid Email",
      color: "#ef4444",
      bgColor: "rgba(239, 68, 68, 0.1)",
      icon: "alert-circle",
      category: "failed",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: false, // Invalid email cannot be retried
      manualRetryOnly: true,
      allowedTransitions: [],
    },
    error: {
      display: "Error",
      color: "#ef4444",
      bgColor: "rgba(239, 68, 68, 0.1)",
      icon: "alert-triangle",
      category: "failed",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: true,
      manualRetryOnly: true, // Only manual retry, not auto
      allowedTransitions: ["pending"],
    },

    // NEGATIVE ENGAGEMENT STATES (Special Handling Required)
    complaint: {
      display: "Spam Complaint",
      color: "#dc2626",
      bgColor: "rgba(220, 38, 38, 0.1)",
      icon: "flag",
      category: "negative",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: true, // Can retry but requires user confirmation
      manualRetryOnly: true,
      pausesFutureMails: true, // CRITICAL: Stop all future mails
      showInComplaintPage: true,
      allowedTransitions: ["pending"],
    },
    unsubscribed: {
      display: "Unsubscribed",
      color: "#9333ea",
      bgColor: "rgba(147, 51, 234, 0.1)",
      icon: "user-x",
      category: "negative",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: true, // Can retry but requires user confirmation
      manualRetryOnly: true,
      pausesFutureMails: true, // CRITICAL: Stop all future mails
      showInComplaintPage: true,
      allowedTransitions: ["pending"],
    },

    // DEAD STATE (Retry Limit Exceeded)
    dead: {
      display: "Dead",
      color: "#4b5563",
      bgColor: "rgba(75, 85, 99, 0.1)",
      icon: "skull",
      category: "terminal",
      isTerminal: true,
      isActive: false,
      showInQueue: false,
      canRetry: false, // Cannot retry - lead is dead
      manualRetryOnly: false,
      pausesFutureMails: true, // CRITICAL: Stop all future mails
      showInTerminalPage: true,
      description:
        "Lead exceeded maximum retry attempts - all future mails cancelled",
      allowedTransitions: [], // No transitions from dead
    },
  },

  // ============================================================
  // SECTION 2.5: STATUS GROUPS
  // Categorized groups for Resume/Retry/Auto-Resume logic
  // ============================================================
  statusGroups: {
    // Jobs that show RESUME button (temporary pause, no retry increment)
    resumable: ["paused"],

    // Jobs that show RETRY button (permanent failure/cancellation, increments retry count)
    retriable: [
      "cancelled",
      "failed",
      "soft_bounce",
      "deferred",
      "error",
      "hard_bounce",
      "blocked",
      "complaint",
      "unsubscribed",
      "invalid",
    ],

    // Jobs with terminal failures that can retry with user confirmation
    manualRetriable: [
      "hard_bounce",
      "blocked",
      "complaint",
      "unsubscribed",
      "invalid",
      "error",
    ],

    // High-priority mail types that trigger auto-resume when cancelled/completed
    triggersAutoResume: ["conditional", "manual"],

    // Statuses that should trigger auto-resume of paused jobs
    autoResumeOnStatus: ["delivered", "cancelled"],
  },

  // ============================================================
  // SECTION 3: LEAD STATUS RULES
  // Rules for what status should be displayed on the Lead
  // ============================================================
  leadStatusRules: {
    // FORBIDDEN STATUSES: These should NEVER appear as lead status
    // Engagement events are job-level, not lead-level
    forbiddenStatuses: [
      "opened",
      "unique_opened",
      "clicked",
      "delivered",
      "paused",
      "cancelled",
      "skipped",
    ],

    // ALLOWED STATUSES: Only these can be shown as lead status
    allowedStatuses: [
      "scheduled",
      "rescheduled",
      "sent",
      "blocked",
      "failed",
      "hard_bounce",
      "soft_bounce",
      "spam",
      "invalid",
      "error",
    ],

    // SPECIAL LEAD STATUSES: Non-email statuses
    specialStatuses: [
      "frozen",
      "converted",
      "idle",
      "sequence_complete",
      "unsubscribed",
    ],

    // DEFAULT STATUS: When no other status applies
    defaultStatus: "idle",

    // STATUS FORMAT: How lead status should be displayed
    // Format: {type}:{status} e.g., "Followup Mail:scheduled"
    statusFormat: "{simplifiedType}:{status}",

    // PRIORITY ORDER: Higher number = higher priority (shows this status)
    priority: {
      // Terminal statuses - highest priority
      converted: 100,
      unsubscribed: 99,

      // Frozen state
      frozen: 95,

      // Failure states - should be visible
      blocked: 90,
      hard_bounce: 89,
      failed: 88,
      spam: 87,
      soft_bounce: 86,

      // Scheduled states - most common display
      scheduled: 80,
      rescheduled: 79,
      pending: 78,

      // Sent state
      sent: 70,

      // Complete
      sequence_complete: 60,

      // Default
      idle: 10,
    },

    // WHEN TO UPDATE LEAD STATUS
    updateOn: {
      jobScheduled: true,
      jobSent: true,
      jobFailed: true,
      jobCancelled: true, // Will look for next scheduled job
      jobCompleted: false, // Engagement events don't update lead status
      leadFrozen: true,
      leadConverted: true,
      leadUnfrozen: true,
    },
  },

  // ============================================================
  // SECTION 4: TRIGGER RULES
  // What events trigger what actions
  // ============================================================
  triggerRules: {
    // CONDITIONAL EMAIL TRIGGERS
    // These webhook events trigger conditional email evaluation
    conditionalTriggerEvents: ["opened", "unique_opened", "clicked"],

    // FOLLOWUP TRIGGERS
    // These events trigger the next followup to be scheduled
    followupTriggerEvents: ["delivered"],

    // RESCHEDULE TRIGGERS
    // These events cause automatic rescheduling
    rescheduleEvents: ["soft_bounce", "deferred"],
    rescheduleDelayHours: 2, // Hours before retry

    // SCORE ADJUSTMENT TRIGGERS
    scoreAdjustments: {
      opened: 5,
      unique_opened: 5,
      clicked: 10,
      delivered: 2,
      soft_bounce: -1,
      hard_bounce: -10,
      spam: -20,
      blocked: -20,
    },

    // WEBHOOK EVENTS TO PROCESS
    processedWebhookEvents: [
      "sent",
      "delivered",
      "opened",
      "unique_opened",
      "clicked",
      "soft_bounce",
      "hard_bounce",
      "blocked",
      "spam",
      "deferred",
      "error",
    ],
  },

  // ============================================================
  // SECTION 5: ACTION RULES
  // Side effects when certain actions are performed
  // ============================================================
  actionRules: {
    // WHEN SCHEDULING MANUAL MAIL
    manualMailActions: {
      pausePendingFollowups: true,
      cancelPendingFollowups: false,
      pausePendingConditionals: false,
      checkRateLimit: true,
      updateLeadStatus: true,
      createEventHistory: true,
    },

    // WHEN CONDITIONAL EMAIL TRIGGERS
    conditionalEmailActions: {
      preventDuplicates: true, // Don't create if already exists
      cancelPendingFollowupsIfConfigured: true, // Based on conditional email config
      pausePendingFollowupsIfConfigured: false,
      checkRateLimit: true,
      updateLeadStatus: true,
      createEventHistory: true,
    },

    // WHEN RESUMING FOLLOWUPS
    resumeFollowupActions: {
      checkForConditionalEmails: true, // Don't schedule if conditional exists
      checkForManualEmails: true, // Don't schedule if manual exists
      deleteOldPausedJobs: true,
      scheduleNextFollowup: true,
      updateLeadStatus: true,
      createEventHistory: true,
    },

    // WHEN PAUSING FOLLOWUPS
    pauseFollowupActions: {
      pauseOnlyFollowups: true, // Don't pause conditionals or manuals
      updateJobStatus: true,
      updateLeadStatus: true,
      createEventHistory: true,
    },

    // WHEN JOB IS CANCELLED
    cancelJobActions: {
      lookForNextScheduledJob: true,
      updateLeadStatusToNextJob: true,
      setIdleIfNoJobs: true,
      createEventHistory: true,
    },

    // WHEN FREEZING LEAD
    freezeActions: {
      cancelAllPendingJobs: true,
      setLeadStatusToFrozen: true,
      createEventHistory: true,
    },

    // WHEN UNFREEZING LEAD
    unfreezeActions: {
      rescheduleNextFollowup: true,
      updateLeadStatus: true,
      createEventHistory: true,
    },

    // WHEN CONVERTING LEAD
    convertActions: {
      cancelAllPendingJobs: true,
      setLeadStatusToConverted: true,
      preventFutureEmails: true,
      createEventHistory: true,
    },

    // WHEN RETRYING A JOB
    retryJobActions: {
      createNewJob: true,
      copyOriginalJobDetails: true,
      incrementRetryCount: true,
      checkMaxRetries: true,
      updateLeadStatus: true,
      createEventHistory: true,
    },

    // WHEN RESCHEDULING A JOB
    rescheduleJobActions: {
      cancelOldJob: true,
      createNewJob: true,
      markAsRescheduled: true,
      checkRateLimit: true,
      updateLeadStatus: true,
      createEventHistory: true,
    },
  },

  // ============================================================
  // SECTION 6: DISPLAY RULES
  // How things should be displayed in the UI
  // ============================================================
  displayRules: {
    // TIMELINE DISPLAY
    groupTimelineByType: true, // Show one box per mail type
    showCancelledInTimeline: false, // Hide cancelled jobs
    prioritizeActiveJobs: true, // Show pending/scheduled over cancelled

    // TYPE NAME DISPLAY
    useSimplifiedTypeNames: true, // "Followup Mail" instead of "First Followup"

    // STATUS FORMAT
    leadStatusFormat: "{type}:{status}", // "Followup Mail:scheduled"
    queueStatusFormat: "{type}:{status}", // "First Followup:Pending"

    // CONDITIONAL EMAIL STATUS FORMAT
    conditionalStatusFormat: "condition {trigger}:{status}", // "condition opened:scheduled"
    useConditionFormatInQueue: true,
    useConditionFormatInLeadStatus: true, // Use "condition opened:scheduled" format for lead status

    // DATE/TIME DISPLAY
    useRelativeTime: true, // "in 2 hours" instead of "10:30 AM"
    timezone: "lead", // Use lead's timezone for display
    dateFormat: "MMM DD, YYYY",
    timeFormat: "hh:mm A",
  },

  // ============================================================
  // SECTION 7: VALIDATION RULES
  // Rules for validating data and preventing errors
  // ============================================================
  validationRules: {
    // DUPLICATE PREVENTION
    preventDuplicateConditionals: true,
    preventDuplicateFollowups: true,
    preventDuplicateInitials: true,

    // RATE LIMITING
    enforceRateLimits: true,
    maxEmailsPerWindow: 2,
    windowMinutes: 15,

    // BUSINESS HOURS
    enforceBusinessHours: true,
    defaultBusinessHoursStart: 9,
    defaultBusinessHoursEnd: 18,

    // RETRY LIMITS
    maxRetriesPerJob: 3,
    retryDelayMinutes: 30,
  },

  // ============================================================
  // SECTION 8: NOTIFICATION RULES
  // When to send notifications
  // ============================================================
  notificationRules: {
    notifyOnBounce: true,
    notifyOnBlock: true,
    notifyOnSpam: true,
    notifyOnHighEngagement: true, // Multiple clicks/opens
    notifyOnSequenceComplete: false,
    notifyOnConversion: true,
  },

  // ============================================================
  // SECTION 9: STATUS GROUPS
  // Single Source of Truth for all status arrays used in queries
  // ANY code that checks job status MUST use these groups
  // ============================================================
  statusGroups: {
    // Active/Pending jobs - waiting to be sent
    active: ["pending", "queued", "scheduled", "rescheduled", "deferred"],

    // Jobs that can be cancelled by user
    cancellable: ["pending", "queued", "scheduled", "rescheduled", "paused"],

    // Jobs that can be rescheduled
    reschedulable: [
      "pending",
      "queued",
      "scheduled",
      "rescheduled",
      "deferred",
      "failed",
      "soft_bounce",
      "hard_bounce",
      "blocked",
    ],

    // Failure statuses - job failed to deliver
    failure: ["blocked", "failed", "hard_bounce", "spam", "bounced"],

    // Hard failure - cannot retry automatically
    hardFailure: ["blocked", "hard_bounce", "spam"],

    // Soft failure - can retry automatically
    softFailure: ["soft_bounce", "deferred"],

    // Terminal statuses - job lifecycle complete
    terminal: [
      "delivered",
      "opened",
      "clicked",
      "cancelled",
      "skipped",
      "hard_bounce",
      "blocked",
      "spam",
    ],

    // Sent but not terminal - waiting for engagement
    sentNotTerminal: ["sent", "delivered", "opened", "clicked"],

    // All non-terminal statuses that count as "in progress"
    inProgress: [
      "pending",
      "queued",
      "scheduled",
      "rescheduled",
      "sent",
      "delivered",
      "opened",
      "clicked",
    ],

    // Statuses for duplicate checking (don't create new job if exists with these)
    existingNonCancelled: [
      "pending",
      "queued",
      "scheduled",
      "rescheduled",
      "sent",
      "delivered",
      "opened",
      "clicked",
    ],

    // Engagement events
    engagement: ["opened", "unique_opened", "clicked"],

    // Statuses that should show in email queue
    showInQueue: [
      "pending",
      "queued",
      "scheduled",
      "rescheduled",
      "deferred",
      "paused",
    ],

    // Processed/completed statuses for metrics
    processed: [
      "sent",
      "delivered",
      "opened",
      "clicked",
      "cancelled",
      "skipped",
      "hard_bounce",
      "soft_bounce",
      "spam",
      "blocked",
      "failed",
      "error",
      "invalid",
      "complaint",
      "unsubscribed",
    ],

    // Retriable statuses - terminal failures that can be manually retried
    retriable: [
      "failed",
      "bounced",
      "soft_bounce",
      "hard_bounce",
      "cancelled",
      "deferred",
      "blocked",
      "spam",
      "rescheduled",
      "error",
      "complaint",
      "unsubscribed",
    ],

    // AUTO-RETRIABLE: System automatically retries with exponential backoff
    autoRetryable: ["soft_bounce", "deferred"],

    // MANUAL-RETRY-ONLY: User must manually trigger retry
    manualRetryOnly: [
      "hard_bounce",
      "blocked",
      "error",
      "invalid",
      "spam",
      "complaint",
      "unsubscribed",
      "failed",
    ],

    // NEGATIVE STATUSES: Show in complaint/unsubscribed page, pause future mails
    negative: ["complaint", "unsubscribed"],

    // Pending only (for email queue filtering - active jobs awaiting delivery)
    pendingOnly: ["pending", "queued"],

    // Completed/history statuses (for showing in completed email list)
    completedHistory: [
      "sent",
      "delivered",
      "opened",
      "clicked",
      "bounced",
      "hard_bounce",
      "soft_bounce",
      "failed",
      "cancelled",
      "rescheduled",
      "error",
      "invalid",
      "complaint",
      "unsubscribed",
    ],

    // Awaiting delivery (for bounce/click handlers - jobs to cancel when engagement received)
    awaitingDelivery: ["pending", "queued", "deferred"],

    // Successfully sent statuses (for metrics and follow-up calculations)
    successfullySent: ["sent", "delivered", "opened", "clicked"],

    // All statuses except cancelled and skipped (for analytics counts)
    allExceptCancelledSkipped: [
      "sent",
      "delivered",
      "opened",
      "clicked",
      "bounced",
      "failed",
      "hard_bounce",
      "scheduled",
      "soft_bounce",
      "deferred",
      "blocked",
      "spam",
      "error",
      "invalid",
      "complaint",
      "unsubscribed",
      "dead",
    ],

    // TERMINAL STATES: Lead journey has ended (for Terminal States page)
    terminalStates: ["dead", "unsubscribed", "complaint"],

    // PAUSES FUTURE MAILS: No more emails should be sent
    pausesFutureMails: ["dead", "unsubscribed", "complaint"],

    // AUTO-RESUME ON STATUS: Which statuses trigger resuming paused jobs
    // Only 'delivered' and 'cancelled' should trigger resume
    // NOTE: 'soft_bounce', 'rescheduled' should NOT trigger resume (mail still active)
    autoResumeOnStatus: ["delivered", "cancelled"],

    // FAILURE STATUSES: Require manual intervention before resuming
    failureStatuses: [
      "hard_bounce",
      "blocked",
      "spam",
      "invalid",
      "error",
      "failed",
    ],
  },

  // ============================================================
  // SECTION 10: EVENT CATEGORIES
  // Single Source of Truth for classifying webhook events
  // User-configurable: Which events fall into which category
  // THIS DRIVES ALL HANDLER BEHAVIOR
  // ============================================================
  eventCategories: {
    // SUCCESS EVENTS: Increment score, trigger next steps
    // Handler: scheduleNext, triggerConditional
    success: {
      events: ["delivered", "opened", "clicked"],
      description: "Positive engagement events that advance the lead journey",
      scoreAdjustments: {
        delivered: 2,
        opened: 5,
        clicked: 10,
      },
      actions: {
        delivered: ["scheduleNextMail", "resumePausedMails"],
        opened: ["triggerConditional", "pauseOtherMails", "incrementScore"],
        clicked: [
          "triggerConditional",
          "pauseOtherMails",
          "incrementScore",
          "cancelFollowups",
        ],
      },
    },

    // AUTO-RESCHEDULE EVENTS: System automatically retries
    // Handler: incrementRetry, checkLimit, scheduleWithDelay
    autoReschedule: {
      events: ["soft_bounce", "deferred"],
      description: "Temporary failures that auto-reschedule with smart delay",
      actions: [
        "incrementRetryCount",
        "checkRetryLimit",
        "calculateSmartDelay",
        "reschedule",
      ],
      onMaxRetriesExceeded: "markAsDead",
    },

    // SPAM EVENTS: Lead opted out or complained
    // Handler: pauseAll, updateTerminalState
    spam: {
      events: ["unsubscribed", "complaint"],
      description:
        "Lead marked as spam or unsubscribed - STOP all future mails",
      actions: [
        "pauseAllFutureMails",
        "updateLeadTerminalState",
        "addToTerminalPage",
      ],
      allowManualRetry: true,
      requiresConfirmation: true,
    },

    // FAILED EVENTS: Delivery failed - manual retry only
    // Handler: pauseAll, addToFailedOutreach
    failed: {
      events: ["hard_bounce", "blocked", "error", "invalid"],
      description: "Hard failures - pause all, manual retry only",
      actions: ["pauseAllScheduling", "addToFailedOutreach"],
      autoRetry: false,
      allowManualRetry: true,
      onMaxRetriesExceeded: "markAsDead",
    },
  },

  // ============================================================
  // SECTION 11: DEAD MAIL RULES
  // Configuration for handling leads that exceed retry limits
  // ============================================================
  deadMailRules: {
    // When to mark a lead as dead
    triggerConditions: {
      maxRetriesExceeded: true,
      applyToCategories: ["autoReschedule", "failed"],
    },

    // Actions when lead is marked dead
    actions: {
      updateLeadStatus: "dead",
      setTerminalState: "dead",
      cancelAllPendingJobs: true,
      createEventHistory: true,
      createNotification: true,
      notificationType: "warning",
      notificationMessage:
        "Lead marked as dead after exceeding max retry limit",
    },

    // Resurrection (allow retry after fixing issue)
    resurrection: {
      enabled: true,
      resetRetryCount: true,
      clearTerminalState: true,
      requiresUserConfirmation: true,
    },
  },

  // ============================================================
  // SECTION 12: ACTION IMPACT MAPS
  // Defines what each action affects in the system
  // Used by action executors to ensure ALL side effects are handled
  // THIS IS THE SINGLE SOURCE OF TRUTH FOR ACTION BEHAVIOR
  // ============================================================
  actionImpacts: {
    // CANCEL JOB - Manual (user clicks cancel)
    cancelJobManual: {
      description: "User manually cancels a pending job",
      jobStatusChange: "cancelled",
      removeFromQueue: true,
      updateLeadStatus: true, // MUST look for next scheduled job
      createEventHistory: true,
      eventType: "job_cancelled_manual",
      leadStatusFallback: "idle", // If no other jobs found
      lookForNextScheduledJob: true,
    },

    // CANCEL JOB - Dynamic (system cancels due to priority)
    cancelJobDynamic: {
      description:
        "System cancels job due to higher priority job being scheduled",
      jobStatusChange: "cancelled",
      removeFromQueue: true,
      updateLeadStatus: false, // Higher priority job will handle status
      createEventHistory: true,
      eventType: "job_cancelled_priority",
      preserveHigherPriorityStatus: true,
      lookForNextScheduledJob: false,
    },

    // SKIP JOB
    skipJob: {
      description: "User skips a followup in the sequence",
      jobStatusChange: "skipped",
      removeFromQueue: true,
      updateLeadStatus: true,
      createEventHistory: true,
      eventType: "job_skipped",
      scheduleNextFollowup: true, // Schedule the next step in sequence
      leadStatusFallback: "idle",
      lookForNextScheduledJob: true,
    },

    // PAUSE FOLLOWUPS
    pauseFollowups: {
      description: "User pauses all followups for a lead",
      jobStatusChange: "cancelled", // Cancel the pending followup jobs
      affectedTypes: ["followup"], // Only affects followup-type jobs
      excludeTypes: ["conditional", "manual", "initial"], // Don't touch these
      removeFromQueue: true,
      updateLeadStatus: true,
      createEventHistory: true,
      eventType: "followups_paused",
      setLeadFlag: "followupsPaused",
      leadStatusFallback: "idle",
      lookForNextScheduledJob: true, // Check for conditional/manual to show
    },

    // RESUME FOLLOWUPS
    resumeFollowups: {
      description: "User resumes followups for a lead",
      removeFromQueue: false,
      updateLeadStatus: true,
      createEventHistory: true,
      eventType: "followups_resumed",
      clearLeadFlag: "followupsPaused",
      scheduleNextFollowup: true,
      checkForBlockingJobs: true, // Don't schedule if manual/conditional exists
      blockingJobTypes: ["manual", "conditional"],
      lookForNextScheduledJob: true,
    },

    // RETRY JOB
    retryJob: {
      description: "User retries a failed job",
      oldJobStatusChange: "rescheduled", // Mark old job as rescheduled
      createNewJob: true,
      incrementRetryCount: true,
      removeFromQueue: false, // Old job already removed from queue
      updateLeadStatus: true,
      createEventHistory: true,
      eventType: "job_retried",
      checkMaxRetries: true,
      maxRetries: 3,
    },

    // RESCHEDULE JOB
    rescheduleJob: {
      description: "User or system reschedules a job to new time",
      oldJobStatusChange: "rescheduled",
      createNewJob: true,
      removeFromQueue: true,
      updateLeadStatus: true,
      createEventHistory: true,
      eventType: "job_rescheduled",
    },

    // FREEZE LEAD
    freezeLead: {
      description: "User freezes a lead temporarily",
      cancelAllPendingJobs: true,
      removeFromQueue: true,
      updateLeadStatus: true,
      createEventHistory: true,
      eventType: "lead_frozen",
      forcedLeadStatus: "frozen",
      setLeadFlag: "frozenUntil",
    },

    // UNFREEZE LEAD
    unfreezeLead: {
      description: "User unfreezes a lead or freeze expires",
      updateLeadStatus: true,
      createEventHistory: true,
      eventType: "lead_unfrozen",
      clearLeadFlag: "frozenUntil",
      scheduleNextFollowup: true,
      leadStatusFallback: "idle",
      lookForNextScheduledJob: true,
    },

    // CONVERT LEAD
    convertLead: {
      description: "User marks lead as converted/won",
      cancelAllPendingJobs: true,
      removeFromQueue: true,
      updateLeadStatus: true,
      createEventHistory: true,
      eventType: "lead_converted",
      forcedLeadStatus: "converted",
      preventFutureScheduling: true,
    },

    // SCHEDULE CONDITIONAL EMAIL
    // Complete flow for scheduling a conditional email when triggered
    scheduleConditionalEmail: {
      description:
        "System schedules a conditional email after trigger event (opened/clicked)",

      // STEP 1: Check for existing scheduled jobs
      checkForExistingJobs: true,
      existingJobTypes: ["followup", "manual"], // Types to check/pause

      // STEP 2: If pending job exists, pause it and remove from queue
      pausePendingJobs: true,
      updatePausedJobStatus: "paused", // Status for paused jobs
      removeFromQueue: true,
      createPauseEventHistory: true,

      // STEP 3: Schedule the conditional email
      createConditionalJob: true,
      conditionalJobStatus: "scheduled", // NOT 'pending' - use 'scheduled'
      addToBullmqQueue: true,

      // STEP 4: Update lead status with trigger format
      updateLeadStatus: true,
      leadStatusFormat: "condition {triggerEvent}:{status}", // e.g., 'condition opened:scheduled'

      // STEP 5: Create event history
      createEventHistory: true,
      eventType: "conditional_scheduled",

      // STEP 6: Update emailSchedule (Sequence Progress)
      updateEmailSchedule: true,

      // Error handling
      rollbackOnFailure: true,
    },
  },

  // ============================================================
  // SECTION 11: RETRY RULES
  // Comprehensive retry configuration for different failure types
  // ============================================================
  retryRules: {
    // AUTO-RETRY CONFIGURATION
    // Used for soft_bounce and deferred statuses
    autoRetry: {
      enabled: true,
      maxAutoRetries: 3, // Maximum auto-retry attempts

      // Exponential backoff configuration
      backoff: {
        type: "exponential", // 'fixed' or 'exponential'
        initialDelayMinutes: 30, // First retry after 30 mins
        multiplier: 2, // Double delay each retry
        maxDelayHours: 24, // Cap at 24 hours between retries
      },

      // Statuses that trigger auto-retry
      triggerStatuses: ["soft_bounce", "deferred"],

      // Actions on auto-retry
      onRetry: {
        incrementRetryCount: true,
        createEventHistory: true,
        eventType: "auto_retry",
        updateJobStatus: "rescheduled",
        updateLeadStatus: false, // Don't change lead status on auto-retry
      },

      // Escalation when max retries exceeded
      onMaxRetriesExceeded: {
        updateJobStatus: "failed",
        updateLeadStatus: true,
        createEventHistory: true,
        createNotification: true,
        notificationType: "warning",
        notificationMessage: "Email failed after maximum auto-retry attempts",
      },
    },

    // MANUAL RETRY CONFIGURATION
    // Used for hard_bounce, blocked, error, invalid, complaint, unsubscribed
    manualRetry: {
      enabled: true,
      maxManualRetries: 5, // User can retry up to 5 times

      // Statuses that allow manual retry
      allowedStatuses: [
        "hard_bounce",
        "blocked",
        "error",
        "failed",
        "cancelled",
        "spam",
        "complaint",
        "unsubscribed",
      ],

      // Statuses that CANNOT be retried even manually
      forbiddenStatuses: ["invalid", "skipped"],

      // Actions on manual retry
      onRetry: {
        incrementRetryCount: true,
        createEventHistory: true,
        eventType: "manual_retry",
        updateLeadStatus: true,
        checkForConflicts: true, // Check for existing scheduled jobs
      },
    },

    // CONFLICT RESOLUTION
    // What happens when retry conflicts with existing scheduled job
    conflictResolution: {
      // Priority comparison
      compareByPriority: true,

      // If retry has LOWER priority than existing job
      lowerPriorityAction: "block", // 'block' | 'queue_after' | 'cancel_existing'
      lowerPriorityMessage:
        "Cannot retry: A higher priority {existingType} is already scheduled",

      // If retry has HIGHER priority than existing job
      higherPriorityAction: "prompt", // 'prompt' | 'auto_cancel' | 'block'
      higherPriorityMessage:
        "A {existingType} is scheduled. Cancel it to retry this email?",

      // If retry has SAME priority
      samePriorityAction: "block",
      samePriorityMessage:
        "Cannot retry: A {existingType} of same priority is already scheduled",
    },
  },

  // ============================================================
  // SECTION 12: QUEUE WATCHER RULES
  // Prevents duplicate scheduling and ensures queue integrity
  // ============================================================
  queueWatcherRules: {
    // DUPLICATE PREVENTION
    duplicatePrevention: {
      enabled: true,

      // Rule: Same lead cannot have multiple active jobs
      maxActiveJobsPerLead: 1,

      // What counts as "active" for duplicate checking
      activeStatuses: [
        "pending",
        "queued",
        "scheduled",
        "rescheduled",
        "deferred",
      ],

      // What to do when duplicate detected
      onDuplicateDetected: "block", // 'block' | 'queue_later' | 'cancel_older'

      // Exception: Paused jobs don't count as active
      excludeStatuses: ["paused", "cancelled", "skipped"],
    },

    // PRIORITY SCHEDULING
    priorityScheduling: {
      enabled: true,

      // Mail type priorities (higher = more important)
      priorities: {
        conditional: 100,
        manual: 90,
        initial: 80,
        followup: 70,
      },

      // Higher priority can pause lower priority
      canPauseLowerPriority: true,
      pauseOnSchedule: true, // Automatically pause when higher priority scheduled
      resumeOnComplete: true, // Resume paused jobs when higher priority completes/cancels
    },

    // INTEGRITY CHECKS
    integrityChecks: {
      enabled: true,
      checkInterval: 60000, // Check every minute

      // Check for orphaned jobs (no lead exists)
      detectOrphanedJobs: true,
      orphanAction: "cancel",

      // Check for stuck jobs (pending too long)
      detectStuckJobs: true,
      stuckThresholdHours: 48,
      stuckAction: "notify",

      // Check for invalid state combinations
      validateStatusTransitions: true,
    },
  },

  // ============================================================
  // SECTION 13: FOLLOWUP-SPECIFIC RULES
  // Special rules for followup mail behavior (pause/resume/skip)
  // ============================================================
  followupRules: {
    // PAUSE BEHAVIOR
    pause: {
      // What pause does
      updateJobStatus: true,
      jobStatusValue: "paused",

      // CRITICAL: Pause does NOT update lead status
      updateLeadStatus: false,

      // Only affects followup jobs
      affectedTypes: ["followup"],
      excludedTypes: ["initial", "manual", "conditional"],

      // History
      createEventHistory: true,
      eventType: "followups_paused",

      // Lead flag
      setLeadFlag: { key: "followupsPaused", value: true },
    },

    // RESUME BEHAVIOR
    resume: {
      // What resume does
      deleteOldPausedJobs: true, // Remove the old paused job
      scheduleNextFollowup: true, // Create fresh job for next followup

      // CRITICAL: Resume does NOT update lead status directly
      // Status is updated when new job is scheduled
      updateLeadStatus: false,

      // Check for blocking jobs before resume
      checkForBlockingJobs: true,
      blockingTypes: ["manual", "conditional"],
      onBlockingJobExists: "skip", // Don't schedule if manual/conditional exists

      // History
      createEventHistory: true,
      eventType: "followups_resumed",

      // Clear flag
      clearLeadFlag: "followupsPaused",
    },

    // SKIP BEHAVIOR
    skip: {
      // What skip does
      updateJobStatus: true,
      jobStatusValue: "skipped",

      // Schedule next followup in sequence
      scheduleNextFollowup: true,

      // If no next followup exists
      onNoNextFollowup: {
        updateLeadStatus: true,
        leadStatusValue: "idle",
      },

      // History
      createEventHistory: true,
      eventType: "followup_skipped",

      // Add to skipped list
      addToSkippedList: true,
    },

    // REVERT SKIP BEHAVIOR
    revertSkip: {
      // What revert does
      removeFromSkippedList: true,
      scheduleSkippedFollowup: true,

      // Check for blocking jobs
      checkForBlockingJobs: true,
      blockingTypes: ["manual", "conditional"],

      // History
      createEventHistory: true,
      eventType: "skip_reverted",
    },
  },
};

// ========================================
// RULEBOOK SERVICE
// ========================================

class RulebookService {
  constructor() {
    this._cachedRulebook = null;
    this._cacheTimestamp = null;
    this._cacheTTL = 60000; // 1 minute cache
  }

  /**
   * Get the current rulebook (user-configured or default)
   */
  async getRulebook() {
    // Check cache
    if (
      this._cachedRulebook &&
      Date.now() - this._cacheTimestamp < this._cacheTTL
    ) {
      return this._cachedRulebook;
    }

    try {
      // Try to get user-configured rulebook from settings
      const settings = await prisma.settings.findFirst();

      if (settings?.rulebook) {
        // Merge with defaults to ensure all keys exist
        this._cachedRulebook = this._deepMerge(
          DEFAULT_RULEBOOK,
          settings.rulebook,
        );
      } else {
        this._cachedRulebook = { ...DEFAULT_RULEBOOK };
      }

      this._cacheTimestamp = Date.now();
      return this._cachedRulebook;
    } catch (error) {
      console.error("[RulebookService] Error loading rulebook:", error);
      return { ...DEFAULT_RULEBOOK };
    }
  }

  /**
   * Get default rulebook
   */
  getDefaultRulebook() {
    return JSON.parse(JSON.stringify(DEFAULT_RULEBOOK));
  }

  /**
   * Update rulebook with user configuration
   */
  async updateRulebook(updates) {
    try {
      const currentRulebook = await this.getRulebook();
      const newRulebook = this._deepMerge(currentRulebook, updates);
      newRulebook.lastUpdated = new Date().toISOString();

      // Update in settings
      await prisma.settings.updateMany({
        data: { rulebook: newRulebook },
      });

      // Clear cache
      this._cachedRulebook = null;

      console.log("[RulebookService] Rulebook updated");
      return newRulebook;
    } catch (error) {
      console.error("[RulebookService] Error updating rulebook:", error);
      throw error;
    }
  }

  /**
   * Reset rulebook to defaults
   */
  async resetRulebook() {
    try {
      const defaultCopy = JSON.parse(JSON.stringify(DEFAULT_RULEBOOK));
      defaultCopy.lastUpdated = new Date().toISOString();

      await prisma.settings.updateMany({
        data: { rulebook: defaultCopy },
      });

      // Clear cache
      this._cachedRulebook = null;

      console.log("[RulebookService] Rulebook reset to defaults");
      return defaultCopy;
    } catch (error) {
      console.error("[RulebookService] Error resetting rulebook:", error);
      throw error;
    }
  }

  /**
   * Deep merge two objects
   */
  _deepMerge(target, source) {
    const result = { ...target };

    for (const key in source) {
      if (
        source[key] &&
        typeof source[key] === "object" &&
        !Array.isArray(source[key])
      ) {
        result[key] = this._deepMerge(result[key] || {}, source[key]);
      } else {
        result[key] = source[key];
      }
    }

    return result;
  }

  // ========================================
  // HELPER METHODS - TYPE RESOLUTION
  // ========================================

  /**
   * Get mail type definition from internal type string
   */
  getMailType(typeString) {
    if (!typeString) return this.getDefaultRulebook().mailTypes.followup;

    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    const t = typeString.toLowerCase();

    // Check each mail type
    for (const [key, mailType] of Object.entries(rulebook.mailTypes)) {
      // Check if matches any internal type
      if (mailType.internalTypes.some((it) => t.includes(it.toLowerCase()))) {
        return mailType;
      }
    }

    // Default to followup
    return rulebook.mailTypes.followup;
  }

  /**
   * Get simplified type name for display
   * For followups, returns the actual followup name (e.g., "First Followup")
   * For other types, returns the generic display name
   * NOTE: For conditional emails, prefer getDisplayTypeName() which uses metadata for proper formatting
   */
  getSimplifiedTypeName(type, metadata = null) {
    if (!type) return "Email";

    const mailType = this.getMailType(type);

    // For followups, return the actual type name instead of generic "Followup Mail"
    // This ensures status shows "First Followup:scheduled" not "Followup Mail:scheduled"
    if (mailType.id === "followup") {
      // Check if it's a specific followup name (matches one of the internalTypes)
      const isSpecificFollowup = mailType.internalTypes.some(
        (it) =>
          type.toLowerCase() === it.toLowerCase() &&
          it.toLowerCase() !== "followup",
      );
      if (isSpecificFollowup) {
        return type; // Return actual name like "First Followup"
      }
    }

    // For conditionals, try to use trigger event from metadata for proper display
    if (mailType.id === "conditional" && type.startsWith("conditional:")) {
      // If metadata has triggerEvent, format as "conditional {triggerEvent}"
      if (metadata?.triggerEvent) {
        return `conditional ${metadata.triggerEvent}`;
      }
      // Fallback: return "conditional" without the name part
      return "conditional";
    }

    // For initial and manual, use the display name
    return mailType.displayName;
  }

  /**
   * Get display-friendly type name for UI (Email Queue, Schedule pages)
   * Uses job metadata for proper conditional email formatting
   * @param {Object} job - The EmailJob object with type and metadata
   * @returns {string} Display-friendly type name
   */
  getDisplayTypeName(job) {
    if (!job) return "Email";
    return this.getSimplifiedTypeName(job.type, job.metadata);
  }

  /**
   * Format status for display with proper type name
   * Used by Email Queue and Schedule pages
   * @param {Object} job - The EmailJob object
   * @returns {string} Formatted status like "condition opened:pending"
   */
  formatJobStatusForDisplay(job) {
    if (!job) return "unknown";

    const status = job.status || "pending";

    // Special handling for conditional emails - use "condition {triggerEvent}" format
    if (job.type?.startsWith("conditional:")) {
      // Try metadata.triggerEvent first
      let triggerEvent = job.metadata?.triggerEvent;

      // Fallback: Try metadata.trigger (alternate field name)
      if (!triggerEvent && job.metadata?.trigger) {
        triggerEvent = job.metadata.trigger;
      }

      // Fallback: Try metadata.conditionType
      if (!triggerEvent && job.metadata?.conditionType) {
        triggerEvent = job.metadata.conditionType;
      }

      if (triggerEvent) {
        return `condition ${triggerEvent}:${status}`;
      }

      // Final fallback: Return generic conditional format
      // The triggerEvent will be added when we have async lookup ability
      return `conditional:${status}`;
    }

    // For other types, use the display type name
    const typeName = this.getDisplayTypeName(job);
    return `${typeName}:${status}`;
  }

  /**
   * Format status for display with async lookup for missing triggerEvent
   * @param {Object} job - The EmailJob object
   * @returns {Promise<string>} Formatted status like "condition opened:pending"
   */
  async formatJobStatusForDisplayAsync(job) {
    if (!job) return "unknown";

    const status = job.status || "pending";

    // Special handling for conditional emails
    if (job.type?.startsWith("conditional:")) {
      let triggerEvent =
        job.metadata?.triggerEvent ||
        job.metadata?.trigger ||
        job.metadata?.conditionType;

      // If not in metadata, try to look up from conditional email settings
      if (!triggerEvent && job.metadata?.conditionalEmailId) {
        try {
          const conditionalEmail = await prisma.conditionalEmail.findUnique({
            where: { id: job.metadata.conditionalEmailId },
            select: { triggerEvent: true },
          });
          if (conditionalEmail?.triggerEvent) {
            triggerEvent = conditionalEmail.triggerEvent;
          }
        } catch (e) {
          console.warn(
            `[formatJobStatusForDisplayAsync] Lookup failed:`,
            e.message,
          );
        }
      }

      // Still not found? Try extracting from job type (e.g., "conditional:Thank You Mail" -> lookup by name)
      if (!triggerEvent) {
        try {
          const conditionalName = job.type.replace("conditional:", "");
          const conditionalEmail = await prisma.conditionalEmail.findFirst({
            where: { name: conditionalName },
            select: { triggerEvent: true },
          });
          if (conditionalEmail?.triggerEvent) {
            triggerEvent = conditionalEmail.triggerEvent;
          }
        } catch (e) {
          // Ignore lookup errors
        }
      }

      if (triggerEvent) {
        return `condition ${triggerEvent}:${status}`;
      }

      return `conditional:${status}`;
    }

    const typeName = this.getDisplayTypeName(job);
    return `${typeName}:${status}`;
  }

  /**
   * Check if type is a conditional email
   */
  isConditional(type) {
    if (!type) return false;
    return type.toLowerCase().startsWith("conditional:");
  }

  /**
   * Check if type is an initial email
   */
  isInitial(type) {
    if (!type) return false;
    return type.toLowerCase().includes("initial");
  }

  /**
   * Check if type is a manual email
   */
  isManual(type) {
    if (!type) return false;
    return type.toLowerCase() === "manual";
  }

  /**
   * Check if a mail type can be cancelled
   * Skippable types should use skip instead of cancel
   */
  canCancelType(type) {
    const mailType = this.getMailType(type);
    return mailType.canCancel === true;
  }

  /**
   * Check if a mail type can be skipped
   */
  canSkipType(type) {
    const mailType = this.getMailType(type);
    return mailType.canSkip === true;
  }

  /**
   * Check if a mail type can be paused
   */
  canPauseType(type) {
    const mailType = this.getMailType(type);
    return mailType.canPause === true;
  }

  /**
   * Check if a mail type can be retried
   */
  canRetryType(type) {
    const mailType = this.getMailType(type);
    return mailType.canRetry === true;
  }

  /**
   * Check if a mail type can be rescheduled
   */
  canRescheduleType(type) {
    const mailType = this.getMailType(type);
    return mailType.canReschedule === true;
  }

  /**
   * Get allowed actions for a mail type
   */
  getAllowedActions(type) {
    const mailType = this.getMailType(type);
    return {
      canSkip: mailType.canSkip || false,
      canCancel: mailType.canCancel || false,
      canPause: mailType.canPause || false,
      canRetry: mailType.canRetry || false,
      canReschedule: mailType.canReschedule || false,
    };
  }

  // ========================================
  // HELPER METHODS - STATUS RESOLUTION
  // ========================================

  /**
   * Get status display info
   */
  async getStatusDisplay(status) {
    const rulebook = await this.getRulebook();
    return (
      rulebook.statuses[status] || {
        display: status,
        color: "#64748b",
        bgColor: "rgba(100, 116, 139, 0.1)",
        isTerminal: false,
      }
    );
  }

  /**
   * Check if status is forbidden for lead display
   */
  async isStatusForbiddenForLead(status) {
    const rulebook = await this.getRulebook();
    return rulebook.leadStatusRules.forbiddenStatuses.includes(status);
  }

  /**
   * Check if status is a terminal state
   */
  async isTerminalStatus(status) {
    const rulebook = await this.getRulebook();
    return rulebook.statuses[status]?.isTerminal || false;
  }

  /**
   * Check if status is an active/pending state
   */
  async isActiveStatus(status) {
    const rulebook = await this.getRulebook();
    return rulebook.statuses[status]?.isActive || false;
  }

  /**
   * Get lead status priority
   */
  async getLeadStatusPriority(status) {
    const rulebook = await this.getRulebook();
    return rulebook.leadStatusRules.priority[status] || 0;
  }

  // ========================================
  // HELPER METHODS - TRIGGER CHECKS
  // ========================================

  /**
   * Check if event should trigger conditional evaluation
   */
  async shouldTriggerConditional(event) {
    const rulebook = await this.getRulebook();
    return rulebook.triggerRules.conditionalTriggerEvents.includes(event);
  }

  /**
   * Check if event should trigger followup scheduling
   */
  async shouldTriggerFollowup(event) {
    const rulebook = await this.getRulebook();
    return rulebook.triggerRules.followupTriggerEvents.includes(event);
  }

  /**
   * Check if event should trigger reschedule
   */
  async shouldReschedule(event) {
    const rulebook = await this.getRulebook();
    return rulebook.triggerRules.rescheduleEvents.includes(event);
  }

  /**
   * Get score adjustment for an event
   */
  async getScoreAdjustment(event) {
    const rulebook = await this.getRulebook();
    return rulebook.triggerRules.scoreAdjustments[event] || 0;
  }

  // ========================================
  // HELPER METHODS - ACTION RULES
  // ========================================

  /**
   * Get action rules for manual mail
   */
  async getManualMailActionRules() {
    const rulebook = await this.getRulebook();
    return rulebook.actionRules.manualMailActions;
  }

  /**
   * Get action rules for conditional emails
   */
  async getConditionalEmailActionRules() {
    const rulebook = await this.getRulebook();
    return rulebook.actionRules.conditionalEmailActions;
  }

  /**
   * Get action rules for resuming followups
   */
  async getResumeFollowupActionRules() {
    const rulebook = await this.getRulebook();
    return rulebook.actionRules.resumeFollowupActions;
  }

  /**
   * Get action rules for cancelling a job
   */
  async getCancelJobActionRules() {
    const rulebook = await this.getRulebook();
    return rulebook.actionRules.cancelJobActions;
  }

  // ========================================
  // HELPER METHODS - STATUS FORMATTING
  // ========================================

  /**
   * Format lead status according to rules
   * @param {string} type - Email type (e.g., "conditional:Thank You Mail", "First Followup")
   * @param {string} status - Status (e.g., "scheduled", "pending")
   * @param {Object} metadata - Optional metadata containing triggerEvent for conditionals
   */
  formatLeadStatus(type, status, metadata = {}) {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;

    // Check if conditional and should use special format
    if (
      this.isConditional(type) &&
      rulebook.displayRules.useConditionFormatInLeadStatus
    ) {
      const triggerEvent =
        metadata.triggerEvent || this._extractTriggerFromMetadata(metadata);
      if (triggerEvent) {
        return `condition ${triggerEvent}:${status}`;
      }
    }

    const simplifiedType = this.getSimplifiedTypeName(type);
    return `${simplifiedType}:${status}`;
  }

  /**
   * Format conditional email status specifically
   * Always uses "condition {trigger}:{status}" format
   * @param {string} triggerEvent - The trigger event (opened, clicked, etc.)
   * @param {string} status - The status (scheduled, pending, sent, etc.)
   */
  formatConditionalStatus(triggerEvent, status) {
    return `condition ${triggerEvent}:${status}`;
  }

  /**
   * Extract trigger event from metadata or job type
   */
  _extractTriggerFromMetadata(metadata) {
    if (metadata.triggerEvent) return metadata.triggerEvent;
    if (metadata.trigger) return metadata.trigger;
    // Try to extract from conditionalExpr or other fields
    return null;
  }

  /**
   * Format queue status according to rules
   */
  formatQueueStatus(type, status, metadata = {}) {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;

    // Check if conditional and should use special format
    if (
      this.isConditional(type) &&
      rulebook.displayRules.useConditionFormatInQueue &&
      metadata.triggerEvent
    ) {
      const displayStatus = status === "pending" ? "pending" : status;
      return `condition ${metadata.triggerEvent}:${displayStatus}`;
    }

    // Standard format
    const displayStatus =
      status.charAt(0).toUpperCase() + status.slice(1).replace("_", " ");
    return `${type}:${displayStatus}`;
  }

  // ========================================
  // HELPER METHODS - VALIDATION
  // ========================================

  /**
   * Check if status transition is allowed
   */
  async isTransitionAllowed(fromStatus, toStatus) {
    const rulebook = await this.getRulebook();
    const statusDef = rulebook.statuses[fromStatus];

    if (!statusDef) return true; // Unknown status, allow
    if (!statusDef.allowedTransitions) return true; // No restrictions

    return statusDef.allowedTransitions.includes(toStatus);
  }

  /**
   * Check if job can be retried
   */
  async canRetryJob(status, retryCount) {
    const rulebook = await this.getRulebook();
    const statusDef = rulebook.statuses[status];

    if (!statusDef) return false;
    if (!statusDef.canRetry) return false;

    return retryCount < rulebook.validationRules.maxRetriesPerJob;
  }

  // ========================================
  // ACTION VALIDATION METHODS
  // Call these before performing any action
  // ========================================

  /**
   * Validate if an action can be performed on a mail type
   * @param {string} action - 'skip' | 'cancel' | 'pause' | 'resume' | 'retry' | 'reschedule'
   * @param {string} type - Email type string
   * @param {string} status - Current job status
   * @returns {{allowed: boolean, reason?: string}}
   */
  validateAction(action, type, status) {
    const mailType = this.getMailType(type);
    const statusDef = (this._cachedRulebook || DEFAULT_RULEBOOK).statuses[
      status
    ];

    switch (action) {
      case "skip":
        if (!mailType.canSkip) {
          return {
            allowed: false,
            reason: `${mailType.displayName} cannot be skipped`,
          };
        }
        if (
          !["pending", "scheduled", "queued", "rescheduled"].includes(status)
        ) {
          return {
            allowed: false,
            reason: `Cannot skip job in ${status} status`,
          };
        }
        return { allowed: true };

      case "cancel":
        if (!mailType.canCancel) {
          return {
            allowed: false,
            reason: `${mailType.displayName} cannot be cancelled. Use skip for followups.`,
          };
        }
        if (
          !["pending", "scheduled", "queued", "rescheduled", "paused"].includes(
            status,
          )
        ) {
          return {
            allowed: false,
            reason: `Cannot cancel job in ${status} status`,
          };
        }
        return { allowed: true };

      case "pause":
        if (!mailType.canPause) {
          return {
            allowed: false,
            reason: `${mailType.displayName} cannot be paused`,
          };
        }
        if (
          !["pending", "scheduled", "queued", "rescheduled"].includes(status)
        ) {
          return {
            allowed: false,
            reason: `Cannot pause job in ${status} status`,
          };
        }
        return { allowed: true };

      case "resume":
        // Resume is only allowed if followups are paused
        if (!mailType.canPause) {
          return {
            allowed: false,
            reason: `${mailType.displayName} does not support pause/resume`,
          };
        }
        return { allowed: true };

      case "retry":
        if (!mailType.canRetry) {
          return {
            allowed: false,
            reason: `${mailType.displayName} cannot be retried`,
          };
        }
        if (!statusDef?.canRetry) {
          return {
            allowed: false,
            reason: `Cannot retry job in ${status} status`,
          };
        }
        return { allowed: true };

      case "reschedule":
        if (!mailType.canReschedule) {
          return {
            allowed: false,
            reason: `${mailType.displayName} cannot be rescheduled`,
          };
        }
        const reschedulableStatuses = [
          "pending",
          "scheduled",
          "queued",
          "rescheduled",
          "deferred",
          "failed",
          "soft_bounce",
        ];
        if (!reschedulableStatuses.includes(status)) {
          return {
            allowed: false,
            reason: `Cannot reschedule job in ${status} status`,
          };
        }
        return { allowed: true };

      default:
        return { allowed: false, reason: `Unknown action: ${action}` };
    }
  }

  /**
   * Get all allowed actions for a job
   * @param {string} type - Email type
   * @param {string} status - Current status
   * @returns {Object} Map of action -> {allowed, reason}
   */
  getAllowedActionsForJob(type, status) {
    const actions = [
      "skip",
      "cancel",
      "pause",
      "resume",
      "retry",
      "reschedule",
    ];
    const result = {};

    for (const action of actions) {
      result[action] = this.validateAction(action, type, status);
    }

    return result;
  }

  /**
   * Get retry configuration for a status
   * @param {string} status - Job status
   * @returns {Object} Retry configuration
   */
  getRetryConfig(status) {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    const retryRules = rulebook.retryRules;

    // Check if auto-retry
    if (retryRules.autoRetry.triggerStatuses.includes(status)) {
      return {
        type: "auto",
        maxRetries: retryRules.autoRetry.maxAutoRetries,
        backoff: retryRules.autoRetry.backoff,
        onRetry: retryRules.autoRetry.onRetry,
        onMaxExceeded: retryRules.autoRetry.onMaxRetriesExceeded,
      };
    }

    // Check if manual retry allowed
    if (retryRules.manualRetry.allowedStatuses.includes(status)) {
      return {
        type: "manual",
        maxRetries: retryRules.manualRetry.maxManualRetries,
        onRetry: retryRules.manualRetry.onRetry,
      };
    }

    // Check if forbidden
    if (retryRules.manualRetry.forbiddenStatuses.includes(status)) {
      return {
        type: "forbidden",
        reason: "This status cannot be retried",
      };
    }

    return { type: "unknown" };
  }

  /**
   * Calculate retry delay using exponential backoff
   * @param {number} retryCount - Current retry count
   * @returns {number} Delay in milliseconds
   */
  calculateRetryDelay(retryCount) {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    const backoff = rulebook.retryRules.autoRetry.backoff;

    const delayMinutes =
      backoff.initialDelayMinutes * Math.pow(backoff.multiplier, retryCount);
    const maxDelayMs = backoff.maxDelayHours * 60 * 60 * 1000;
    const delayMs = Math.min(delayMinutes * 60 * 1000, maxDelayMs);

    return delayMs;
  }

  /**
   * Check if status requires special handling (complaint/unsubscribed)
   * @param {string} status - Job/Lead status
   * @returns {{pausesFuture: boolean, showInComplaintPage: boolean}}
   */
  getNegativeStatusHandling(status) {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    const statusDef = rulebook.statuses[status];

    return {
      pausesFuture: statusDef?.pausesFutureMails || false,
      showInComplaintPage: statusDef?.showInComplaintPage || false,
      isNegative: rulebook.statusGroups.negative.includes(status),
    };
  }

  /**
   * Get auto-retry statuses
   */
  getAutoRetryableStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.autoRetryable];
  }

  /**
   * Get manual-retry-only statuses
   */
  getManualRetryOnlyStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.manualRetryOnly];
  }

  /**
   * Get negative statuses (complaint/unsubscribed)
   */
  getNegativeStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.negative];
  }

  // ========================================
  // EVENT CATEGORY METHODS
  // Single source of truth for event classification
  // ========================================

  /**
   * Get event category for a given event type
   * @param {string} eventType - The webhook event type
   * @returns {Object} Category info with name and config
   */
  getEventCategory(eventType) {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    const categories = rulebook.eventCategories;

    for (const [categoryName, config] of Object.entries(categories)) {
      if (config.events.includes(eventType)) {
        return {
          name: categoryName,
          ...config,
        };
      }
    }

    return { name: "unknown", events: [], actions: [] };
  }

  /**
   * Check if event is a success event
   */
  isSuccessEvent(eventType) {
    return this.getEventCategory(eventType).name === "success";
  }

  /**
   * Check if event is an auto-reschedule event
   */
  isAutoRescheduleEvent(eventType) {
    return this.getEventCategory(eventType).name === "autoReschedule";
  }

  /**
   * Check if event is a spam event
   */
  isSpamEvent(eventType) {
    return this.getEventCategory(eventType).name === "spam";
  }

  /**
   * Check if event is a failed event
   */
  isFailedEvent(eventType) {
    return this.getEventCategory(eventType).name === "failed";
  }

  /**
   * Get actions for an event type
   * @param {string} eventType - The event type
   * @returns {string[]} Array of action names to execute
   */
  getEventActions(eventType) {
    const category = this.getEventCategory(eventType);
    if (
      category.actions &&
      typeof category.actions === "object" &&
      !Array.isArray(category.actions)
    ) {
      return category.actions[eventType] || [];
    }
    return category.actions || [];
  }

  /**
   * Get score adjustment for an event
   */
  getEventScoreAdjustment(eventType) {
    const category = this.getEventCategory(eventType);
    if (category.scoreAdjustments) {
      return category.scoreAdjustments[eventType] || 0;
    }
    return 0;
  }

  // ========================================
  // TERMINAL STATE METHODS
  // ========================================

  /**
   * Get all terminal state types
   */
  getTerminalStates() {
    return [...DEFAULT_RULEBOOK.statusGroups.terminalStates];
  }

  /**
   * Check if status is a terminal state (for Terminal States page)
   */
  isTerminalState(status) {
    return this.getTerminalStates().includes(status);
  }

  /**
   * Get statuses that pause future mails
   */
  getPausesFutureMailsStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.pausesFutureMails];
  }

  /**
   * Check if status should pause all future mails
   */
  shouldPauseFutureMails(status) {
    return this.getPausesFutureMailsStatuses().includes(status);
  }

  /**
   * Get failure statuses that require manual intervention
   * These block auto-resume until manually cleared
   */
  getFailureStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.failureStatuses];
  }

  /**
   * Check if status is a failure requiring manual intervention
   */
  isFailureStatus(status) {
    return this.getFailureStatuses().includes(status);
  }

  // ========================================
  // DYNAMIC RETRY LIMIT METHODS
  // Gets max retries from Settings, not hardcoded
  // ========================================

  /**
   * Get maximum retries for a mail type from Settings
   * Priority: Settings per-type -> Settings global -> Rulebook default
   * @param {string} mailType - The mail type
   * @returns {Promise<number>} Max retry count
   */
  async getMaxRetries(mailType) {
    try {
      const { SettingsRepository } = require("../repositories");
      const settings = await SettingsRepository.getSettings();

      // Get mail type config from rulebook
      const mailTypeConfig = this.getMailType(mailType);

      // Priority 1: Per-type setting from Settings
      const perTypeRetries = settings.retry?.perType?.[mailTypeConfig.id];
      if (perTypeRetries !== undefined) {
        return perTypeRetries;
      }

      // Priority 2: Global setting from Settings
      const globalRetries =
        settings.retryMaxAttempts || settings.retry?.maxAttempts;
      if (globalRetries !== undefined) {
        return globalRetries;
      }

      // Priority 3: Rulebook default
      return mailTypeConfig.maxRetries || 3;
    } catch (error) {
      console.error("[RulebookService] Error getting max retries:", error);
      return 3; // Fallback
    }
  }

  /**
   * Get retry delay hours from Settings
   * @returns {Promise<number>} Delay in hours
   */
  async getRetryDelayHours() {
    try {
      const { SettingsRepository } = require("../repositories");
      const settings = await SettingsRepository.getSettings();
      return (
        settings.retrySoftBounceDelayHrs ||
        settings.retry?.softBounceDelayHours ||
        2
      );
    } catch (error) {
      return 2; // Default 2 hours
    }
  }

  /**
   * Check if a job has exceeded retry limit
   * @param {Object} job - Email job with retryCount and type
   * @returns {Promise<boolean>} True if max retries exceeded
   */
  async hasExceededRetryLimit(job) {
    const maxRetries = await this.getMaxRetries(job.type);
    return (job.retryCount || 0) >= maxRetries;
  }

  // ========================================
  // DEAD MAIL METHODS
  // ========================================

  /**
   * Get dead mail rules configuration
   */
  getDeadMailRules() {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    return rulebook.deadMailRules;
  }

  /**
   * Check if a job failure should trigger dead mail status
   * @param {Object} job - The email job
   * @param {string} eventType - The event that occurred
   * @returns {Promise<boolean>} True if should mark as dead
   */
  async shouldMarkAsDead(job, eventType) {
    // Terminal failure events that should trigger dead mail check
    const terminalFailureEvents = [
      "hard_bounce",
      "blocked",
      "invalid",
      "error",
      "complaint",
      "unsubscribed",
    ];

    // Only check for terminal failure events
    if (!terminalFailureEvents.includes(eventType.toLowerCase())) {
      return false;
    }

    // Check if max retries exceeded
    const exceeded = await this.hasExceededRetryLimit(job);
    console.log(
      `[RulebookService] Dead mail check for job ${job.id}: eventType=${eventType}, retryCount=${job.retryCount || 0}, exceeded=${exceeded}`,
    );
    return exceeded;
  }

  /**
   * Get mail type priority for scheduling
   * @param {string} type - Email type
   * @returns {number} Priority (higher = more important)
   */
  getMailTypePriority(type) {
    const mailType = this.getMailType(type);
    return mailType.priority || 50;
  }

  /**
   * Get followup rules
   */
  getFollowupRules() {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    return rulebook.followupRules;
  }

  /**
   * Get queue watcher rules
   */
  getQueueWatcherRules() {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    return rulebook.queueWatcherRules;
  }

  /**
   * Get mail type permissions for frontend
   * Returns a simplified object for frontend use
   */
  getMailTypePermissions() {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    const permissions = {};

    for (const [key, mailType] of Object.entries(rulebook.mailTypes)) {
      permissions[key] = {
        id: mailType.id,
        displayName: mailType.displayName,
        category: mailType.category,
        priority: mailType.priority,
        canSkip: mailType.canSkip || false,
        canCancel: mailType.canCancel || false,
        canPause: mailType.canPause || false,
        canResume: mailType.canPause || false, // Resume allowed if pause allowed
        canRetry: mailType.canRetry || false,
        canReschedule: mailType.canReschedule || false,
      };
    }

    return permissions;
  }

  /**
   * Get status definitions for frontend
   */
  getStatusDefinitions() {
    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    return rulebook.statuses;
  }

  // ========================================
  // STATUS GROUP GETTERS
  // Use these methods instead of hardcoding status arrays!
  // ========================================

  /**
   * Get active/pending statuses (jobs waiting to be sent)
   * Use in queries: status: { in: RulebookService.getActiveStatuses() }
   */
  getActiveStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.active];
  }

  /**
   * Get failure statuses
   */
  getFailureStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.failure];
  }

  /**
   * Get hard failure statuses (cannot retry)
   */
  getHardFailureStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.hardFailure];
  }

  /**
   * Get soft failure statuses (can retry)
   */
  getSoftFailureStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.softFailure];
  }

  /**
   * Get cancellable statuses
   */
  getCancellableStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.cancellable];
  }

  /**
   * Get reschedulable statuses
   */
  getReschedulableStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.reschedulable];
  }

  /**
   * Get terminal statuses (job complete)
   */
  getTerminalStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.terminal];
  }

  /**
   * Get in-progress statuses
   */
  getInProgressStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.inProgress];
  }

  /**
   * Get existing non-cancelled statuses (for duplicate checking)
   */
  getExistingNonCancelledStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.existingNonCancelled];
  }

  /**
   * Get engagement statuses
   */
  getEngagementStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.engagement];
  }

  /**
   * Get show in queue statuses
   */
  getShowInQueueStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.showInQueue];
  }

  /**
   * Get processed statuses (for metrics)
   */
  getProcessedStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.processed];
  }

  /**
   * Get retriable statuses (terminal failures that can be manually retried)
   */
  getRetriableStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.retriable];
  }

  /**
   * Get pending only statuses (for email queue filtering)
   */
  getPendingOnlyStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.pendingOnly];
  }

  /**
   * Get completed/history statuses (for showing in completed email list)
   */
  getCompletedHistoryStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.completedHistory];
  }

  /**
   * Get awaiting delivery statuses (for handlers that need to cancel pending jobs)
   */
  getAwaitingDeliveryStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.awaitingDelivery];
  }

  /**
   * Get successfully sent statuses (sent, delivered, opened, clicked)
   */
  getSuccessfullySentStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.successfullySent];
  }

  /**
   * Get sent not terminal statuses (alias for successfullySent)
   */
  getSentNotTerminalStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.sentNotTerminal];
  }

  /**
   * Get all statuses except cancelled and skipped (for analytics)
   */
  getAllExceptCancelledSkippedStatuses() {
    return [...DEFAULT_RULEBOOK.statusGroups.allExceptCancelledSkipped];
  }

  /**
   * Check if a status is in a specific group
   */
  isStatusInGroup(status, groupName) {
    const group = DEFAULT_RULEBOOK.statusGroups[groupName];
    return group ? group.includes(status) : false;
  }

  /**
   * Check if status is a failure status
   */
  isFailureStatus(status) {
    return this.isStatusInGroup(status, "failure");
  }

  /**
   * Check if status is active/pending
   */
  isActiveStatus(status) {
    return this.isStatusInGroup(status, "active");
  }

  /**
   * Check if status is terminal
   */
  isTerminal(status) {
    return this.isStatusInGroup(status, "terminal");
  }

  // ========================================
  // LEAD STATUS RESOLVER
  // Single Source of Truth for determining lead status
  // Called after ANY job state change
  // ========================================

  /**
   * Find the next scheduled job for a lead
   * Used to determine lead status after job cancellation/completion
   * @param {number} leadId
   * @returns {Promise<Object|null>} The next scheduled job or null
   */
  async findNextScheduledJob(leadId) {
    const activeStatuses = this.getActiveStatuses();

    const nextJob = await prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        status: { in: activeStatuses },
      },
      orderBy: [{ scheduledFor: "asc" }],
      select: {
        id: true,
        type: true,
        status: true,
        scheduledFor: true,
        metadata: true,
      },
    });

    return nextJob;
  }

  /**
   * LEAD STATUS RESOLVER
   * Single Source of Truth for determining what a lead's status should be
   * Called after ANY job state change to ensure lead status reflects reality
   *
   * Priority Order:
   * 1. Forced status (frozen, converted) - highest priority
   * 2. Active scheduled job (pending, queued, scheduled, rescheduled)
   * 3. Last successfully sent job
   * 4. Failure state (if last action was failure)
   * 5. idle (default fallback)
   *
   * @param {number} leadId - Lead to resolve status for
   * @param {Object} options - { forcedStatus?, skipJobLookup? }
   * @returns {Promise<{status: string, reason: string, job?: Object}>}
   */
  async resolveLeadStatus(leadId, options = {}) {
    const { forcedStatus, skipJobLookup = false } = options;

    // Priority 1: Forced status (frozen, converted)
    if (forcedStatus) {
      return {
        status: forcedStatus,
        reason: `Forced status: ${forcedStatus}`,
      };
    }

    // Check lead special states
    const lead = await prisma.lead.findUnique({
      where: { id: parseInt(leadId) },
      select: {
        status: true,
        frozenUntil: true,
        followupsPaused: true,
      },
    });

    if (!lead) {
      return { status: "idle", reason: "Lead not found" };
    }

    // Check frozen state
    if (lead.frozenUntil && new Date(lead.frozenUntil) > new Date()) {
      return { status: "frozen", reason: "Lead is frozen" };
    }

    // Check converted state
    if (lead.status === "converted") {
      return { status: "converted", reason: "Lead is converted" };
    }

    // Priority 2: Find next scheduled job
    if (!skipJobLookup) {
      const nextJob = await this.findNextScheduledJob(leadId);

      if (nextJob) {
        const isRescheduled =
          nextJob.status === "rescheduled" ||
          nextJob.metadata?.rescheduled ||
          nextJob.metadata?.retryReason;
        const statusWord = isRescheduled ? "rescheduled" : "scheduled";

        // Check if conditional email - use special format
        if (
          this.isConditional(nextJob.type) &&
          nextJob.metadata?.triggerEvent
        ) {
          const formattedStatus = this.formatConditionalStatus(
            nextJob.metadata.triggerEvent,
            statusWord,
          );
          return {
            status: formattedStatus,
            reason: `Has scheduled conditional: ${nextJob.type}`,
            job: nextJob,
          };
        }

        const typeName = this.getSimplifiedTypeName(
          nextJob.type,
          nextJob.metadata,
        );

        return {
          status: `${typeName}:${statusWord}`,
          reason: `Has scheduled job: ${nextJob.type}`,
          job: nextJob,
        };
      }
    }

    // Priority 3: Check for last sent job
    const lastSentJob = await prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        status: { in: this.getSuccessfullySentStatuses() },
      },
      orderBy: { sentAt: "desc" },
      select: { id: true, type: true, status: true },
    });

    if (lastSentJob) {
      // Check if sequence is complete
      const settings = await prisma.settings.findFirst();
      const sequence = (settings?.followups || []).filter(
        (f) => f.enabled && !f.globallySkipped,
      );

      if (sequence.length > 0) {
        // Check if all sequence steps are completed
        const completedTypes = await prisma.emailJob.findMany({
          where: {
            leadId: parseInt(leadId),
            status: { in: this.getSuccessfullySentStatuses() },
          },
          select: { type: true },
          distinct: ["type"],
        });

        const completedTypeNames = completedTypes.map((j) => j.type);
        const allComplete = sequence.every((step) =>
          completedTypeNames.some((t) =>
            t.toLowerCase().includes(step.name.toLowerCase()),
          ),
        );

        if (allComplete) {
          return {
            status: "sequence_complete",
            reason: "All sequence steps completed",
          };
        }
      }

      // Show last sent status
      const typeName = this.getSimplifiedTypeName(lastSentJob.type);
      return {
        status: `${typeName}:sent`,
        reason: `Last sent: ${lastSentJob.type}`,
        job: lastSentJob,
      };
    }

    // Priority 4: Default to idle
    return { status: "idle", reason: "No scheduled or sent jobs found" };
  }

  // ========================================
  // ACTION IMPACT GETTERS
  // ========================================

  /**
   * Get action impact configuration
   */
  getActionImpact(actionName) {
    return DEFAULT_RULEBOOK.actionImpacts[actionName] || null;
  }

  // ========================================
  // ACTION EXECUTORS
  // Execute actions with ALL side effects
  // ========================================

  /**
   * Execute job cancellation with all side effects
   * @param {number} jobId - Job to cancel
   * @param {string} reason - Why being cancelled
   * @param {boolean} isManual - true if user action, false if system/priority
   * @param {Object} context - { higherPriorityJobId?, triggeredBy? }
   * @returns {Promise<{success: boolean, newLeadStatus?: string, nextJob?: Object, error?: string}>}
   */
  async executeCancelJob(
    jobId,
    reason = "User cancelled",
    isManual = true,
    context = {},
  ) {
    const impactKey = isManual ? "cancelJobManual" : "cancelJobDynamic";
    const impact = this.getActionImpact(impactKey);

    try {
      console.log(
        `[RulebookAction] ${impactKey} started for job ${jobId}: ${reason}`,
      );

      // 1. Get the job
      const job = await prisma.emailJob.findUnique({
        where: { id: parseInt(jobId) },
        include: { lead: true },
      });

      if (!job) {
        return { success: false, error: "Job not found" };
      }

      // Check if cancellable
      if (!this.getCancellableStatuses().includes(job.status)) {
        return {
          success: false,
          error: `Cannot cancel job with status: ${job.status}`,
        };
      }

      // 2. Update job status
      await prisma.emailJob.update({
        where: { id: parseInt(jobId) },
        data: {
          status: impact.jobStatusChange,
          lastError: reason,
          metadata: {
            ...(job.metadata || {}),
            cancelledAt: new Date().toISOString(),
            cancelReason: reason,
            cancelledBy: isManual ? "user" : "system",
            ...(context.higherPriorityJobId && {
              supersededBy: context.higherPriorityJobId,
            }),
          },
        },
      });

      // 3. Remove from BullMQ queue if exists
      if (impact.removeFromQueue && job.metadata?.queueJobId) {
        try {
          const { emailSendQueue } = require("../queues/emailQueues");
          const queueJob = await emailSendQueue.getJob(job.metadata.queueJobId);
          if (queueJob) await queueJob.remove();
        } catch (err) {
          console.warn(
            `[RulebookAction] Could not remove from queue:`,
            err.message,
          );
        }
      }

      // 4. Resolve lead status (for manual cancellation)
      let newLeadStatus = null;
      let nextJob = null;

      if (impact.updateLeadStatus && impact.lookForNextScheduledJob) {
        const resolved = await this.resolveLeadStatus(job.leadId);
        newLeadStatus = resolved.status;
        nextJob = resolved.job;

        // Update lead status
        await prisma.lead.update({
          where: { id: job.leadId },
          data: { status: newLeadStatus },
        });

        console.log(
          `[RulebookAction] Lead ${job.leadId} status updated to: ${newLeadStatus}`,
        );
      }

      // 5. Create event history
      if (impact.createEventHistory) {
        await prisma.eventHistory.create({
          data: {
            leadId: job.leadId,
            event: impact.eventType,
            timestamp: new Date(),
            details: {
              jobId: job.id,
              jobType: job.type,
              reason: reason,
              newLeadStatus: newLeadStatus,
              ...(context.higherPriorityJobId && {
                higherPriorityJobId: context.higherPriorityJobId,
              }),
            },
            emailType: job.type,
          },
        });
      }

      console.log(`[RulebookAction] ${impactKey} completed for job ${jobId}`);

      return {
        success: true,
        newLeadStatus,
        nextJob,
        jobId: job.id,
        leadId: job.leadId,
      };
    } catch (error) {
      console.error(`[RulebookAction] ${impactKey} failed:`, error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Execute skip job with all side effects
   * @param {number} jobId - Job to skip
   * @param {string} reason - Why being skipped
   * @returns {Promise<{success: boolean, newLeadStatus?: string, error?: string}>}
   */
  async executeSkipJob(jobId, reason = "User skipped") {
    const impact = this.getActionImpact("skipJob");

    try {
      console.log(`[RulebookAction] skipJob started for job ${jobId}`);

      // 1. Get the job
      const job = await prisma.emailJob.findUnique({
        where: { id: parseInt(jobId) },
        include: { lead: true },
      });

      if (!job) {
        return { success: false, error: "Job not found" };
      }

      // 2. Update job status
      await prisma.emailJob.update({
        where: { id: parseInt(jobId) },
        data: {
          status: impact.jobStatusChange,
          lastError: reason,
          metadata: {
            ...(job.metadata || {}),
            skippedAt: new Date().toISOString(),
            skipReason: reason,
          },
        },
      });

      // 3. Remove from queue
      if (impact.removeFromQueue && job.metadata?.queueJobId) {
        try {
          const { emailSendQueue } = require("../queues/emailQueues");
          const queueJob = await emailSendQueue.getJob(job.metadata.queueJobId);
          if (queueJob) await queueJob.remove();
        } catch (err) {
          console.warn(
            `[RulebookAction] Could not remove from queue:`,
            err.message,
          );
        }
      }

      // 4. Add to lead's skipped followups
      const currentSkipped = job.lead.skippedFollowups || [];
      if (!currentSkipped.includes(job.type)) {
        await prisma.lead.update({
          where: { id: job.leadId },
          data: {
            skippedFollowups: [...currentSkipped, job.type],
          },
        });
      }

      // 5. Schedule next followup (will be handled by scheduler)
      // The scheduler will pick up from where we left off

      // 6. Resolve lead status
      const resolved = await this.resolveLeadStatus(job.leadId);

      await prisma.lead.update({
        where: { id: job.leadId },
        data: { status: resolved.status },
      });

      // 7. Create event history
      if (impact.createEventHistory) {
        await prisma.eventHistory.create({
          data: {
            leadId: job.leadId,
            event: impact.eventType,
            timestamp: new Date(),
            details: {
              jobId: job.id,
              jobType: job.type,
              reason: reason,
              newLeadStatus: resolved.status,
            },
            emailType: job.type,
          },
        });
      }

      console.log(`[RulebookAction] skipJob completed for job ${jobId}`);

      return {
        success: true,
        newLeadStatus: resolved.status,
        jobId: job.id,
        leadId: job.leadId,
      };
    } catch (error) {
      console.error(`[RulebookAction] skipJob failed:`, error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Execute pause followups with all side effects
   * @param {number} leadId - Lead to pause followups for
   * @returns {Promise<{success: boolean, cancelledCount: number, newLeadStatus?: string, error?: string}>}
   */
  async executePauseFollowups(leadId) {
    const impact = this.getActionImpact("pauseFollowups");

    try {
      console.log(`[RulebookAction] pauseFollowups started for lead ${leadId}`);

      // 1. Find all pending followup jobs (exclude manual, conditional, initial)
      const pendingFollowups = await prisma.emailJob.findMany({
        where: {
          leadId: parseInt(leadId),
          status: { in: this.getActiveStatuses() },
          NOT: {
            OR: [
              { type: { contains: "initial", mode: "insensitive" } },
              { type: { contains: "manual", mode: "insensitive" } },
              { type: { startsWith: "conditional:" } },
            ],
          },
        },
      });

      // 2. Cancel each followup job
      for (const job of pendingFollowups) {
        await prisma.emailJob.update({
          where: { id: job.id },
          data: {
            status: impact.jobStatusChange,
            lastError: "Followups paused by user",
            metadata: {
              ...(job.metadata || {}),
              pausedAt: new Date().toISOString(),
            },
          },
        });

        // Remove from queue
        if (job.metadata?.queueJobId) {
          try {
            const { emailSendQueue } = require("../queues/emailQueues");
            const queueJob = await emailSendQueue.getJob(
              job.metadata.queueJobId,
            );
            if (queueJob) await queueJob.remove();
          } catch (err) {
            // Ignore queue removal errors
          }
        }
      }

      // 3. Set lead flag
      await prisma.lead.update({
        where: { id: parseInt(leadId) },
        data: { followupsPaused: true },
      });

      // 4. Resolve lead status (check for manual/conditional jobs)
      const resolved = await this.resolveLeadStatus(leadId);

      await prisma.lead.update({
        where: { id: parseInt(leadId) },
        data: { status: resolved.status },
      });

      // 5. Create event history
      if (impact.createEventHistory) {
        await prisma.eventHistory.create({
          data: {
            leadId: parseInt(leadId),
            event: impact.eventType,
            timestamp: new Date(),
            details: {
              cancelledJobs: pendingFollowups.map((j) => ({
                id: j.id,
                type: j.type,
              })),
              cancelledCount: pendingFollowups.length,
              newLeadStatus: resolved.status,
            },
          },
        });
      }

      console.log(
        `[RulebookAction] pauseFollowups completed: ${pendingFollowups.length} jobs cancelled`,
      );

      return {
        success: true,
        cancelledCount: pendingFollowups.length,
        newLeadStatus: resolved.status,
        leadId: parseInt(leadId),
      };
    } catch (error) {
      console.error(`[RulebookAction] pauseFollowups failed:`, error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Execute resume followups with all side effects
   * @param {number} leadId - Lead to resume followups for
   * @returns {Promise<{success: boolean, scheduledJob?: Object, error?: string}>}
   */
  async executeResumeFollowups(leadId) {
    const impact = this.getActionImpact("resumeFollowups");

    try {
      console.log(
        `[RulebookAction] resumeFollowups started for lead ${leadId}`,
      );

      // 1. Clear lead flag
      await prisma.lead.update({
        where: { id: parseInt(leadId) },
        data: { followupsPaused: false },
      });

      // 2. Check for blocking jobs (manual, conditional)
      if (impact.checkForBlockingJobs) {
        const blockingJob = await prisma.emailJob.findFirst({
          where: {
            leadId: parseInt(leadId),
            status: { in: this.getActiveStatuses() },
            OR: [
              { type: { contains: "manual", mode: "insensitive" } },
              { type: { startsWith: "conditional:" } },
            ],
          },
        });

        if (blockingJob) {
          console.log(
            `[RulebookAction] Blocking job found: ${blockingJob.type}. Not scheduling followup.`,
          );

          // Update lead status to show blocking job
          const typeName = this.getSimplifiedTypeName(blockingJob.type);
          await prisma.lead.update({
            where: { id: parseInt(leadId) },
            data: { status: `${typeName}:scheduled` },
          });

          return {
            success: true,
            blockingJob: blockingJob,
            newLeadStatus: `${typeName}:scheduled`,
            leadId: parseInt(leadId),
          };
        }
      }

      // 3. Schedule next followup (handled by EmailSchedulerService)
      // We call it here to ensure proper scheduling
      const EmailSchedulerService = require("./EmailSchedulerService");
      const scheduledJob = await EmailSchedulerService.scheduleNextEmail(
        leadId,
        "pending",
      );

      // 4. Resolve lead status
      const resolved = await this.resolveLeadStatus(leadId);

      await prisma.lead.update({
        where: { id: parseInt(leadId) },
        data: { status: resolved.status },
      });

      // 5. Create event history
      if (impact.createEventHistory) {
        await prisma.eventHistory.create({
          data: {
            leadId: parseInt(leadId),
            event: impact.eventType,
            timestamp: new Date(),
            details: {
              scheduledJob: scheduledJob
                ? { id: scheduledJob.id, type: scheduledJob.type }
                : null,
              newLeadStatus: resolved.status,
            },
          },
        });
      }

      console.log(`[RulebookAction] resumeFollowups completed`);

      return {
        success: true,
        scheduledJob,
        newLeadStatus: resolved.status,
        leadId: parseInt(leadId),
      };
    } catch (error) {
      console.error(`[RulebookAction] resumeFollowups failed:`, error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Execute after any job status change to ensure lead status is correct
   * This is a wrapper that should be called after any direct job status updates
   * @param {number} leadId
   * @param {string} contextAction - What caused this update
   * @returns {Promise<{status: string}>}
   */
  async syncLeadStatusAfterJobChange(leadId, contextAction = "job_update") {
    try {
      const resolved = await this.resolveLeadStatus(leadId);

      await prisma.lead.update({
        where: { id: parseInt(leadId) },
        data: { status: resolved.status },
      });

      console.log(
        `[RulebookAction] Lead ${leadId} status synced to: ${resolved.status} (${contextAction})`,
      );

      return resolved;
    } catch (error) {
      console.error(`[RulebookAction] syncLeadStatus failed:`, error);
      return { status: "idle", reason: "Error syncing status" };
    }
  }

  // ========================================
  // PRIORITY & STATUS GROUP HELPERS
  // ========================================

  /**
   * Get the priority of a mail type (higher = more important)
   * Used by QueueWatcher to determine which jobs to pause
   * @param {string} type - The mail type
   * @returns {number} Priority value (100 = highest)
   */
  getMailTypePriority(type) {
    const rulebook = DEFAULT_RULEBOOK;

    if (!type) return 0;

    const normalizedType = type.toLowerCase();

    // Check for conditional prefix
    if (
      normalizedType.startsWith("conditional:") ||
      normalizedType === "conditional"
    ) {
      return rulebook.mailTypes.conditional?.priority || 95;
    }

    // Check each mail type for matches
    for (const [key, mailType] of Object.entries(rulebook.mailTypes)) {
      if (
        mailType.internalTypes?.some((t) =>
          normalizedType.includes(t.toLowerCase()),
        )
      ) {
        return mailType.priority || 0;
      }
    }

    // Default to followup priority if unknown
    return rulebook.mailTypes.followup?.priority || 70;
  }

  /**
   * Check if a status is resumable (shows Resume button instead of Retry)
   * @param {string} status - The job status
   * @returns {boolean}
   */
  isResumable(status) {
    const rulebook = DEFAULT_RULEBOOK;
    return rulebook.statusGroups?.resumable?.includes(status) || false;
  }

  /**
   * Check if a status is retriable (shows Retry button, increments count)
   * @param {string} status - The job status
   * @returns {boolean}
   */
  isRetriable(status) {
    const rulebook = DEFAULT_RULEBOOK;
    return rulebook.statusGroups?.retriable?.includes(status) || false;
  }

  /**
   * Check if a status requires manual retry confirmation
   * @param {string} status - The job status
   * @returns {boolean}
   */
  isManualRetriable(status) {
    const rulebook = DEFAULT_RULEBOOK;
    return rulebook.statusGroups?.manualRetriable?.includes(status) || false;
  }

  /**
   * Get the status group for a given status
   * @param {string} status - The job status
   * @returns {'resumable'|'retriable'|'manualRetriable'|'nonRetriable'|'unknown'}
   */
  getStatusGroup(status) {
    const rulebook = DEFAULT_RULEBOOK;
    const groups = rulebook.statusGroups || {};

    if (groups.resumable?.includes(status)) return "resumable";
    if (groups.retriable?.includes(status)) return "retriable";
    if (groups.manualRetriable?.includes(status)) return "manualRetriable";
    if (groups.nonRetriable?.includes(status)) return "nonRetriable";

    return "unknown";
  }

  /**
   * Check if a mail type triggers auto-resume when completed/cancelled
   * @param {string} type - The mail type
   * @returns {boolean}
   */
  triggersAutoResume(type) {
    const rulebook = DEFAULT_RULEBOOK;
    const normalizedType = type?.toLowerCase() || "";

    // Check for conditional prefix
    if (normalizedType.startsWith("conditional:")) {
      return (
        rulebook.statusGroups?.triggersAutoResume?.includes("conditional") ||
        false
      );
    }

    return (
      rulebook.statusGroups?.triggersAutoResume?.includes(normalizedType) ||
      false
    );
  }

  /**
   * Check if a status should trigger auto-resume of paused jobs
   * @param {string} status - The job status
   * @returns {boolean}
   */
  shouldTriggerAutoResume(status) {
    const rulebook = DEFAULT_RULEBOOK;
    return rulebook.statusGroups?.autoResumeOnStatus?.includes(status) || false;
  }

  // ========================================
  // PRIORITY-BASED PAUSE/RESUME METHODS
  // Core methods for handling mail priority hierarchy
  // ========================================

  /**
   * Get the priority value for a mail type
   * Higher number = higher priority
   * @param {string} type - The mail type (e.g., 'conditional:Clicked', 'manual', 'First Followup')
   * @returns {number} Priority value
   */
  getMailTypePriority(type) {
    if (!type) return 70; // Default to followup priority

    const rulebook = this._cachedRulebook || DEFAULT_RULEBOOK;
    const priorities = rulebook.queueWatcherRules?.priorityScheduling
      ?.priorities || {
      conditional: 100,
      manual: 90,
      initial: 80,
      followup: 70,
    };

    const t = type.toLowerCase();

    // Check type prefixes
    if (t.startsWith("conditional:") || t.startsWith("conditional"))
      return priorities.conditional;
    if (t === "manual" || t.includes("manual")) return priorities.manual;
    if (t === "initial" || t.includes("initial")) return priorities.initial;

    // Default to followup for anything else
    return priorities.followup;
  }

  /**
   * Get the mail types that should be paused when scheduling a higher priority mail type
   * @param {string} schedulingType - The mail type being scheduled
   * @returns {string[]} Array of mail type categories to pause (e.g., ['followup', 'manual'])
   */
  getTypesToPauseFor(schedulingType) {
    if (!schedulingType) return [];

    const t = schedulingType.toLowerCase();

    // Conditional pauses both followups and manual mails
    if (t.startsWith("conditional:") || t.startsWith("conditional")) {
      return ["followup", "manual"];
    }

    // Manual pauses only followups
    if (t === "manual" || t.includes("manual")) {
      return ["followup"];
    }

    // Initial doesn't pause anything
    // Followups don't pause anything
    return [];
  }

  /**
   * CRITICAL: Pause all lower-priority pending jobs for a lead when scheduling a high-priority mail
   * Updates job status to 'paused' (NOT cancelled) and removes from BullMQ queue
   *
   * @param {number} leadId - The lead ID
   * @param {string} schedulingType - The high-priority mail type being scheduled (e.g., 'conditional:Opened', 'manual')
   * @returns {Promise<{pausedCount: number, pausedJobs: Array}>} Result with count and paused job details
   */
  async pauseLowerPriorityJobs(leadId, schedulingType) {
    const typesToPause = this.getTypesToPauseFor(schedulingType);

    if (typesToPause.length === 0) {
      console.log(
        `[RulebookService] No mail types to pause for ${schedulingType}`,
      );
      return { pausedCount: 0, pausedJobs: [] };
    }

    console.log(
      `[RulebookService] Pausing ${typesToPause.join(", ")} jobs for lead ${leadId} due to ${schedulingType}`,
    );

    // Build query conditions for types to pause
    const typeConditions = typesToPause
      .map((t) => {
        if (t === "followup") {
          return { type: { contains: "Followup" } };
        } else if (t === "manual") {
          return { type: { equals: "manual" } };
        }
        return null;
      })
      .filter(Boolean);

    // Also add lowercase checks for different naming conventions
    const allTypeConditions = [
      ...typeConditions,
      ...typesToPause
        .map((t) => {
          if (t === "followup") {
            return { type: { contains: "followup" } };
          }
          return null;
        })
        .filter(Boolean),
    ];

    // Find pending jobs that should be paused
    const pendingStatuses = [
      "pending",
      "queued",
      "scheduled",
      "rescheduled",
      "deferred",
    ];

    const jobsToPause = await prisma.emailJob.findMany({
      where: {
        leadId: parseInt(leadId),
        status: { in: pendingStatuses },
        OR: allTypeConditions.length > 0 ? allTypeConditions : undefined,
      },
    });

    if (jobsToPause.length === 0) {
      console.log(
        `[RulebookService] No pending jobs to pause for lead ${leadId}`,
      );
      return { pausedCount: 0, pausedJobs: [] };
    }

    console.log(`[RulebookService] Found ${jobsToPause.length} jobs to pause`);

    // Remove from BullMQ queues
    const { emailSendQueue, followupQueue } = require("../queues/emailQueues");

    for (const job of jobsToPause) {
      if (job.metadata?.queueJobId) {
        try {
          // Try both queues
          let bullJob = await emailSendQueue.getJob(job.metadata.queueJobId);
          if (bullJob) {
            await bullJob.remove();
            console.log(
              `[RulebookService] Removed job ${job.id} from emailSendQueue`,
            );
          } else {
            bullJob = await followupQueue.getJob(job.metadata.queueJobId);
            if (bullJob) {
              await bullJob.remove();
              console.log(
                `[RulebookService] Removed job ${job.id} from followupQueue`,
              );
            }
          }
        } catch (err) {
          console.warn(
            `[RulebookService] Failed to remove job ${job.id} from queue:`,
            err.message,
          );
        }
      }
    }

    // Update job statuses to 'paused' (NOT cancelled!)
    const pauseResult = await prisma.emailJob.updateMany({
      where: {
        id: { in: jobsToPause.map((j) => j.id) },
      },
      data: {
        status: "paused",
        lastError: `Auto-paused for higher priority ${schedulingType} mail`,
        metadata: {
          pausedAt: new Date().toISOString(),
          pausedByMailType: schedulingType,
          previousStatus: "pending", // Will be used to restore on resume
        },
      },
    });

    console.log(
      `[RulebookService] ✓ Paused ${pauseResult.count} jobs for lead ${leadId}`,
    );

    // Emit batch event for all paused jobs (more efficient than individual events)
    const EventBus = require("../events/EventBus");
    if (jobsToPause.length > 0) {
      await EventBus.emit("jobs.paused.batch", {
        leadId: leadId,
        jobIds: jobsToPause.map(j => j.id),
        reason: `Paused for ${schedulingType}`,
        pausedByMailType: schedulingType,
        count: jobsToPause.length
      });
    }

    return {
      pausedCount: pauseResult.count,
      pausedJobs: jobsToPause.map((j) => ({ id: j.id, type: j.type })),
    };
  }

  /**
   * CRITICAL: Resume paused jobs after a high-priority mail completes or is cancelled
   * Called when conditional/manual mail is delivered/cancelled
   *
   * @param {number} leadId - The lead ID
   * @param {string} completedType - The high-priority mail type that just completed
   * @param {string} completedStatus - The status that triggered resume (e.g., 'delivered', 'cancelled')
   * @returns {Promise<{resumedCount: number}>} Result with count of resumed jobs
   */
  async resumePausedJobsAfter(
    leadId,
    completedType,
    completedStatus = "delivered",
  ) {
    console.log(
      `[RulebookService] Checking for paused jobs to resume after ${completedType} -> ${completedStatus}`,
    );

    // ==========================================
    // CRITICAL SAFETY CHECK: Terminal State Guard
    // No jobs should EVER resume if lead is in terminal state
    // This is the PRIMARY guard against invalid resumes
    // ==========================================
    const lead = await prisma.lead.findUnique({
      where: { id: parseInt(leadId) },
      select: { 
        terminalState: true, 
        status: true,
        isInFailure: true  // If this field exists
      }
    });
    
    if (!lead) {
      console.log(`[RulebookService] Lead ${leadId} not found, cannot resume`);
      return { resumedCount: 0, blocked: true, reason: 'lead_not_found' };
    }
    
    // Block resume if in terminal state (dead, unsubscribed, complaint)
    if (lead.terminalState) {
      console.log(`[RulebookService] ⛔ Lead ${leadId} is in terminal state (${lead.terminalState}), BLOCKING resume`);
      return { resumedCount: 0, blocked: true, reason: 'terminal_state', terminalState: lead.terminalState };
    }
    
    // Block resume if lead is marked as in failure (requires manual intervention)
    if (lead.isInFailure) {
      console.log(`[RulebookService] ⛔ Lead ${leadId} is in failure state, BLOCKING resume - manual retry required`);
      return { resumedCount: 0, blocked: true, reason: 'in_failure' };
    }
    
    // Block resume if status indicates failure (backup check)
    const failureStatuses = this.getFailureStatuses();
    const statusPart = lead.status?.includes(':') ? lead.status.split(':')[1] : lead.status;
    if (failureStatuses.includes(statusPart)) {
      console.log(`[RulebookService] ⛔ Lead ${leadId} has failure status (${lead.status}), BLOCKING resume`);
      return { resumedCount: 0, blocked: true, reason: 'failure_status' };
    }
    // ==========================================

    // First, check if this mail type triggers auto-resume
    if (!this.triggersAutoResume(completedType)) {
      console.log(
        `[RulebookService] ${completedType} does not trigger auto-resume`,
      );
      return { resumedCount: 0 };
    }

    // Check if the status triggers auto-resume
    if (!this.shouldTriggerAutoResume(completedStatus)) {
      console.log(
        `[RulebookService] Status ${completedStatus} does not trigger auto-resume`,
      );
      return { resumedCount: 0 };
    }

    // Find paused jobs for this lead
    const pausedJobs = await prisma.emailJob.findMany({
      where: {
        leadId: parseInt(leadId),
        status: "paused",
      },
      orderBy: { scheduledFor: "asc" }, // Resume in order
    });

    if (pausedJobs.length === 0) {
      console.log(
        `[RulebookService] No paused jobs to resume for lead ${leadId}`,
      );
      return { resumedCount: 0 };
    }

    console.log(
      `[RulebookService] Found ${pausedJobs.length} paused job(s) to resume`,
    );

    // Before resuming, check if there's STILL a higher-priority active job
    // (e.g., conditional completed but manual is still pending)
    const higherPriorityActive = await prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        status: { in: ["pending", "queued", "scheduled", "rescheduled"] },
        NOT: { id: { in: pausedJobs.map((j) => j.id) } },
      },
    });

    if (higherPriorityActive) {
      const activePriority = this.getMailTypePriority(
        higherPriorityActive.type,
      );
      const firstPausedPriority = this.getMailTypePriority(pausedJobs[0].type);

      if (activePriority > firstPausedPriority) {
        console.log(
          `[RulebookService] Higher priority job still active (${higherPriorityActive.type}), keeping paused jobs paused`,
        );
        return { resumedCount: 0 };
      }
    }

    // Resume jobs - reschedule with fresh times
    const { SettingsRepository } = require("../repositories");
    const EmailSchedulerService = require("./EmailSchedulerService");
    const settings = await SettingsRepository.getSettings();

    let resumedCount = 0;

    for (const job of pausedJobs) {
      try {
        // Get lead for timezone
        const lead = await prisma.lead.findUnique({
          where: { id: job.leadId },
        });
        if (!lead) continue;

        // Find next available slot from now
        const slotResult = await EmailSchedulerService.findNextAvailableSlot(
          lead.timezone || "UTC",
          new Date(), // Start from now
          settings,
        );

        if (!slotResult.success) {
          console.warn(
            `[RulebookService] Could not find slot for resumed job ${job.id}: ${slotResult.reason}`,
          );
          continue;
        }

        // Update job with new schedule and pending status
        await prisma.emailJob.update({
          where: { id: job.id },
          data: {
            status: "pending",
            scheduledFor: slotResult.scheduledTime,
            lastError: null,
            metadata: {
              ...job.metadata,
              resumedAt: new Date().toISOString(),
              resumedAfter: completedType,
            },
          },
        });

        // Re-add to queue
        const { followupQueue } = require("../queues/emailQueues");
        const delay = Math.max(
          0,
          slotResult.scheduledTime.getTime() - Date.now(),
        );

        const bullJob = await followupQueue.add(
          "sendEmail",
          { leadId: job.leadId, emailJobId: job.id, type: job.type },
          { delay, jobId: `resumed-${job.id}-${Date.now()}` },
        );

        // Update metadata with new queue job ID
        await prisma.emailJob.update({
          where: { id: job.id },
          data: {
            metadata: {
              ...job.metadata,
              queueJobId: bullJob.id,
              resumedAt: new Date().toISOString(),
            },
          },
        });

        console.log(
          `[RulebookService] ✓ Resumed job ${job.id} (${job.type}) scheduled for ${slotResult.scheduledTime}`,
        );
        resumedCount++;
      } catch (err) {
        console.error(
          `[RulebookService] Failed to resume job ${job.id}:`,
          err.message,
        );
      }
    }

    console.log(
      `[RulebookService] ✓ Resumed ${resumedCount} of ${pausedJobs.length} paused jobs`,
    );

    // Update lead status if jobs were resumed
    if (resumedCount > 0) {
      await this.syncLeadStatusAfterJobChange(leadId, "jobs_resumed");
    }

    return { resumedCount };
  }
  
  /**
   * Get the priority value for a job/lead status
   * Higher number = higher priority (won't be overwritten by lower priority)
   * @param {string} status - The status to get priority for
   * @returns {number} Priority value (0-100)
   */
  getStatusPriority(status) {
    const s = (status || '').toLowerCase();
    
    // Terminal states - highest priority (never overwrite)
    if (['converted', 'unsubscribed'].includes(s)) return 100;
    
    // Frozen/Dead - very high priority
    if (['frozen', 'dead'].includes(s)) return 95;
    
    // Paused - high priority (temporary but important)
    if (['paused'].includes(s)) return 90;
    
    // Active scheduled states - high priority (current work)
    if (['scheduled', 'rescheduled', 'queued', 'pending'].includes(s)) return 85;
    
    // Failure states - high priority (need attention)
    if (this.getFailureStatuses().includes(s)) return 80;
    
    // Engagement events - medium priority (good news)
    if (['clicked'].includes(s)) return 40;
    if (['opened', 'unique_opened'].includes(s)) return 35;
    if (['delivered'].includes(s)) return 30;
    if (['sent'].includes(s)) return 25;
    
    // Skipped/cancelled - low priority
    if (['skipped', 'cancelled'].includes(s)) return 20;
    
    // Idle/unknown - lowest priority
    if (['idle'].includes(s)) return 10;
    
    return 0;
  }
}

// Export singleton
module.exports = new RulebookService();
module.exports.DEFAULT_RULEBOOK = DEFAULT_RULEBOOK;
