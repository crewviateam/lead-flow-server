// inngest/functions/workflowFunctions.js
// Durable workflow functions for followups, conditionals, and lead lifecycle

const { inngest, EVENTS } = require('../client');
const { EmailJobRepository, LeadRepository, SettingsRepository } = require('../../repositories');
const EmailSchedulerService = require('../../services/EmailSchedulerService');
const ConditionalEmailService = require('../../services/ConditionalEmailService');
const RulebookService = require('../../services/RulebookService');
const { prisma } = require('../../lib/prisma');

/**
 * Schedule Followup Workflow
 * Schedules the entire email sequence for a lead
 */
const scheduleFollowups = inngest.createFunction(
  {
    id: 'schedule-followups',
    name: 'Schedule Email Followups',
    retries: 3,
    // Prevent duplicate scheduling for same lead
    idempotency: 'event.data.leadId + "-followups"',
    // Concurrency limit per lead
    concurrency: {
      limit: 1,
      key: 'event.data.leadId'
    }
  },
  { event: EVENTS.FOLLOWUP_SCHEDULE },
  async ({ event, step }) => {
    const { leadId, startFrom } = event.data;
    
    // Step 1: Get lead and verify it exists
    const lead = await step.run('get-lead', async () => {
      const l = await LeadRepository.findById(leadId);
      if (!l) throw new Error(`Lead ${leadId} not found`);
      return l;
    });
    
    // Step 2: Get settings and followup sequence
    const settings = await step.run('get-settings', async () => {
      return await SettingsRepository.getSettings();
    });
    
    const sequence = settings.followups
      .filter(f => f.enabled && !f.globallySkipped)
      .sort((a, b) => a.order - b.order);
    
    if (sequence.length === 0) {
      return { status: 'skipped', reason: 'No followups configured' };
    }
    
    // Step 3: Schedule each followup
    const scheduledJobs = [];
    
    for (let i = startFrom || 0; i < sequence.length; i++) {
      const followup = sequence[i];
      
      const job = await step.run(`schedule-${followup.name}`, async () => {
        // Check if already scheduled
        const existing = await prisma.emailJob.findFirst({
          where: {
            leadId: parseInt(leadId),
            type: followup.name,
            status: { in: RulebookService.getActiveStatuses() }
          }
        });
        
        if (existing) {
          return { skipped: true, reason: 'Already scheduled' };
        }
        
        // Calculate schedule time
        const delay = followup.delayDays * 24 * 60 * 60 * 1000;
        const previousJob = scheduledJobs[scheduledJobs.length - 1];
        const baseTime = previousJob 
          ? new Date(previousJob.scheduledFor)
          : new Date();
        
        const scheduledFor = new Date(baseTime.getTime() + delay);
        
        // Create job
        const newJob = await EmailSchedulerService.scheduleEmailJob({
          leadId: parseInt(leadId),
          type: followup.name,
          scheduledFor,
          templateId: followup.templateId
        });
        
        return newJob;
      });
      
      if (!job.skipped) {
        scheduledJobs.push(job);
      }
    }
    
    // Step 4: Update lead status
    await step.run('update-lead-status', async () => {
      const { status } = await RulebookService.resolveLeadStatus(leadId);
      await LeadRepository.update(leadId, { status });
    });
    
    return {
      status: 'completed',
      scheduledCount: scheduledJobs.length,
      leadId
    };
  }
);

/**
 * Trigger Conditional Email Function
 * Evaluates and triggers conditional emails based on engagement events
 */
const triggerConditional = inngest.createFunction(
  {
    id: 'trigger-conditional',
    name: 'Trigger Conditional Email',
    retries: 3,
    // Deduplicate by event combination
    idempotency: 'event.data.leadId + "-" + event.data.eventType + "-" + event.data.sourceJobId'
  },
  { event: EVENTS.CONDITIONAL_TRIGGER },
  async ({ event, step }) => {
    const { leadId, eventType, sourceEmailType, sourceJobId } = event.data;
    
    // Step 1: Evaluate triggers
    const triggeredJobs = await step.run('evaluate-triggers', async () => {
      return await ConditionalEmailService.evaluateTriggers(
        leadId,
        eventType,
        sourceEmailType,
        sourceJobId
      );
    });
    
    if (triggeredJobs.length === 0) {
      return { 
        status: 'no-triggers', 
        reason: 'No conditional emails matched' 
      };
    }
    
    // Step 2: For each triggered job, schedule the send
    for (const job of triggeredJobs) {
      await step.sendEvent(`send-conditional-${job.id}`, {
        name: EVENTS.EMAIL_SEND,
        data: {
          emailJobId: job.id,
          leadId: job.leadId
        },
        // Delay until scheduled time
        ts: new Date(job.scheduledFor).getTime()
      });
    }
    
    return {
      status: 'triggered',
      count: triggeredJobs.length,
      jobs: triggeredJobs.map(j => j.id)
    };
  }
);

/**
 * Lead Created Workflow
 * Orchestrates the onboarding flow for new leads
 */
const onLeadCreated = inngest.createFunction(
  {
    id: 'on-lead-created',
    name: 'New Lead Onboarding',
    retries: 3
  },
  { event: EVENTS.LEAD_CREATED },
  async ({ event, step }) => {
    const { leadId, scheduleEmails } = event.data;
    
    // Step 1: Wait a moment for any race conditions to settle
    await step.sleep('settle', '1s');
    
    // Step 2: Schedule initial email
    const initialJob = await step.run('schedule-initial', async () => {
      const settings = await SettingsRepository.getSettings();
      const initialStep = settings.followups.find(f => 
        f.name.toLowerCase().includes('initial') && f.enabled
      );
      
      if (!initialStep) return null;
      
      return await EmailSchedulerService.scheduleEmailJob({
        leadId: parseInt(leadId),
        type: initialStep.name,
        scheduledFor: new Date(),
        templateId: initialStep.templateId
      });
    });
    
    // Step 3: If scheduleEmails flag is true, schedule followups
    if (scheduleEmails) {
      await step.sendEvent('schedule-followups', {
        name: EVENTS.FOLLOWUP_SCHEDULE,
        data: { leadId, startFrom: 1 }
      });
    }
    
    return {
      status: 'onboarded',
      initialJobId: initialJob?.id,
      leadId
    };
  }
);

/**
 * Bulk Import Workflow
 * Handles bulk lead imports with parallel processing
 */
const bulkImport = inngest.createFunction(
  {
    id: 'bulk-import',
    name: 'Bulk Lead Import',
    retries: 2,
    // Long running function
    cancelOn: [{ event: 'bulk/cancel', match: 'data.batchId' }]
  },
  { event: EVENTS.BULK_IMPORT },
  async ({ event, step }) => {
    const { leads, batchId, scheduleEmails } = event.data;
    
    // Step 1: Validate and deduplicate
    const validLeads = await step.run('validate-leads', async () => {
      const emails = leads.map(l => l.email.toLowerCase());
      const existing = await prisma.lead.findMany({
        where: { email: { in: emails } },
        select: { email: true }
      });
      
      const existingEmails = new Set(existing.map(e => e.email.toLowerCase()));
      return leads.filter(l => !existingEmails.has(l.email.toLowerCase()));
    });
    
    if (validLeads.length === 0) {
      return { status: 'skipped', reason: 'All leads already exist' };
    }
    
    // Step 2: Create leads in batches
    const batchSize = 50;
    const results = [];
    
    for (let i = 0; i < validLeads.length; i += batchSize) {
      const batch = validLeads.slice(i, i + batchSize);
      
      const created = await step.run(`create-batch-${i}`, async () => {
        return await prisma.lead.createMany({
          data: batch.map(l => ({
            name: l.name,
            email: l.email.toLowerCase(),
            country: l.country,
            city: l.city,
            timezone: l.timezone || 'UTC',
            status: 'pending'
          })),
          skipDuplicates: true
        });
      });
      
      results.push(created);
    }
    
    // Step 3: Get created lead IDs
    const createdLeads = await step.run('get-created-ids', async () => {
      return await prisma.lead.findMany({
        where: {
          email: { in: validLeads.map(l => l.email.toLowerCase()) }
        },
        select: { id: true }
      });
    });
    
    // Step 4: Trigger onboarding for each lead
    if (scheduleEmails) {
      await step.sendEvent('trigger-onboarding', createdLeads.map(lead => ({
        name: EVENTS.LEAD_CREATED,
        data: { leadId: lead.id, scheduleEmails: true }
      })));
    }
    
    return {
      status: 'completed',
      imported: createdLeads.length,
      skipped: leads.length - validLeads.length,
      batchId
    };
  }
);

module.exports = {
  scheduleFollowups,
  triggerConditional,
  onLeadCreated,
  bulkImport
};
