// events/handlers/EmailDeliveredHandler.js
// Event handler using Prisma with IDEMPOTENCY

const EventBus = require('../EventBus');
const StatusUpdateService = require('../../services/StatusUpdateService');
const RulebookService = require('../../services/RulebookService');
const { prisma } = require('../../lib/prisma');
const { LeadRepository, EmailJobRepository } = require('../../repositories');

// Track recently processed deliveries to prevent duplicate handling
const recentDeliveries = new Map();
const DEDUP_WINDOW_MS = 60000; // 60 seconds

// Clean old entries periodically
setInterval(() => {
  const cutoff = Date.now() - DEDUP_WINDOW_MS;
  for (const [key, timestamp] of recentDeliveries.entries()) {
    if (timestamp < cutoff) recentDeliveries.delete(key);
  }
}, 60000);

EventBus.on('EmailDelivered', async (payload) => {
  try {
    console.log('EmailDelivered event received:', payload);
    const { emailJobId } = payload; 

    if (!emailJobId) return;

    // IDEMPOTENCY CHECK 1: Have we already processed this exact job recently?
    const dedupKey = `delivered_${emailJobId}`;
    if (recentDeliveries.has(dedupKey)) {
      console.log(`[DeliveredHandler] Already processed job ${emailJobId} recently, skipping`);
      return;
    }
    recentDeliveries.set(dedupKey, Date.now());

    const job = await EmailJobRepository.findById(emailJobId);
    if (!job) {
      console.warn(`[DeliveredHandler] Job ${emailJobId} not found.`);
      return;
    }

    // IDEMPOTENCY CHECK 2: Is this job already marked as delivered?
    if (['delivered', 'opened', 'clicked'].includes(job.status)) {
      console.log(`[DeliveredHandler] Job ${emailJobId} already has status '${job.status}', checking if scheduling needed`);
    }

    const updatedLead = await StatusUpdateService.updateStatus(
      job.leadId, 
      'delivered', 
      {
        brevoMessageId: payload.eventData?.messageId, 
        timestamp: new Date(),
        source: 'EmailDeliveredHandler'
      }, 
      emailJobId, 
      job.type
    );
    
    if (!updatedLead) return;

    console.log(`[DeliveredHandler] Processed delivered event for ${updatedLead.email}`);

    // Check if manual or conditional mail - resume followups if paused
    const isManual = job.metadata?.manual === true || 
                     (job.metadata?.manualTitle && job.metadata.manualTitle.length > 0);
    const isConditional = job.type?.startsWith('conditional:');
    
    if ((isManual || isConditional) && updatedLead.followupsPaused) {
      console.log(`[DeliveredHandler] ${isConditional ? 'Conditional' : 'Manual'} mail delivered. Resuming followups for ${updatedLead.email}`);
      
      // 1. Reset followupsPaused flag on lead
      await prisma.lead.update({
        where: { id: updatedLead.id },
        data: { followupsPaused: false }
      });
      
      // 2. Actually resume the paused jobs - update their status back to pending
      const pausedJobs = await prisma.emailJob.findMany({
        where: {
          leadId: updatedLead.id,
          status: 'paused',
          cancellationReason: 'priority_paused'
        }
      });
      
      if (pausedJobs.length > 0) {
        console.log(`[DeliveredHandler] Resuming ${pausedJobs.length} paused jobs for lead ${updatedLead.id}`);
        
        for (const pausedJob of pausedJobs) {
          await prisma.emailJob.update({
            where: { id: pausedJob.id },
            data: {
              status: 'pending',
              cancellationReason: null,
              lastError: `Auto-resumed after ${isConditional ? 'conditional' : 'manual'} mail delivered`,
              resumedAt: new Date()
            }
          });
          console.log(`[DeliveredHandler] Resumed job ${pausedJob.id} (${pausedJob.type})`);
        }
        
        // 3. Update emailSchedule followups status from paused to scheduled
        try {
          const leadWithSchedule = await prisma.lead.findUnique({
            where: { id: updatedLead.id },
            include: { emailSchedule: true }
          });
          
          if (leadWithSchedule?.emailSchedule) {
            let followups = leadWithSchedule.emailSchedule.followups || [];
            if (!Array.isArray(followups)) {
              try { followups = JSON.parse(followups); } catch (e) { followups = []; }
            }
            
            // Reset paused followups to scheduled
            let updated = false;
            for (const followup of followups) {
              if (followup.status === 'paused') {
                followup.status = 'scheduled';
                updated = true;
              }
            }
            
            if (updated) {
              await prisma.emailSchedule.update({
                where: { id: leadWithSchedule.emailSchedule.id },
                data: { followups }
              });
              console.log(`[DeliveredHandler] Updated emailSchedule followups status for lead ${updatedLead.id}`);
            }
          }
        } catch (e) {
          console.error(`[DeliveredHandler] Error updating emailSchedule: ${e.message}`);
        }
        
        // 4. Add event history for auto-resume
        await LeadRepository.addEvent(updatedLead.id, 'resumed', {
          reason: `Auto-resumed after ${isConditional ? 'conditional' : 'manual'} mail delivered`,
          resumedJobs: pausedJobs.map(j => ({ id: j.id, type: j.type }))
        }, 'followups');
      }
    }

    // IDEMPOTENCY CHECK 3: Check if we already have a pending followup scheduled
    // This is the most critical check to prevent duplicates
    const existingPendingFollowup = await prisma.emailJob.findFirst({
      where: {
        leadId: updatedLead.id,
        status: { in: RulebookService.getActiveStatuses() },
        type: { 
          not: { in: ['manual', 'Initial Email', 'initial'] },
          notContains: 'conditional:'
        },
        createdAt: { gte: new Date(Date.now() - 120000) } // Created in last 2 minutes
      }
    });

    if (existingPendingFollowup) {
      console.log(`[DeliveredHandler] Already has pending followup '${existingPendingFollowup.type}' for ${updatedLead.email}. Skipping schedule.`);
      return;
    }

    // Schedule next step
    console.log(`[DeliveredHandler] Triggering next step scheduler for lead ${updatedLead.id}`);
    const EmailSchedulerService = require('../../services/EmailSchedulerService');
    await EmailSchedulerService.scheduleNextEmail(updatedLead.id);
    
  } catch (error) {
    console.error('Error handling EmailDeliveredHandler event:', error);
  }
});

