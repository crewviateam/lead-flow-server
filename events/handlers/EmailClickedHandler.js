// events/handlers/EmailClickedHandler.js
// Event handler using Prisma - FIXED: Uses StatusUpdateService

const EventBus = require('../EventBus');
const { prisma } = require('../../lib/prisma');
const { EmailJobRepository, LeadRepository } = require('../../repositories');
const StatusUpdateService = require('../../services/StatusUpdateService');
const RulebookService = require('../../services/RulebookService');

EventBus.on('EmailClicked', async (payload) => {
  try {
    console.log('EmailClicked event received:', payload);
    const { leadId, emailJobId } = payload;

    let job = null;
    if (emailJobId) {
      job = await EmailJobRepository.findById(emailJobId);
      
      // Update the EMAIL JOB status to 'clicked'
      await prisma.emailJob.update({
        where: { id: emailJobId },
        data: { status: 'clicked' }
      });
    }

    if (leadId) {
      const lead = await LeadRepository.findById(leadId);
      if (!lead) {
        console.warn(`[ClickedHandler] Lead ${leadId} not found.`);
        return;
      }

      // Cancel all pending/queued FOLLOWUP jobs for this lead (not conditional)
      console.log(`Lead ${leadId} clicked! Cancelling future followups.`);
      
      const result = await prisma.emailJob.updateMany({
        where: { 
          leadId: parseInt(leadId), 
          status: { in: RulebookService.getAwaitingDeliveryStatuses() },
          // Don't cancel conditional emails - they may still be wanted
          NOT: { type: { startsWith: 'conditional' } }
        },
        data: { 
          status: 'cancelled', 
          lastError: 'Stopped: Lead clicked a link' 
        }
      });
      
      console.log(`Cancelled ${result.count} pending jobs for lead ${leadId}`);

      // Increment clicked counter
      await prisma.lead.update({
        where: { id: parseInt(leadId) },
        data: { emailsClicked: { increment: 1 } }
      });
      
      // Use StatusUpdateService to update lead status with priority logic
      // This will NOT overwrite scheduled status if a conditional email is pending
      await StatusUpdateService.updateStatus(
        lead.id, 
        'clicked', 
        { source: 'EmailClickedHandler' }, 
        emailJobId, 
        job?.type
      );

      console.log(`[ClickedHandler] Processed clicked event for lead ${lead.email}`);
    }
  } catch (error) {
    console.error('Error handling EmailClicked event:', error);
  }
});