// events/handlers/EmailOpenedHandler.js
// Event handler using Prisma - FIXED: Uses StatusUpdateService

const EventBus = require('../EventBus');
const { prisma } = require('../../lib/prisma');
const { EmailJobRepository, LeadRepository } = require('../../repositories');
const StatusUpdateService = require('../../services/StatusUpdateService');

EventBus.on('EmailOpened', async (payload) => {
  try {
    console.log('EmailOpened event received:', payload);
    const { emailJobId } = payload;
    
    if (!emailJobId) return;
    
    const job = await EmailJobRepository.findById(emailJobId);
    if (!job) {
      console.warn(`[OpenedHandler] Job ${emailJobId} not found.`);
      return;
    }
    
    const lead = await LeadRepository.findById(job.leadId);
    if (!lead) {
      console.warn(`[OpenedHandler] Lead ${job.leadId} not found.`);
      return;
    }
    
    // Update the EMAIL JOB status to 'opened' (not the lead status directly)
    await prisma.emailJob.update({
      where: { id: emailJobId },
      data: { status: 'opened' }
    });
    
    // Increment opened counter on lead
    await prisma.lead.update({
      where: { id: lead.id },
      data: { emailsOpened: { increment: 1 } }
    });
    
    // Use StatusUpdateService to update lead status with priority logic
    // This will NOT overwrite scheduled status if a conditional email is pending
    await StatusUpdateService.updateStatus(
      lead.id, 
      'opened', 
      { source: 'EmailOpenedHandler' }, 
      emailJobId, 
      job.type
    );
    
    console.log(`[OpenedHandler] Processed opened event for lead ${lead.email}, job ${emailJobId}`);
    
  } catch (error) {
    console.error('Error handling EmailOpened event:', error);
  }
});
