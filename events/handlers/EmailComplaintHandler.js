// events/handlers/EmailComplaintHandler.js
// Handles: complaint (spam report) events
// When a lead marks email as spam, pause ALL future mails

const EventBus = require('../EventBus');
const { prisma } = require('../../lib/prisma');
const { LeadRepository } = require('../../repositories');
const RulebookService = require('../../services/RulebookService');

EventBus.on('EmailComplaint', async (payload) => {
  try {
    console.log('[ComplaintHandler] Complaint event received:', payload);

    const { emailJobId, leadId, eventData } = payload;

    // 1. Update the email job status
    if (emailJobId) {
      await prisma.emailJob.update({
        where: { id: parseInt(emailJobId) },
        data: { 
          status: 'complaint',
          failedAt: new Date(),
          lastError: eventData?.reason || 'Lead filed spam complaint'
        }
      });
    }

    // 2. Get the lead
    const lead = await LeadRepository.findById(leadId);
    if (!lead) {
      console.warn(`[ComplaintHandler] Lead ${leadId} not found.`);
      return;
    }

    // 3. Update lead to terminal state
    await prisma.lead.update({
      where: { id: parseInt(leadId) },
      data: {
        status: 'complaint',
        terminalState: 'complaint',
        terminalStateAt: new Date(),
        terminalReason: eventData?.reason || 'Lead filed spam complaint against email'
      }
    });

    // 4. Cancel ALL pending/scheduled emails for this lead (critical compliance)
    const cancelResult = await prisma.emailJob.updateMany({
      where: { 
        leadId: parseInt(leadId),
        status: { in: RulebookService.getActiveStatuses() },
        NOT: { id: parseInt(emailJobId) } // Don't update the triggering job again
      },
      data: { 
        status: 'cancelled',
        lastError: 'Lead filed spam complaint - all future mails stopped for compliance'
      }
    });

    console.log(`[ComplaintHandler] Cancelled ${cancelResult.count} pending jobs for lead ${leadId}`);

    // 5. Add event to lead history
    await LeadRepository.addEvent(
      leadId,
      'complaint',
      {
        reason: eventData?.reason || 'Spam complaint',
        cancelledJobs: cancelResult.count,
        source: 'EmailComplaintHandler',
        eventData
      },
      null,
      emailJobId
    );

    // 6. Create notification for admin
    await prisma.notification.create({
      data: {
        type: 'warning',
        message: `Lead ${lead.name || lead.email} filed spam complaint`,
        details: `All ${cancelResult.count + 1} emails stopped. Future mails paused for compliance.`,
        leadId: parseInt(leadId),
        emailJobId: emailJobId ? parseInt(emailJobId) : null,
        event: 'complaint'
      }
    });

    console.log(`[ComplaintHandler] Lead ${lead.email} marked as complaint, all future mails stopped`);

  } catch (error) {
    console.error('[ComplaintHandler] Error handling EmailComplaint event:', error);
  }
});

console.log('[ComplaintHandler] Registered EmailComplaint handler');
