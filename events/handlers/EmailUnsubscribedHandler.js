// events/handlers/EmailUnsubscribedHandler.js
// Handles: unsubscribed events
// When a lead unsubscribes, pause ALL future mails

const EventBus = require('../EventBus');
const { prisma } = require('../../lib/prisma');
const { LeadRepository } = require('../../repositories');
const RulebookService = require('../../services/RulebookService');

EventBus.on('EmailUnsubscribed', async (payload) => {
  try {
    console.log('[UnsubscribedHandler] Unsubscribed event received:', payload);

    const { emailJobId, leadId, eventData } = payload;

    // 1. Update the email job status
    if (emailJobId) {
      await prisma.emailJob.update({
        where: { id: parseInt(emailJobId) },
        data: { 
          status: 'unsubscribed',
          failedAt: new Date(),
          lastError: eventData?.reason || 'Lead unsubscribed'
        }
      });
    }

    // 2. Get the lead
    const lead = await LeadRepository.findById(leadId);
    if (!lead) {
      console.warn(`[UnsubscribedHandler] Lead ${leadId} not found.`);
      return;
    }

    // 3. Update lead to terminal state
    await prisma.lead.update({
      where: { id: parseInt(leadId) },
      data: {
        status: 'unsubscribed',
        terminalState: 'unsubscribed',
        terminalStateAt: new Date(),
        terminalReason: eventData?.reason || 'Lead opted out of email communications'
      }
    });

    // 4. Cancel ALL pending/scheduled emails for this lead (compliance requirement)
    const cancelResult = await prisma.emailJob.updateMany({
      where: { 
        leadId: parseInt(leadId),
        status: { in: RulebookService.getActiveStatuses() },
        NOT: { id: parseInt(emailJobId) } // Don't update the triggering job again
      },
      data: { 
        status: 'cancelled',
        lastError: 'Lead unsubscribed - all future mails stopped for compliance'
      }
    });

    console.log(`[UnsubscribedHandler] Cancelled ${cancelResult.count} pending jobs for lead ${leadId}`);

    // 5. Add event to lead history
    await LeadRepository.addEvent(
      leadId,
      'unsubscribed',
      {
        reason: eventData?.reason || 'Unsubscribed',
        cancelledJobs: cancelResult.count,
        source: 'EmailUnsubscribedHandler',
        eventData
      },
      null,
      emailJobId
    );

    // 6. Create notification for admin
    await prisma.notification.create({
      data: {
        type: 'info',
        message: `Lead ${lead.name || lead.email} unsubscribed`,
        details: `All ${cancelResult.count + 1} emails stopped. Lead moved to unsubscribed list.`,
        leadId: parseInt(leadId),
        emailJobId: emailJobId ? parseInt(emailJobId) : null,
        event: 'unsubscribed'
      }
    });

    console.log(`[UnsubscribedHandler] Lead ${lead.email} marked as unsubscribed, all future mails stopped`);

  } catch (error) {
    console.error('[UnsubscribedHandler] Error handling EmailUnsubscribed event:', error);
  }
});

console.log('[UnsubscribedHandler] Registered EmailUnsubscribed handler');
