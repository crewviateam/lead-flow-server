// events/handlers/EmailClickedHandler.js
// Event handler using Prisma - FIXED: Uses StatusUpdateService

const EventBus = require("../EventBus");
const { prisma } = require("../../lib/prisma");
const { EmailJobRepository, LeadRepository } = require("../../repositories");
const StatusUpdateService = require("../../services/StatusUpdateService");

EventBus.on("EmailClicked", async (payload) => {
  try {
    console.log("EmailClicked event received:", payload);
    const { leadId, emailJobId } = payload;

    let job = null;
    if (emailJobId) {
      job = await EmailJobRepository.findById(emailJobId);

      // Update the EMAIL JOB status to 'clicked'
      await prisma.emailJob.update({
        where: { id: emailJobId },
        data: { status: "clicked" },
      });
    }

    if (leadId) {
      const lead = await LeadRepository.findById(leadId);
      if (!lead) {
        console.warn(`[ClickedHandler] Lead ${leadId} not found.`);
        return;
      }

      // NOTE: We do NOT cancel/pause followups here!
      // If a conditional email exists for 'clicked' trigger, it will be evaluated by
      // AnalyticsService.recordEvent -> ConditionalEmailService.evaluateTriggers
      // And that will call RulebookService.pauseLowerPriorityJobs() which properly PAUSES followups
      //
      // If NO conditional email exists, followups should CONTINUE NORMALLY!
      // This is the expected behavior - click engagement doesn't stop the sequence unless
      // a conditional email is configured to respond to it.
      console.log(
        `[ClickedHandler] Lead ${leadId} clicked - not cancelling followups (handled by ConditionalEmailService if needed)`,
      );

      // Increment clicked counter
      await prisma.lead.update({
        where: { id: parseInt(leadId) },
        data: { emailsClicked: { increment: 1 } },
      });

      // Use StatusUpdateService to update lead status with priority logic
      // This will NOT overwrite scheduled status if a conditional email is pending
      await StatusUpdateService.updateStatus(
        lead.id,
        "clicked",
        { source: "EmailClickedHandler" },
        emailJobId,
        job?.type,
      );

      console.log(
        `[ClickedHandler] Processed clicked event for lead ${lead.email}`,
      );
    }
  } catch (error) {
    console.error("Error handling EmailClicked event:", error);
  }
});