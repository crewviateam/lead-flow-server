const EventBus = require('../EventBus');
const StatusUpdateService = require('../../services/StatusUpdateService');

// events/handlers/EmailSentHandler.js
EventBus.on('EmailSent', async (payload) => {
  try {
    console.log('EmailSent event received:', payload);

    const { emailJobId, leadId, type, sentAt, brevoMessageId } = payload;
    
    // Use centralized service to update status
    // This handles:
    // 1. lead.addEvent (syncs manual mails, logs history, updates counters)
    // 2. lead.recalculateStatus (updates overall status)
    // 3. saving the lead
    await StatusUpdateService.updateStatus(
        leadId, 
        'sent', 
        { 
            brevoMessageId, 
            sentAt: sentAt || new Date() 
        }, 
        emailJobId, 
        type
    );
     
  } catch (error) {
    console.error('Error handling EmailSent event:', error);
  }
});