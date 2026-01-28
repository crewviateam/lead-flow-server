// controllers/WebhookController.js
const { analyticsQueue } = require('../queues/emailQueues');
const { loggers } = require('../lib/logger');
const ingestService = require('../lib/ingest');

const log = loggers.webhook || loggers.default;

class WebhookController {
  async handleBrevoWebhook(req, res) {
    try {
      const events = req.body;

      // Brevo sends events as an array or single object
      const eventArray = Array.isArray(events) ? events : [events];
      
      let processed = 0;
      let skipped = 0;

      for (const event of eventArray) {
        // Use ingest service for deduplication
        const messageId = event['message-id'] || event.messageId;
        const eventType = event.event;
        
        if (messageId && eventType) {
          const dedupKey = ingestService.getWebhookEventKey(messageId, eventType);
          
          if (ingestService.isDuplicate(dedupKey)) {
            log.debug({ messageId, eventType }, 'Duplicate webhook event, skipping');
            skipped++;
            continue;
          }
          
          ingestService.markProcessed(dedupKey);
        }
        
        // Queue analytics processing
        await analyticsQueue.add('process-webhook', {
          eventType: eventType,
          eventData: event
        });
        
        processed++;
      }

      log.info({ processed, skipped, total: eventArray.length }, 'Webhook batch processed');

      // Always return 200 to acknowledge receipt
      res.status(200).json({ message: 'Webhook received', processed, skipped });
    } catch (error) {
      log.error({ error: error.message }, 'Webhook processing error');
      // Still return 200 to prevent retries for malformed data
      res.status(200).json({ message: 'Webhook processed with errors' });
    }
  }

  async verifyWebhook(req, res) {
    // Brevo webhook verification endpoint
    res.status(200).send('Webhook endpoint verified');
  }
}

module.exports = new WebhookController();