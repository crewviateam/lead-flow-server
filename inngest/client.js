// inngest/client.js
// Inngest client configuration
// Central client for all Inngest functions

const { Inngest } = require('inngest');

// Create Inngest client
const inngest = new Inngest({
  id: 'lead-email-system',
  // Event key for production (optional for dev)
  eventKey: process.env.INNGEST_EVENT_KEY,
  // Signing key for security
  signingKey: process.env.INNGEST_SIGNING_KEY,
});

// Event types for type safety and documentation
const EVENTS = {
  // Lead lifecycle events
  LEAD_CREATED: 'lead/created',
  LEAD_UPDATED: 'lead/updated',
  LEAD_FROZEN: 'lead/frozen',
  LEAD_UNFROZEN: 'lead/unfrozen',
  LEAD_CONVERTED: 'lead/converted',
  
  // Email job events
  EMAIL_SCHEDULE: 'email/schedule',
  EMAIL_SEND: 'email/send',
  EMAIL_RETRY: 'email/retry',
  EMAIL_DELIVERED: 'email/delivered',
  EMAIL_OPENED: 'email/opened',
  EMAIL_CLICKED: 'email/clicked',
  EMAIL_BOUNCED: 'email/bounced',
  EMAIL_FAILED: 'email/failed',
  
  // Webhook events from Brevo
  WEBHOOK_RECEIVED: 'webhook/received',
  
  // Followup workflow events
  FOLLOWUP_SCHEDULE: 'followup/schedule',
  FOLLOWUP_TRIGGER: 'followup/trigger',
  
  // Conditional email events
  CONDITIONAL_TRIGGER: 'conditional/trigger',
  CONDITIONAL_EVALUATE: 'conditional/evaluate',
  
  // Bulk operations
  BULK_SCHEDULE: 'bulk/schedule',
  BULK_IMPORT: 'bulk/import',
  
  // Analytics events
  ANALYTICS_REFRESH: 'analytics/refresh'
};

module.exports = {
  inngest,
  EVENTS
};
