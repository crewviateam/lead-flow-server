// inngest/index.js
// Central export for all Inngest functions
// Register all functions when integrating with Express

const { inngest, EVENTS } = require('./client');

// Import all functions
const { sendEmail, retryEmail, processWebhook } = require('./functions/emailFunctions');
const { scheduleFollowups, triggerConditional, onLeadCreated, bulkImport } = require('./functions/workflowFunctions');

// All functions to register with Inngest
const functions = [
  // Email functions
  sendEmail,
  retryEmail,
  processWebhook,
  
  // Workflow functions
  scheduleFollowups,
  triggerConditional,
  onLeadCreated,
  bulkImport
];

module.exports = {
  inngest,
  EVENTS,
  functions
};
