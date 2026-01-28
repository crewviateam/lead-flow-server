// events/handlers/LeadCreatedHandler.js
// Event handler using Prisma

const EventBus = require('../EventBus');

EventBus.on('LeadCreated', async (payload) => {
  try {
    console.log('LeadCreated event received:', payload);
    // Additional business logic could go here
  } catch (error) {
    console.error('Error handling LeadCreated event:', error);
  }
});