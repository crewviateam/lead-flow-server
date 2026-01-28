const EventBus = require('../EventBus');

// events/handlers/FollowUpScheduledHandler.js
EventBus.on('FollowUpScheduled', async (payload) => {
  try {
    console.log('FollowUpScheduled event received:', payload);

    // Additional logic for follow-up tracking
  } catch (error) {
    console.error('Error handling FollowUpScheduled event:', error);
  }
});