// events/handlers/index.js
// Import all handlers to ensure they register with EventBus
require('./LeadCreatedHandler');
require('./EmailSentHandler');
require('./EmailFailedHandler');
require('./EmailOpenedHandler');
require('./EmailDeliveredHandler');
require('./EmailClickedHandler');
require('./EmailBouncedHandler');
require('./EmailDeferredHandler');
require('./FollowUpScheduledHandler');
require('./NotificationHandler');

// NEW: Terminal state handlers
require('./EmailComplaintHandler');
require('./EmailUnsubscribedHandler');

module.exports = require('../EventBus');