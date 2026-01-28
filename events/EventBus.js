// events/EventBus.js
// Event bus using Prisma with WebSocket integration for real-time updates

const EventEmitter = require('events');
const { prisma } = require('../lib/prisma');

// Lazy-load WebSocket to avoid circular dependencies
let websocketService = null;
const getWebSocket = () => {
  if (!websocketService) {
    try {
      websocketService = require('../lib/websocket');
    } catch (e) {
      // WebSocket not available yet
    }
  }
  return websocketService;
};

class EventBus extends EventEmitter {
  constructor() {
    super();
    this.setMaxListeners(50);
    
    // Event type to WebSocket event mapping
    this.wsEventMap = {
      'email.sent': 'job:sent',
      'email.delivered': 'job:delivered',
      'email.opened': 'job:opened',
      'email.clicked': 'job:clicked',
      'email.bounced': 'job:failed',
      'email.failed': 'job:failed',
      'lead.created': 'lead:created',
      'lead.updated': 'lead:updated',
      'lead.statusChanged': 'lead:status_changed',
      'lead.frozen': 'lead:updated',
      'lead.unfrozen': 'lead:updated',
      'lead.converted': 'lead:updated',
      'job.created': 'job:created',
      'job.cancelled': 'job:cancelled',
      'job.rescheduled': 'job:updated',
      'notification.created': 'notification:new'
    };
  }

  async emit(eventType, payload, metadata = {}) {
    // Persist event to event store using Prisma
    let isNewEvent = true;
    try {
      await prisma.eventStore.create({
        data: {
          eventType,
          aggregateId: payload.leadId?.toString() || payload.emailJobId?.toString() || 'system',
          aggregateType: payload.leadId ? 'Lead' : 'EmailJob',
          payload,
          metadata: {
            ...metadata,
            source: payload.source || 'system'
          }
        }
      });
    } catch (error) {
      // Check for unique constraint violation (event already processed)
      if (error.code === 'P2002') {
        console.log(`Event ${eventType} for ${payload.emailJobId || payload.leadId} already processed, skipping`);
        isNewEvent = false;
      } else {
        console.error('Failed to persist event:', error);
      }
    }

    // Only emit to handlers and WebSocket if this is a NEW event
    if (isNewEvent) {
      super.emit(eventType, payload);
      
      // Emit to WebSocket for real-time updates
      this._emitToWebSocket(eventType, payload);
    }
  }

  /**
   * Emit event to WebSocket clients
   * @private
   */
  _emitToWebSocket(eventType, payload) {
    const ws = getWebSocket();
    if (!ws || !ws.isInitialized) return;
    
    // Map internal event to WebSocket event
    const wsEvent = this.wsEventMap[eventType];
    if (!wsEvent) return;
    
    // Determine the room to emit to
    const leadId = payload.leadId || payload.lead?.id;
    
    // Emit to appropriate channels
    if (wsEvent.startsWith('lead:')) {
      if (leadId) {
        ws.emit(wsEvent, payload, `lead:${leadId}`);
      }
      ws.emit(wsEvent, payload, 'leads');
    } else if (wsEvent.startsWith('job:')) {
      if (leadId) {
        ws.emit(wsEvent, payload, `lead:${leadId}`);
      }
      ws.emit(wsEvent, payload, 'jobs');
    } else if (wsEvent.startsWith('notification:')) {
      ws.emit(wsEvent, payload, 'notifications');
    }
  }
}

const eventBus = new EventBus();

module.exports = eventBus;