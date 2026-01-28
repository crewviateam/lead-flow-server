// lib/websocket.js
// WebSocket service with Redis Pub/Sub for horizontal scaling
// Provides real-time updates for leads, jobs, notifications, analytics

const { Server } = require('socket.io');
const { createAdapter } = require('@socket.io/redis-adapter');
const Redis = require('ioredis');

class WebSocketService {
  constructor() {
    this.io = null;
    this.pubClient = null;
    this.subClient = null;
    this.isInitialized = false;
    
    // Event types for type safety
    this.EVENTS = {
      // Lead events
      LEAD_CREATED: 'lead:created',
      LEAD_UPDATED: 'lead:updated',
      LEAD_STATUS_CHANGED: 'lead:status_changed',
      LEAD_DELETED: 'lead:deleted',
      
      // Email job events
      JOB_CREATED: 'job:created',
      JOB_UPDATED: 'job:updated',
      JOB_STATUS_CHANGED: 'job:status_changed',
      JOB_SENT: 'job:sent',
      JOB_DELIVERED: 'job:delivered',
      JOB_OPENED: 'job:opened',
      JOB_CLICKED: 'job:clicked',
      JOB_FAILED: 'job:failed',
      JOB_CANCELLED: 'job:cancelled',
      
      // Notification events
      NOTIFICATION_NEW: 'notification:new',
      NOTIFICATION_READ: 'notification:read',
      
      // Analytics events
      ANALYTICS_UPDATE: 'analytics:update',
      
      // Schedule events
      SCHEDULE_UPDATE: 'schedule:update',
      
      // System events
      QUEUE_UPDATE: 'queue:update'
    };
    
    // Cache for preventing duplicate emits
    this._lastEmitCache = new Map();
    this._emitCacheTTL = 1000; // 1 second dedup window
  }

  /**
   * Initialize WebSocket server with Redis adapter
   * @param {http.Server} httpServer - The HTTP server instance
   * @param {Object} redisConnection - Existing Redis connection
   */
  async init(httpServer, redisConnection) {
    if (this.isInitialized) {
      console.warn('[WebSocket] Already initialized');
      return this.io;
    }

    // Create Socket.IO server with optimized settings
    this.io = new Server(httpServer, {
      cors: {
        origin: process.env.CORS_ORIGIN || '*',
        methods: ['GET', 'POST'],
        credentials: true
      },
      // Performance optimizations
      pingTimeout: 60000,
      pingInterval: 25000,
      transports: ['websocket', 'polling'],
      // Limit payload size
      maxHttpBufferSize: 1e6, // 1MB
      // Connection state recovery (for reconnects)
      connectionStateRecovery: {
        maxDisconnectionDuration: 2 * 60 * 1000, // 2 minutes
        skipMiddlewares: true
      }
    });

    // Setup Redis adapter for horizontal scaling
    try {
      const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
      
      this.pubClient = new Redis(redisUrl, {
        retryDelayOnFailover: 100,
        maxRetriesPerRequest: 3,
        lazyConnect: true
      });
      
      this.subClient = this.pubClient.duplicate();
      
      await Promise.all([
        this.pubClient.connect(),
        this.subClient.connect()
      ]);
      
      this.io.adapter(createAdapter(this.pubClient, this.subClient));
      console.log('[WebSocket] Redis adapter connected for horizontal scaling');
    } catch (error) {
      console.warn('[WebSocket] Redis adapter failed, running in single-instance mode:', error.message);
    }

    // Setup connection handlers
    this._setupConnectionHandlers();
    
    this.isInitialized = true;
    console.log('✅ WebSocket server initialized');
    
    return this.io;
  }

  /**
   * Setup connection event handlers
   */
  _setupConnectionHandlers() {
    this.io.on('connection', (socket) => {
      console.log(`[WebSocket] Client connected: ${socket.id}`);
      
      // Join rooms based on subscription requests
      socket.on('subscribe', (rooms) => {
        if (Array.isArray(rooms)) {
          rooms.forEach(room => socket.join(room));
          console.log(`[WebSocket] ${socket.id} joined rooms:`, rooms);
        }
      });
      
      socket.on('unsubscribe', (rooms) => {
        if (Array.isArray(rooms)) {
          rooms.forEach(room => socket.leave(room));
        }
      });
      
      // Subscribe to specific lead updates
      socket.on('subscribe:lead', (leadId) => {
        socket.join(`lead:${leadId}`);
      });
      
      // Subscribe to job updates
      socket.on('subscribe:jobs', () => {
        socket.join('jobs');
      });
      
      // Subscribe to analytics updates
      socket.on('subscribe:analytics', () => {
        socket.join('analytics');
      });
      
      // Subscribe to notifications
      socket.on('subscribe:notifications', () => {
        socket.join('notifications');
      });
      
      socket.on('disconnect', (reason) => {
        console.log(`[WebSocket] Client disconnected: ${socket.id}, reason: ${reason}`);
      });
    });
  }

  /**
   * Emit event with deduplication
   * @param {string} event - Event name
   * @param {*} data - Event data
   * @param {string} room - Optional room to emit to
   */
  emit(event, data, room = null) {
    if (!this.io) {
      console.warn('[WebSocket] Not initialized, skipping emit');
      return;
    }
    
    // Deduplicate rapid-fire events
    const cacheKey = `${event}:${room || 'global'}:${JSON.stringify(data)}`;
    const lastEmit = this._lastEmitCache.get(cacheKey);
    
    if (lastEmit && Date.now() - lastEmit < this._emitCacheTTL) {
      return; // Skip duplicate
    }
    
    this._lastEmitCache.set(cacheKey, Date.now());
    
    // Clean old cache entries periodically
    if (this._lastEmitCache.size > 1000) {
      const now = Date.now();
      for (const [key, time] of this._lastEmitCache.entries()) {
        if (now - time > this._emitCacheTTL) {
          this._lastEmitCache.delete(key);
        }
      }
    }
    
    // Emit to specific room or broadcast
    if (room) {
      this.io.to(room).emit(event, data);
    } else {
      this.io.emit(event, data);
    }
  }

  // ==========================================
  // LEAD EVENTS
  // ==========================================
  
  emitLeadCreated(lead) {
    this.emit(this.EVENTS.LEAD_CREATED, { lead }, 'leads');
  }
  
  emitLeadUpdated(lead) {
    this.emit(this.EVENTS.LEAD_UPDATED, { lead }, 'leads');
    this.emit(this.EVENTS.LEAD_UPDATED, { lead }, `lead:${lead.id}`);
  }
  
  emitLeadStatusChanged(leadId, newStatus, oldStatus, reason) {
    const data = { leadId, newStatus, oldStatus, reason, timestamp: new Date().toISOString() };
    this.emit(this.EVENTS.LEAD_STATUS_CHANGED, data, 'leads');
    this.emit(this.EVENTS.LEAD_STATUS_CHANGED, data, `lead:${leadId}`);
  }
  
  emitLeadDeleted(leadId) {
    this.emit(this.EVENTS.LEAD_DELETED, { leadId }, 'leads');
  }

  // ==========================================
  // JOB EVENTS
  // ==========================================
  
  emitJobCreated(job) {
    this.emit(this.EVENTS.JOB_CREATED, { job }, 'jobs');
    this.emit(this.EVENTS.JOB_CREATED, { job }, `lead:${job.leadId}`);
  }
  
  emitJobUpdated(job) {
    this.emit(this.EVENTS.JOB_UPDATED, { job }, 'jobs');
    this.emit(this.EVENTS.JOB_UPDATED, { job }, `lead:${job.leadId}`);
  }
  
  emitJobStatusChanged(job, oldStatus) {
    const data = { 
      job, 
      oldStatus, 
      newStatus: job.status, 
      timestamp: new Date().toISOString() 
    };
    this.emit(this.EVENTS.JOB_STATUS_CHANGED, data, 'jobs');
    this.emit(this.EVENTS.JOB_STATUS_CHANGED, data, `lead:${job.leadId}`);
  }
  
  emitJobSent(job) {
    this.emit(this.EVENTS.JOB_SENT, { job }, 'jobs');
    this.emit(this.EVENTS.JOB_SENT, { job }, `lead:${job.leadId}`);
  }
  
  emitJobDelivered(job) {
    this.emit(this.EVENTS.JOB_DELIVERED, { job }, 'jobs');
  }
  
  emitJobOpened(job) {
    this.emit(this.EVENTS.JOB_OPENED, { job }, 'jobs');
  }
  
  emitJobClicked(job) {
    this.emit(this.EVENTS.JOB_CLICKED, { job }, 'jobs');
  }
  
  emitJobFailed(job, error) {
    this.emit(this.EVENTS.JOB_FAILED, { job, error }, 'jobs');
    this.emit(this.EVENTS.JOB_FAILED, { job, error }, `lead:${job.leadId}`);
  }

  // ==========================================
  // NOTIFICATION EVENTS
  // ==========================================
  
  emitNotification(notification) {
    this.emit(this.EVENTS.NOTIFICATION_NEW, { notification }, 'notifications');
  }

  // ==========================================
  // ANALYTICS EVENTS  
  // ==========================================
  
  emitAnalyticsUpdate(analytics) {
    this.emit(this.EVENTS.ANALYTICS_UPDATE, { analytics }, 'analytics');
  }

  // ==========================================
  // SCHEDULE EVENTS
  // ==========================================
  
  emitScheduleUpdate(slots) {
    this.emit(this.EVENTS.SCHEDULE_UPDATE, { slots }, 'schedule');
  }

  // ==========================================
  // QUEUE EVENTS
  // ==========================================
  
  emitQueueUpdate(queueStats) {
    this.emit(this.EVENTS.QUEUE_UPDATE, { queueStats }, 'queue');
  }

  /**
   * Get connection statistics
   */
  getStats() {
    if (!this.io) return { connected: 0, rooms: 0 };
    
    return {
      connected: this.io.engine.clientsCount,
      rooms: this.io.sockets.adapter.rooms.size
    };
  }

  /**
   * Graceful shutdown
   */
  async close() {
    if (this.io) {
      this.io.close();
    }
    if (this.pubClient) {
      await this.pubClient.quit();
    }
    if (this.subClient) {
      await this.subClient.quit();
    }
    this.isInitialized = false;
    console.log('[WebSocket] Server closed');
  }
}

// Singleton instance
const websocketService = new WebSocketService();

module.exports = websocketService;
