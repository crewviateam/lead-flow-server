const EventBus = require('../EventBus');
const NotificationService = require('../../services/NotificationService');
const { prisma } = require('../../lib/prisma');

class NotificationHandler {
  constructor() {
    this.setupListeners();
    // In-memory cache for recent notifications (to avoid DB lookups on every event)
    this.recentNotifications = new Map();
    // Clean cache every 10 minutes
    setInterval(() => this.cleanOldNotifications(), 600000);
  }

  setupListeners() {
    // Email Events
    EventBus.on('EmailSent', payload => this.handleEmailEvent('sent', payload));
    EventBus.on('EmailDelivered', payload => this.handleEmailEvent('delivered', payload));
    EventBus.on('EmailOpened', payload => this.handleEmailEvent('opened', payload));
    EventBus.on('EmailClicked', payload => this.handleEmailEvent('clicked', payload));
    EventBus.on('EmailFailed', payload => this.handleEmailEvent('failed', payload));
    EventBus.on('EmailBounced', payload => this.handleEmailEvent('bounced', payload));
    // NOTE: Removed 'scheduled' - too many notifications for regular scheduling
    
    // Lead Events
    EventBus.on('LeadCreated', payload => {
        this.createWithDedup({
            type: 'info',
            message: `New Lead: ${payload.name}`,
            details: `Imported from ${payload.city || 'Unknown'}, ${payload.country || ''}`,
            metadata: { leadId: payload.leadId, event: 'lead_created' }
        }, `lead_created_${payload.leadId}`);
    });
  }

  /**
   * Generate a unique key for deduplication
   */
  getDedupKey(leadId, eventType, emailJobId) {
    return `${leadId || 'unknown'}_${eventType}_${emailJobId || 'none'}`;
  }

  /**
   * Check if notification was recently created (within 5 minutes)
   */
  isDuplicate(dedupKey) {
    const cached = this.recentNotifications.get(dedupKey);
    if (cached) {
      const fiveMinutesAgo = Date.now() - 300000;
      if (cached > fiveMinutesAgo) {
        return true;
      }
    }
    return false;
  }

  /**
   * Clean old entries from cache
   */
  cleanOldNotifications() {
    const fiveMinutesAgo = Date.now() - 300000;
    for (const [key, timestamp] of this.recentNotifications.entries()) {
      if (timestamp < fiveMinutesAgo) {
        this.recentNotifications.delete(key);
      }
    }
  }

  async handleEmailEvent(eventType, payload) {
    try {
        const { leadId, emailJobId, leadEmail, email, type } = payload;
        const targetEmail = leadEmail || email || 'a lead';
        
        // Create deduplication key
        const dedupKey = this.getDedupKey(leadId, eventType, emailJobId);
        
        // Skip if duplicate
        if (this.isDuplicate(dedupKey)) {
          console.log(`[NotificationHandler] Skipping duplicate: ${eventType} for ${targetEmail}`);
          return;
        }
        
        let msg = '';
        let notifType = 'info';

        switch (eventType) {
            case 'sent':
                msg = `📧 Sent ${type || 'email'} to ${targetEmail}`;
                notifType = 'success';
                break;
            case 'delivered':
                msg = `✅ Delivered to ${targetEmail}`;
                notifType = 'success';
                break;
            case 'opened':
                // ACHIEVEMENT: Highlight opened events specially!
                msg = `🏆 ACHIEVEMENT! ${targetEmail} opened your ${type || 'email'}!`;
                notifType = 'achievement';
                break;
            case 'clicked':
                // Also highlight clicks as achievements
                msg = `🎯 CLICK! ${targetEmail} clicked a link in ${type || 'email'}!`;
                notifType = 'achievement';
                break;
            case 'failed':
                msg = `❌ Failed to send to ${targetEmail}`;
                notifType = 'error';
                break;
            case 'bounced':
                msg = `⚠️ Email bounced for ${targetEmail}`;
                notifType = 'error';
                break;
        }

        if (msg) {
           await this.createWithDedup({
               type: notifType,
               message: msg,
               metadata: { leadId, emailJobId, event: eventType }
           }, dedupKey);
        }
    } catch (err) {
        console.error('NotificationHandler Error:', err);
    }
  }

  async createWithDedup(data, dedupKey) {
    // Mark as created in cache
    this.recentNotifications.set(dedupKey, Date.now());
    
    // Also check DB for recent duplicates (belt and suspenders)
    try {
      // Parse leadId as integer since Notification model expects Int
      const leadIdInt = data.metadata?.leadId
        ? parseInt(data.metadata.leadId)
        : null;

      const existing = await prisma.notification.findFirst({
        where: {
          event: data.metadata?.event,
          leadId: leadIdInt,
          createdAt: { gte: new Date(Date.now() - 300000) }, // Last 5 min
        },
      });

      if (existing) {
        console.log(`[NotificationHandler] DB duplicate found, skipping`);
        return;
      }
    } catch (err) {
      // Continue if DB check fails
    }
    
    await NotificationService.createNotification(data);
  }
}

module.exports = new NotificationHandler();

