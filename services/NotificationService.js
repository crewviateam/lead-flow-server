// services/NotificationService.js
// Notification service using Prisma + Firebase Cloud Messaging

const { NotificationRepository } = require('../repositories');

class NotificationService {
  constructor() {
    // Lazy load FCMService to avoid circular dependencies
    this._fcmService = null;
  }
  
  /**
   * Get FCMService instance (lazy loaded)
   */
  get fcmService() {
    if (!this._fcmService) {
      try {
        this._fcmService = require('./FCMService');
      } catch (error) {
        console.log('[NotificationService] FCM not available:', error.message);
        this._fcmService = null;
      }
    }
    return this._fcmService;
  }
  
  /**
   * Create a notification and optionally send push
   * @param {object} data - Notification data
   * @param {boolean} sendPush - Whether to send FCM push notification (default: true)
   */
  async createNotification(data, sendPush = true) {
    try {
      // Save to database
      const notification = await NotificationRepository.create({
        type: data.type || 'info',
        message: data.message,
        details: data.details,
        metadata: data.metadata,
        event: data.metadata?.event,
        leadId: data.metadata?.leadId || null
      });
      
      // Send push notification if enabled and FCM is configured
      if (sendPush && this.fcmService?.isReady()) {
        await this.sendPushNotification(data);
      }
      
      return notification;
    } catch (error) {
      console.error('Failed to create notification:', error);
      return null;
    }
  }

  /**
   * Send FCM push notification
   */
  async sendPushNotification(data) {
    try {
      if (!this.fcmService?.isReady()) {
        return { success: false, reason: 'fcm_not_ready' };
      }
      
      // Map notification types to titles
      const titles = {
        'achievement': '🏆 Achievement!',
        'success': '✅ Success',
        'error': '❌ Error',
        'warning': '⚠️ Warning',
        'info': '📧 Update'
      };
      
      const result = await this.fcmService.sendToAll({
        title: titles[data.type] || '📧 Update',
        body: data.message,
        type: data.type,
        tag: data.metadata?.event || 'notification',
        data: {
          leadId: String(data.metadata?.leadId || ''),
          emailJobId: String(data.metadata?.emailJobId || ''),
          event: data.metadata?.event || ''
        },
        link: data.metadata?.leadId ? `/leads/${data.metadata.leadId}` : '/'
      });
      
      return result;
    } catch (error) {
      console.error('[NotificationService] Push failed:', error.message);
      return { success: false, reason: error.message };
    }
  }

  /**
   * Get notifications (paginated)
   */
  async getNotifications(page = 1, limit = 20, unreadOnly = false) {
    return await NotificationRepository.findMany({ page, limit, unreadOnly });
  }

  /**
   * Mark specific notification or all as read
   */
  async markRead(id = null) {
    return await NotificationRepository.markRead(id);
  }
  
  /**
   * Register device token for push notifications
   */
  async registerDeviceToken(userId, token, platform = 'web') {
    if (!this.fcmService) {
      return { success: false, reason: 'fcm_not_available' };
    }
    return await this.fcmService.registerToken(userId, token, platform);
  }
  
  /**
   * Unregister device token
   */
  async unregisterDeviceToken(token) {
    if (!this.fcmService) {
      return { success: false, reason: 'fcm_not_available' };
    }
    return await this.fcmService.unregisterToken(token);
  }
}

module.exports = new NotificationService();

