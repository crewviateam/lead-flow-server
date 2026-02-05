// services/FCMService.js
// Firebase Cloud Messaging Service for push notifications

/**
 * FCMService handles sending push notifications via Firebase Cloud Messaging
 * 
 * SETUP REQUIRED:
 * 1. Create a Firebase project at https://console.firebase.google.com
 * 2. Go to Project Settings > Service Accounts
 * 3. Generate a new private key
 * 4. Save the JSON file and set FIREBASE_SERVICE_ACCOUNT_PATH env variable
 *    OR set FIREBASE_SERVICE_ACCOUNT as JSON string in env
 * 5. Enable Firebase Cloud Messaging API in your project
 */

const admin = require('firebase-admin');
const { prisma } = require('../lib/prisma');

class FCMService {
  constructor() {
    this.initialized = false;
    this.initializeFirebase();
  }

  /**
   * Initialize Firebase Admin SDK
   */
  initializeFirebase() {
    try {
      // Check if already initialized
      if (admin.apps.length > 0) {
        this.initialized = true;
        console.log('[FCM] Firebase already initialized');
        return;
      }

      // Try to initialize from environment variables
      const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_PATH;
      const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT;

      if (serviceAccountPath) {
        // Initialize from file path - resolve relative paths from project root
        const path = require("path");
        const resolvedPath = serviceAccountPath.startsWith(".")
          ? path.resolve(__dirname, "..", serviceAccountPath)
          : serviceAccountPath;
        const serviceAccount = require(resolvedPath);
        admin.initializeApp({
          credential: admin.credential.cert(serviceAccount),
        });
        this.initialized = true;
        console.log("[FCM] ✓ Firebase initialized from service account file");
      } else if (serviceAccountJson) {
        // Initialize from JSON string
        const serviceAccount = JSON.parse(serviceAccountJson);
        admin.initializeApp({
          credential: admin.credential.cert(serviceAccount)
        });
        this.initialized = true;
        console.log('[FCM] ✓ Firebase initialized from service account JSON');
      } else {
        console.log('[FCM] ⚠ Firebase not configured - push notifications disabled');
        console.log('[FCM] Set FIREBASE_SERVICE_ACCOUNT_PATH or FIREBASE_SERVICE_ACCOUNT env variable');
        this.initialized = false;
      }
    } catch (error) {
      console.error('[FCM] ❌ Firebase initialization failed:', error.message);
      this.initialized = false;
    }
  }

  /**
   * Check if FCM is ready to send notifications
   */
  isReady() {
    return this.initialized;
  }

  /**
   * Register a device token for a user
   * @param {string} userId - User identifier (can be 'admin' for single-user setup)
   * @param {string} token - FCM device token from client
   * @param {string} platform - 'web', 'android', or 'ios'
   */
  async registerToken(userId, token, platform = 'web') {
    try {
      // Store token in database (upsert to avoid duplicates)
      await prisma.deviceToken.upsert({
        where: { token },
        create: {
          userId,
          token,
          platform,
          createdAt: new Date(),
          lastUsed: new Date()
        },
        update: {
          userId,
          platform,
          lastUsed: new Date()
        }
      });
      
      console.log(`[FCM] Registered token for user ${userId} (${platform})`);
      return true;
    } catch (error) {
      console.error('[FCM] Failed to register token:', error.message);
      return false;
    }
  }

  /**
   * Unregister a device token
   * @param {string} token - FCM device token to remove
   */
  async unregisterToken(token) {
    try {
      await prisma.deviceToken.delete({
        where: { token }
      });
      console.log('[FCM] Token unregistered');
      return true;
    } catch (error) {
      console.error('[FCM] Failed to unregister token:', error.message);
      return false;
    }
  }

  /**
   * Get all active device tokens for a user
   * @param {string} userId - User identifier
   */
  async getTokensForUser(userId) {
    try {
      const tokens = await prisma.deviceToken.findMany({
        where: { userId },
        select: { token: true, platform: true }
      });
      return tokens;
    } catch (error) {
      console.error('[FCM] Failed to get tokens:', error.message);
      return [];
    }
  }

  /**
   * Send push notification to all devices
   * @param {object} notification - Notification data
   * @param {string} notification.title - Notification title
   * @param {string} notification.body - Notification body
   * @param {string} notification.type - Notification type (info, success, error, achievement)
   * @param {object} notification.data - Additional data payload
   */
  async sendToAll(notification) {
    if (!this.initialized) {
      console.log('[FCM] Not initialized, skipping push notification');
      return { success: false, reason: 'not_initialized' };
    }

    try {
      // Get all device tokens
      const tokens = await prisma.deviceToken.findMany({
        select: { token: true }
      });

      if (tokens.length === 0) {
        console.log('[FCM] No registered devices, skipping push');
        return { success: false, reason: 'no_devices' };
      }

      const tokenList = tokens.map(t => t.token);
      
      // Prepare message
      const message = {
        notification: {
          title: notification.title || 'Lead Email System',
          body: notification.body || notification.message
        },
        data: {
          type: notification.type || 'info',
          ...(notification.data || {})
        },
        webpush: {
          notification: {
            icon: '/logo192.png',
            badge: '/logo192.png',
            tag: notification.tag || 'lead-email-notification',
            requireInteraction: notification.type === 'achievement' || notification.type === 'error'
          },
          fcmOptions: {
            link: notification.link || '/'
          }
        }
      };

      // Send to all devices
      const response = await admin.messaging().sendEachForMulticast({
        tokens: tokenList,
        ...message
      });

      console.log(`[FCM] Sent to ${response.successCount}/${tokenList.length} devices`);

      // Clean up invalid tokens
      if (response.failureCount > 0) {
        const failedTokens = [];
        response.responses.forEach((resp, idx) => {
          if (!resp.success) {
            const errorCode = resp.error?.code;
            if (errorCode === 'messaging/invalid-registration-token' ||
                errorCode === 'messaging/registration-token-not-registered') {
              failedTokens.push(tokenList[idx]);
            }
          }
        });

        if (failedTokens.length > 0) {
          await this.cleanupInvalidTokens(failedTokens);
        }
      }

      return { 
        success: true, 
        sent: response.successCount, 
        failed: response.failureCount 
      };
    } catch (error) {
      console.error('[FCM] Failed to send notifications:', error.message);
      return { success: false, reason: error.message };
    }
  }

  /**
   * Send push notification to specific user
   * @param {string} userId - User identifier
   * @param {object} notification - Notification data
   */
  async sendToUser(userId, notification) {
    if (!this.initialized) {
      return { success: false, reason: 'not_initialized' };
    }

    try {
      const tokens = await this.getTokensForUser(userId);
      
      if (tokens.length === 0) {
        return { success: false, reason: 'no_devices' };
      }

      const tokenList = tokens.map(t => t.token);
      
      const message = {
        notification: {
          title: notification.title || 'Lead Email System',
          body: notification.body || notification.message
        },
        data: {
          type: notification.type || 'info',
          ...(notification.data || {})
        }
      };

      const response = await admin.messaging().sendEachForMulticast({
        tokens: tokenList,
        ...message
      });

      return { 
        success: true, 
        sent: response.successCount, 
        failed: response.failureCount 
      };
    } catch (error) {
      console.error('[FCM] Failed to send to user:', error.message);
      return { success: false, reason: error.message };
    }
  }

  /**
   * Remove invalid tokens from database
   */
  async cleanupInvalidTokens(tokens) {
    try {
      await prisma.deviceToken.deleteMany({
        where: { token: { in: tokens } }
      });
      console.log(`[FCM] Cleaned up ${tokens.length} invalid tokens`);
    } catch (error) {
      console.error('[FCM] Failed to cleanup tokens:', error.message);
    }
  }

  /**
   * Get notification icon based on type
   */
  getIconForType(type) {
    switch (type) {
      case 'achievement': return '🏆';
      case 'success': return '✅';
      case 'error': return '❌';
      case 'warning': return '⚠️';
      default: return '📧';
    }
  }
}

module.exports = new FCMService();
