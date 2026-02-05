// routes/notificationRoutes.js
// Notification and push notification routes

const express = require('express');
const router = express.Router();
const NotificationController = require('../controllers/NotificationController');
const NotificationService = require('../services/NotificationService');

// Notifications
router.get('/', NotificationController.getNotifications.bind(NotificationController));
router.put('/read', NotificationController.markRead.bind(NotificationController));

// Device tokens (FCM Push)
router.post('/device-tokens', async (req, res) => {
  try {
    const { userId = 'admin', token, platform = 'web' } = req.body;
    if (!token) {
      return res.status(400).json({ error: 'Token is required' });
    }
    const result = await NotificationService.registerDeviceToken(userId, token, platform);
    if (result === true) {
      res.status(200).json({ success: true, message: 'Device registered for push notifications' });
    } else {
      res.status(200).json({ success: false, ...result });
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

router.delete('/device-tokens/:token', async (req, res) => {
  try {
    const { token } = req.params;
    const result = await NotificationService.unregisterDeviceToken(token);
    res.status(200).json({ success: true, removed: result });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
