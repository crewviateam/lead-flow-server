const NotificationService = require('../services/NotificationService');

class NotificationController {
  
  async getNotifications(req, res) {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 20;
      const unreadOnly = req.query.unread === 'true';

      const result = await NotificationService.getNotifications(page, limit, unreadOnly);
      res.json(result);
    } catch (error) {
      console.error('Get notifications error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  async markRead(req, res) {
    try {
      const { id } = req.body; // If null/undefined, marks all
      await NotificationService.markRead(id);
      res.json({ success: true });
    } catch (error) {
      console.error('Mark read error:', error);
      res.status(500).json({ error: error.message });
    }
  }
}

module.exports = new NotificationController();
