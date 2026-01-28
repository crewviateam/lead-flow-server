// repositories/NotificationRepository.js
// Data access layer for Notifications

const { prisma } = require('../lib/prisma');

class NotificationRepository {

  /**
   * Create a notification
   */
  async create(data) {
    return prisma.notification.create({
      data: {
        type: data.type || 'info',
        message: data.message,
        details: data.details,
        leadId: data.metadata?.leadId ? parseInt(data.metadata.leadId) : null,
        emailJobId: data.metadata?.emailJobId ? parseInt(data.metadata.emailJobId) : null,
        event: data.metadata?.event
      }
    });
  }

  /**
   * Get notifications with pagination
   */
  async findMany({ page = 1, limit = 20, unreadOnly = false }) {
    const where = {};
    if (unreadOnly) where.read = false;

    const [notifications, total, unreadCount] = await Promise.all([
      prisma.notification.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        skip: (page - 1) * limit,
        take: limit,
        include: {
          lead: {
            select: { id: true, name: true, email: true }
          }
        }
      }),
      prisma.notification.count({ where }),
      prisma.notification.count({ where: { read: false } })
    ]);

    return {
      notifications,
      unreadCount,
      pagination: {
        page,
        limit,
        total,
        pages: Math.ceil(total / limit)
      }
    };
  }

  /**
   * Mark notification(s) as read
   */
  async markRead(id = null) {
    if (id) {
      return prisma.notification.update({
        where: { id: parseInt(id) },
        data: { read: true }
      });
    }

    // Mark all as read
    return prisma.notification.updateMany({
      where: { read: false },
      data: { read: true }
    });
  }

  /**
   * Get unread count
   */
  async getUnreadCount() {
    return prisma.notification.count({
      where: { read: false }
    });
  }

  /**
   * Delete old notifications (called by cron)
   */
  async cleanupOld(olderThanDays = 30) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - olderThanDays);

    const result = await prisma.notification.deleteMany({
      where: {
        createdAt: { lt: cutoff }
      }
    });

    return result.count;
  }
}

module.exports = new NotificationRepository();
