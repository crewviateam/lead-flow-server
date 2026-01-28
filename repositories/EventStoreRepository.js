// repositories/EventStoreRepository.js
// Data access layer for EventStore (event sourcing)

const { prisma } = require('../lib/prisma');

class EventStoreRepository {

  /**
   * Store an event
   */
  async create(data) {
    return prisma.eventStore.create({
      data: {
        eventType: data.eventType,
        aggregateId: data.aggregateId?.toString() || 'system',
        aggregateType: data.aggregateType || 'Lead',
        payload: data.payload,
        metadata: data.metadata || {},
        version: data.version || 1
      }
    });
  }

  /**
   * Check if event was already processed (for deduplication)
   */
  async wasProcessed(messageId, eventType) {
    const existing = await prisma.processedEvent.findUnique({
      where: {
        messageId_eventType: {
          messageId,
          eventType
        }
      }
    });
    return !!existing;
  }

  /**
   * Mark event as processed
   */
  async markProcessed(messageId, eventType, eventTimestamp = null) {
    try {
      await prisma.processedEvent.create({
        data: {
          messageId,
          eventType,
          eventTimestamp
        }
      });
      return true;
    } catch (error) {
      // Duplicate - already processed
      if (error.code === 'P2002') {
        return false;
      }
      throw error;
    }
  }

  /**
   * Get events for an aggregate
   */
  async findByAggregate(aggregateId, aggregateType = null) {
    const where = { aggregateId: aggregateId.toString() };
    if (aggregateType) where.aggregateType = aggregateType;

    return prisma.eventStore.findMany({
      where,
      orderBy: { timestamp: 'asc' }
    });
  }

  /**
   * Get events by type
   */
  async findByType(eventType, limit = 100) {
    return prisma.eventStore.findMany({
      where: { eventType },
      orderBy: { timestamp: 'desc' },
      take: limit
    });
  }

  /**
   * Get recent events
   */
  async getRecent(limit = 50) {
    return prisma.eventStore.findMany({
      orderBy: { timestamp: 'desc' },
      take: limit
    });
  }

  /**
   * Count events by type (for analytics)
   */
  async countByType(startDate = null, endDate = null) {
    const where = {};
    if (startDate && endDate) {
      where.timestamp = {
        gte: startDate,
        lte: endDate
      };
    }

    const stats = await prisma.eventStore.groupBy({
      by: ['eventType'],
      where,
      _count: { eventType: true }
    });

    return stats.reduce((acc, stat) => {
      acc[stat.eventType] = stat._count.eventType;
      return acc;
    }, {});
  }

  /**
   * Cleanup old processed events (called by cron)
   */
  async cleanupOldProcessedEvents(olderThanDays = 7) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - olderThanDays);

    const result = await prisma.processedEvent.deleteMany({
      where: {
        processedAt: { lt: cutoff }
      }
    });

    return result.count;
  }
}

module.exports = new EventStoreRepository();
