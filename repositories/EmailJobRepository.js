// repositories/EmailJobRepository.js
// Data access layer for EmailJob operations with Prisma

const { prisma } = require('../lib/prisma');
const RulebookService = require('../services/RulebookService');

class EmailJobRepository {

  /**
   * Find job by ID
   */
  async findById(id, include = {}) {
    return prisma.emailJob.findUnique({
      where: { id: parseInt(id) },
      include: {
        lead: include.lead || false,
        template: include.template || false
      }
    });
  }

  /**
   * Find jobs with pagination and filtering
   */
  async findMany({
    page = 1,
    limit = 20,
    status,
    type,
    view,
    startDate,
    endDate,
    leadId,
    sortBy = 'scheduledFor',
    sortOrder = 'desc'
  }) {
    const where = {};
    
    if (status) where.status = status;
    if (type) where.type = type;
    if (leadId) where.leadId = parseInt(leadId);
    
    // Date range
    if (startDate && endDate) {
      where.scheduledFor = {
        gte: new Date(startDate),
        lte: new Date(endDate)
      };
    }
    
    // View filters
    if (view === 'active') {
      where.status = { in: RulebookService.getPendingOnlyStatuses() };
    } else if (view === 'history') {
      where.status = { in: RulebookService.getCompletedHistoryStatuses() };
    }

    const orderBy = { [sortBy]: sortOrder };

    const [jobs, total] = await Promise.all([
      prisma.emailJob.findMany({
        where,
        orderBy,
        skip: (page - 1) * limit,
        take: limit,
        include: {
          lead: {
            select: { id: true, name: true, email: true, country: true, city: true, timezone: true }
          }
        }
      }),
      prisma.emailJob.count({ where })
    ]);

    return {
      jobs,
      pagination: {
        page,
        limit,
        total,
        pages: Math.ceil(total / limit)
      }
    };
  }

  /**
   * Create a new email job
   */
  async create(data) {
    let category = data.category || 'followup';
    if (!data.category && data.type) {
      const t = data.type.toLowerCase();
      if (t.includes('initial')) category = 'initial';
      else if (t.includes('conditional')) category = 'conditional';
      else if (t.includes('manual')) category = 'manual';
    }
    return prisma.emailJob.create({
      data: {
        category,
        leadId: parseInt(data.leadId),
        email: data.email,
        type: data.type,
        scheduledFor: data.scheduledFor,
        status: data.status || 'pending',
        templateId: data.templateId ? parseInt(data.templateId) : null,
        idempotencyKey: data.idempotencyKey,
        retryCount: data.retryCount || 0,
        queueName: data.queueName || 'emailSendQueue',
        condition: data.condition || null,
        metadata: data.metadata || {}
      }
    });
  }

  /**
   * Update a job
   */
  async update(id, data) {
    return prisma.emailJob.update({
      where: { id: parseInt(id) },
      data: {
        ...data,
        updatedAt: new Date()
      }
    });
  }

  /**
   * Delete a job
   */
  async delete(id) {
    return prisma.emailJob.delete({
      where: { id: parseInt(id) }
    });
  }

  /**
   * Find pending jobs for a lead
   */
  async findPendingByLead(leadId) {
    return prisma.emailJob.findFirst({
      where: {
        leadId: parseInt(leadId),
        status: { in: RulebookService.getPendingOnlyStatuses() }
      },
      orderBy: { scheduledFor: 'asc' }
    });
  }

  /**
   * Find all jobs for a lead
   */
  async findByLeadId(leadId) {
    return prisma.emailJob.findMany({
      where: {
        leadId: parseInt(leadId)
      },
      orderBy: { scheduledFor: 'asc' }
    });
  }

  /**
   * Find jobs by status
   */
  async findByStatus(status, limit = 100) {
    return prisma.emailJob.findMany({
      where: { status },
      orderBy: { scheduledFor: 'asc' },
      take: limit,
      include: {
        lead: true
      }
    });
  }

  /**
   * Cancel jobs for a lead
   */
  async cancelByLead(leadId, reason = 'User action') {
    return prisma.emailJob.updateMany({
      where: {
        leadId: parseInt(leadId),
        status: { in: RulebookService.getCancellableStatuses() }
      },
      data: {
        status: 'cancelled',
        lastError: `Cancelled: ${reason}`,
        cancellationReason: reason
      }
    });
  }

  /**
   * Get jobs scheduled for a specific date range (for Schedule page)
   */
  async findByDateRange(startDate, endDate, excludeCancelled = true) {
    const where = {
      scheduledFor: {
        gte: startDate,
        lt: endDate
      }
    };
    
    if (excludeCancelled) {
      where.status = { not: 'cancelled' };
    }

    return prisma.emailJob.findMany({
      where,
      orderBy: { scheduledFor: 'asc' },
      include: {
        lead: {
          select: { id: true, name: true, timezone: true, country: true }
        }
      }
    });
  }

  /**
   * Get job statistics for analytics
   */
  async getStats() {
    const stats = await prisma.emailJob.groupBy({
      by: ['status'],
      _count: { status: true }
    });

    return stats.reduce((acc, stat) => {
      acc[stat.status] = stat._count.status;
      return acc;
    }, {});
  }

  /**
   * Count jobs with specific criteria
   */
  async count(where = {}) {
    return prisma.emailJob.count({ where });
  }

  /**
   * Count sent jobs (unique, excluding rescheduled)
   */
  async countUniqueSent(startDate = null, endDate = null) {
    const where = {
      sentAt: { not: null },
      status: { not: 'rescheduled' }
    };
    
    if (startDate && endDate) {
      where.sentAt = {
        gte: startDate,
        lte: endDate,
        not: null
      };
    }
    
    return prisma.emailJob.count({ where });
  }

  /**
   * Aggregate stats for analytics
   */
  async getAnalyticsAggregation(startDate, endDate) {
    const where = {
      sentAt: {
        gte: startDate,
        lte: endDate
      },
      status: { not: 'rescheduled' }
    };

    const [sent, delivered, opened, clicked, failed] = await Promise.all([
      prisma.emailJob.count({ where: { ...where, sentAt: { not: null } } }),
      prisma.emailJob.count({ where: { ...where, deliveredAt: { not: null } } }),
      prisma.emailJob.count({ where: { ...where, openedAt: { not: null } } }),
      prisma.emailJob.count({ where: { ...where, clickedAt: { not: null } } }),
      prisma.emailJob.count({ 
        where: { 
          ...where, 
          status: { in: RulebookService.getHardFailureStatuses() } 
        } 
      })
    ]);

    return { sent, delivered, opened, clicked, failed };
  }

  /**
   * Get hierarchical analytics by type
   */
  async getHierarchicalStats(startDate, endDate) {
    // Raw query for complex aggregation
    const result = await prisma.$queryRaw`
      SELECT 
        CASE 
          WHEN LOWER(type) LIKE '%initial%' THEN 'Initial'
          WHEN (metadata->>'manual')::boolean = true THEN 'Manual'
          ELSE 'Followup'
        END as email_type,
        COUNT(*) as sent,
        COUNT(CASE WHEN delivered_at IS NOT NULL THEN 1 END) as delivered,
        COUNT(CASE WHEN opened_at IS NOT NULL THEN 1 END) as opened,
        COUNT(CASE WHEN clicked_at IS NOT NULL THEN 1 END) as clicked,
        COUNT(CASE WHEN status = 'soft_bounce' THEN 1 END) as soft_bounce,
        COUNT(CASE WHEN status = 'deferred' THEN 1 END) as deferred,
        COUNT(CASE WHEN status = 'hard_bounce' THEN 1 END) as hard_bounce,
        COUNT(CASE WHEN status = 'blocked' THEN 1 END) as blocked,
        COUNT(CASE WHEN status = 'spam' THEN 1 END) as spam,
        COUNT(CASE WHEN status IN ('failed', 'hard_bounce', 'blocked', 'spam') THEN 1 END) as failed
      FROM email_jobs
      WHERE sent_at BETWEEN ${startDate} AND ${endDate}
        AND status != 'rescheduled'
      GROUP BY email_type
    `;

    return result;
  }

  /**
   * Find recent activity for notifications
   */
  async getRecentActivity(limit = 10) {
    return prisma.emailJob.findMany({
      where: {
        status: { in: RulebookService.getProcessedStatuses() }
      },
      orderBy: { updatedAt: 'desc' },
      take: limit,
      include: {
        lead: {
          select: { email: true, name: true }
        }
      }
    });
  }
}

module.exports = new EmailJobRepository();
