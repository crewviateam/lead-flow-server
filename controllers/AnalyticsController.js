// controllers/AnalyticsController.js
// Analytics controller using Prisma

const AnalyticsService = require('../services/AnalyticsService');
const AnalyticsPollingService = require('../services/AnalyticsPollingService');
const moment = require('moment');
const { prisma } = require('../lib/prisma');
const { LeadRepository } = require('../repositories');
const RulebookService = require('../services/RulebookService');

const parsePeriod = (period) => {
  if (!period) return null;
  
  const match = period.match(/^(\d+)([hd])$/);
  if (!match) return null;
  
  const value = parseInt(match[1]);
  const unit = match[2] === 'h' ? 'hours' : 'days';
  
  return {
    start: moment().subtract(value, unit).toDate(),
    end: moment().toDate()
  };
};

class AnalyticsController {
  async getSummary(req, res) {
    try {
      const { startDate, endDate, period } = req.query;

      let start, end;
      const periodRange = parsePeriod(period);
      
      if (periodRange) {
        start = periodRange.start;
        end = periodRange.end;
      } else {
        start = startDate ? moment(startDate).startOf('day').toDate() : moment().subtract(30, 'days').startOf('day').toDate();
        end = endDate ? moment(endDate).endOf('day').toDate() : moment().endOf('day').toDate();
      }

      const summary = await AnalyticsService.getSummary(start, end);

      res.status(200).json({
        period: { start, end },
        summary: {
          ...summary,
          clickRate: summary.emailsSent > 0 ? ((summary.emailsClicked / summary.emailsSent) * 100).toFixed(1) : 0
        }
      });
    } catch (error) {
      console.error('Get summary error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  async getDashboardStats(req, res) {
    try {
      const { startDate, endDate, period } = req.query;
      let start, end;
      const periodRange = parsePeriod(period);
      
      if (periodRange) {
        start = periodRange.start;
        end = periodRange.end;
      } else {
        start = startDate ? moment(startDate).startOf('day').toDate() : null;
        end = endDate ? moment(endDate).endOf('day').toDate() : null;
      }

      const stats = await AnalyticsService.getDashboardData(start, end);
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async getEmailJobStats(req, res) {
    try {
      // Aggregation using Prisma
      const [statusStats, sentStats, rescheduledStats] = await Promise.all([
        // 1. Status counts
        prisma.emailJob.groupBy({
          by: ['status'],
          _count: { status: true }
        }),
        
        // 2. Unique sent count (jobs with sentAt, excluding rescheduled)
        prisma.emailJob.count({ 
          where: { 
            sentAt: { not: null },
            NOT: { status: 'rescheduled' }
          }
        }),
        
        // 3. Rescheduled count
        prisma.emailJob.count({
          where: {
            OR: [
              { status: 'rescheduled' },
              { retryCount: { gt: 0 } }
            ]
          }
        })
      ]);

      // Build status counts map
      const statusCounts = {};
      statusStats.forEach(stat => {
        statusCounts[stat.status] = stat._count.status;
      });

      const buckets = {
        pending: (statusCounts.pending || 0) + (statusCounts.queued || 0),
        success: sentStats,
        failed: (statusCounts.hard_bounce || 0) + 
                (statusCounts.blocked || 0) + 
                (statusCounts.spam || 0),
        rescheduled: rescheduledStats
      };

      const total = statusStats.reduce((sum, stat) => sum + stat._count.status, 0);

      res.status(200).json({
        emailJobs: statusCounts,
        buckets,
        total
      });
    } catch (error) {
      console.error('Get email job stats error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  async getLeadStats(req, res) {
    try {
      const stats = await prisma.lead.groupBy({
        by: ['status'],
        _count: { status: true }
      });

      const statusCounts = {};
      stats.forEach(stat => {
        statusCounts[stat.status] = stat._count.status;
      });

      res.status(200).json({
        leads: statusCounts,
        total: stats.reduce((sum, stat) => sum + stat._count.status, 0)
      });
    } catch (error) {
      console.error('Get lead stats error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  async syncFromBrevo(req, res) {
    try {
      console.log('📊 Manual analytics sync triggered');
      
      if (req.query.rebuild === 'true') {
        const result = await AnalyticsPollingService.rebuildAnalyticsFromJobs();
        return res.status(200).json({
          message: 'Analytics fully rebuilt from job history',
          stats: result
        });
      }

      const result = await AnalyticsPollingService.pollBrevoEvents();
      const freshStats = await AnalyticsPollingService.rebuildAnalyticsFromJobs();
      
      res.status(200).json({
        message: 'Analytics sync completed',
        pollResult: result,
        currentStats: freshStats
      });
    } catch (error) {
      console.error('Sync from Brevo error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  async getDetailedBreakdown(req, res) {
    try {
      const { period, startDate, endDate } = req.query;
      
      let start, end;
      const periodRange = parsePeriod(period);
      
      if (periodRange) {
        start = periodRange.start;
        end = periodRange.end;
      } else {
        start = startDate ? moment(startDate).startOf('day').toDate() : moment().startOf('day').toDate();
        end = endDate ? moment(endDate).endOf('day').toDate() : moment().endOf('day').toDate();
      }
      
      // Use unified analytics for consistent counting across all pages
      const unified = await AnalyticsService.getUnifiedAnalytics(start, end);
      const t = unified.totals;
      
      res.status(200).json({
        date: start,
        breakdown: {
          sent: t.sent,
          delivered: t.delivered,
          opened: t.opened,
          clicked: t.clicked,
          softBounced: t.softBounce,
          hardBounced: t.hardBounce,
          deferred: t.deferred,
          blocked: t.blocked,
          spam: t.spam,
          failed: t.hardBounce + t.blocked + t.spam,  // DON'T add t.failed - it overlaps with blocked/hardBounce/spam (jobs with failed_at also have these statuses)
          totalBounced: t.softBounce + t.hardBounce,
          pending: t.pending
        },
        failedData: {
          total: t.hardBounce + t.blocked + t.spam,  // DON'T add t.failed - it overlaps with these statuses
          blocked: t.blocked,
          hardBounce: t.hardBounce
        },
        rescheduledData: {
          total: t.rescheduled,
          softBounce: t.softBounce,
          deferred: t.deferred
        },
        rates: unified.rates
      });
    } catch (error) {
      console.error('Get detailed breakdown error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  async getHierarchicalAnalytics(req, res) {
    try {
      const { period, startDate, endDate } = req.query;
      
      let start, end;
      const periodRange = parsePeriod(period);
      
      if (periodRange) {
        start = periodRange.start;
        end = periodRange.end;
      } else {
        start = startDate ? moment(startDate).startOf('day').toDate() : moment().subtract(7, 'days').startOf('day').toDate();
        end = endDate ? moment(endDate).endOf('day').toDate() : moment().endOf('day').toDate();
      }

      // Use unified analytics for consistency across all pages
      const unified = await AnalyticsService.getUnifiedAnalytics(start, end);
      const t = unified.totals;
      const byType = unified.byType;

      const calcPercent = (value, total) => total > 0 ? ((value / total) * 100).toFixed(1) : '0.0';

      // Build hierarchy structure for UI
      const hierarchy = {
        totalSent: {
          count: t.sent,
          children: {
            initial: { count: byType.Initial.sent, percent: calcPercent(byType.Initial.sent, t.sent), label: 'Initial Emails' },
            followup: { count: byType.Followup.sent, percent: calcPercent(byType.Followup.sent, t.sent), label: 'Follow-up Emails' },
            manual: { count: byType.Manual.sent, percent: calcPercent(byType.Manual.sent, t.sent), label: 'Manual Emails' },
            conditional: { count: byType.Conditional?.sent || 0, percent: calcPercent(byType.Conditional?.sent || 0, t.sent), label: 'Conditional Emails' }
          }
        },
        delivered: {
          count: t.delivered,
          percent: calcPercent(t.delivered, t.sent),
          children: {
            opened: { count: t.opened, percent: calcPercent(t.opened, t.delivered), label: 'Opened' },
            clicked: { count: t.clicked, percent: calcPercent(t.clicked, t.delivered), label: 'Clicked' }
          }
        },
        pending: {
          count: t.pending,
          percent: calcPercent(t.pending, t.sent),
          label: 'Pending'
        },
        rescheduled: {
          count: t.rescheduled,
          percent: calcPercent(t.rescheduled, t.sent),
          children: {
            softBounce: { count: t.softBounce, percent: calcPercent(t.softBounce, t.rescheduled), label: 'Soft Bounce' },
            deferred: { count: t.deferred, percent: calcPercent(t.deferred, t.rescheduled), label: 'Deferred' }
          }
        },
        failed: {
          count: t.hardBounce + t.blocked + t.spam,
          percent: calcPercent(t.hardBounce + t.blocked + t.spam, t.sent),
          children: {
            hardBounce: { count: t.hardBounce, percent: calcPercent(t.hardBounce, t.failed), label: 'Hard Bounce' },
            blocked: { count: t.blocked, percent: calcPercent(t.blocked, t.failed), label: 'Blocked' },
            spam: { count: t.spam, percent: calcPercent(t.spam, t.failed), label: 'Spam' }
          }
        }
      };
console.log("byType====", byType);

      const formattedByType = {
        Initial: { ...byType.Initial, pendingReschedule: byType.Initial.rescheduled || 0 },
        Followup: { ...byType.Followup, pendingReschedule: byType.Followup.rescheduled || 0 },
        Manual: { ...byType.Manual, pendingReschedule: byType.Manual.rescheduled || 0 },
        Conditional: { 
          ...(byType.Conditional || { sent: 0, delivered: 0, opened: 0, clicked: 0, rescheduled: 0, failed: 0, softBounce: 0, hardBounce: 0, blocked: 0 }), 
          pendingReschedule: byType.Conditional?.rescheduled || 0 
        }
      };

      res.status(200).json({
        period: { start, end },
        totals: {
          sent: t.sent,
          delivered: t.delivered,
          opened: t.opened,
          clicked: t.clicked,
          softBounce: t.softBounce,
          deferred: t.deferred,
          hardBounce: t.hardBounce,
          blocked: t.blocked,
          spam: t.spam,
          failed: t.hardBounce + t.blocked + t.spam,
          pending: t.pending,
          pendingReschedule: t.rescheduled
        },
        byType: formattedByType,
        hierarchy,
        rates: unified.rates
      });
    } catch (error) {
      console.error('Get hierarchical analytics error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  async getRecentActivity(req, res) {
    try {
      const limit = parseInt(req.query.limit) || 10;
      
      const recentJobs = await prisma.emailJob.findMany({
        where: {
          status: { in: RulebookService.getAllExceptCancelledSkippedStatuses() }
        },
        orderBy: { updatedAt: 'desc' },
        take: limit,
        include: {
          lead: {
            select: { email: true, name: true }
          }
        }
      });
      
      const activities = recentJobs.map(job => ({
        id: job.id,
        event: job.status,
        leadEmail: job.lead?.email || job.email,
        leadName: job.lead?.name || 'Unknown',
        emailType: job.type,
        timestamp: job.updatedAt || job.sentAt || job.createdAt
      }));
      
      res.status(200).json({ activities });
    } catch (error) {
      console.error('Get recent activity error:', error);
      res.status(500).json({ error: error.message });
    }
  }
}

module.exports = new AnalyticsController();
