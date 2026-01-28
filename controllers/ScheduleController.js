// controllers/ScheduleController.js
// Schedule controller using Prisma

const moment = require('moment-timezone');
const { prisma } = require('../lib/prisma');
const { SettingsRepository } = require('../repositories');
const RateLimitService = require('../services/RateLimitService');
const RulebookService = require('../services/RulebookService');

class ScheduleController {
  async getSchedule(req, res) {
    try {
      const { date, timezone = 'UTC' } = req.query;
      const queryDate = date ? moment(date) : moment();
      
      const settings = await SettingsRepository.getSettings();
      const { rateLimit, businessHours } = settings;
      
      const windowMinutes = rateLimit?.windowMinutes || 15;
      const maxEmails = rateLimit?.emailsPerWindow || 2;
      const startHour = businessHours?.startHour || 8;
      const endHour = businessHours?.endHour || 22;

      // Global View: Always use IST
      const viewTimezone = 'Asia/Kolkata';
      const startOfDay = moment.tz(queryDate.format('YYYY-MM-DD'), viewTimezone).startOf('day');
      const endOfDay = moment(startOfDay).endOf('day');

      // 1. Generate ALL 24h slots for the day
      const slots = [];
      let current = moment(startOfDay);
      
      while (current.isBefore(endOfDay)) {
        slots.push({
          startTime: current.toDate(),
          label: current.format('HH:mm'),
          istLabel: current.format('HH:mm'),
          windowStart: current.valueOf(),
          windowEnd: current.clone().add(windowMinutes, 'minutes').valueOf(),
          used: 0,
          max: maxEmails,
          jobs: []
        });
        current.add(windowMinutes, 'minutes');
      }

      // 2. Fetch jobs for this day - include failed emails for day summary visibility
      // Note: Failed emails are included but don't count toward unique journey stats
      const jobs = await prisma.emailJob.findMany({
        where: {
          scheduledFor: {
            gte: startOfDay.toDate(),
            lt: endOfDay.toDate()
          },
          status: { 
            in: [
              'pending', 'queued', 'scheduled', 'sent', 'delivered', 'opened', 'clicked',
              'failed', 'hard_bounce', 'soft_bounce', 'blocked', 'spam', 'deferred'
            ] 
          }
        },
        select: {
          id: true,
          email: true,
          type: true,
          scheduledFor: true,
          status: true,
          leadId: true,
          metadata: true,
          lastError: true,
          lead: {
            select: { name: true, timezone: true, country: true }
          }
        }
      });

      // 3. Map jobs to slots (using async for conditional email lookups)
      const failedStatuses = ['failed', 'hard_bounce', 'blocked', 'spam'];
      
      for (const job of jobs) {
        const jobTime = moment(job.scheduledFor).valueOf();
        
        const slot = slots.find(s => jobTime >= s.windowStart && jobTime < s.windowEnd);
        if (slot) {
          // Only count non-failed jobs toward slot usage
          const isFailed = failedStatuses.includes(job.status);
          if (!isFailed) {
            slot.used++;
          }
          
          // Calculate Lead's Local Time for display
          let leadTimeStr = 'Unknown Time';
          let leadTz = job.lead?.timezone || job.metadata?.timezone;
          
          if (leadTz) {
            leadTimeStr = moment(job.scheduledFor).tz(leadTz).format('HH:mm z');
          }

          // Get display-friendly type for conditional emails using async lookup
          let displayType = RulebookService.getSimplifiedTypeName(job.type, job.metadata);
          
          // For conditional emails, try to get proper display with triggerEvent
          if (job.type?.startsWith('conditional:')) {
            const displayStatus = await RulebookService.formatJobStatusForDisplayAsync(job);
            // Extract type from displayStatus (format: "condition opened:pending" -> "condition opened")
            displayType = displayStatus.split(':')[0];
          }

          slot.jobs.push({
            id: job.id,
            email: job.email,
            name: job.lead?.name || 'Unknown',
            type: job.type,
            displayType,  // e.g., "condition opened" or "First Followup"
            status: job.status,
            isFailed: isFailed,
            failureReason: isFailed ? job.lastError : null,
            isManual: job.metadata?.manual || false,
            time: leadTimeStr,
            country: job.lead?.country,
            timezone: leadTz
          });
        }
      }
      
      // Determine status color/state
      slots.forEach(slot => {
        if (slot.used === 0) slot.status = 'empty';
        else if (slot.used < slot.max) slot.status = 'partial';
        else slot.status = 'full';
        
        delete slot.windowStart;
        delete slot.windowEnd;
      });

      // Check if date is paused
      const dateStr = startOfDay.format('YYYY-MM-DD');
      const pausedDates = settings.pausedDates || [];
      const isPaused = pausedDates.some(pd => 
        moment(pd).format('YYYY-MM-DD') === dateStr
      );
      
      const pausedDatesFormatted = pausedDates.map(pd => moment(pd).format('YYYY-MM-DD'));

      res.status(200).json({
        date: startOfDay.format('YYYY-MM-DD'),
        timezone: viewTimezone,
        isPaused,
        pausedDates: pausedDatesFormatted,
        settings: {
          maxPerWindow: maxEmails,
          windowMinutes,
          businessHours: '24 Hours (Global View)'
        },
        slots
      });

    } catch (error) {
      console.error('Get schedule error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Get available timezones based on existing jobs
  async getTimezones(req, res) {
    try {
      // Get distinct timezones from job metadata using raw query
      const result = await prisma.$queryRaw`
        SELECT DISTINCT metadata->>'timezone' as timezone 
        FROM email_jobs 
        WHERE metadata->>'timezone' IS NOT NULL
      `;
      
      const timezones = result.map(r => r.timezone).filter(Boolean).sort();
      res.status(200).json({ timezones });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }
}

module.exports = new ScheduleController();
