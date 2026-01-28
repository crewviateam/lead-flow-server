// controllers/SettingsController.js
// Settings controller using Prisma

const { SettingsRepository } = require('../repositories');
const { prisma } = require('../lib/prisma');
const axios = require('axios');
const RulebookService = require('../services/RulebookService');
require('dotenv').config();

class SettingsController {
  // Get current settings
  async getSettings(req, res) {
    try {
      const settings = await SettingsRepository.getSettings();
      res.status(200).json(settings);
    } catch (error) {
      console.error('Get settings error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Update settings
  async updateSettings(req, res) {
    try {
      const { rateLimit, businessHours, retry, brevo, smartSendTime, reporting } = req.body;
      
      const updates = {};
      if (rateLimit) updates.rateLimit = rateLimit;
      if (businessHours) updates.businessHours = businessHours;
      if (retry) updates.retry = retry;
      if (brevo) updates.brevo = brevo;
      if (smartSendTime) updates.smartSendTime = smartSendTime;
      if (reporting) updates.reporting = reporting;
      
      const settings = await SettingsRepository.updateSettings(updates);
      res.status(200).json({
        message: 'Settings updated successfully',
        settings
      });
    } catch (error) {
      console.error('Update settings error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Get followups
  async getFollowups(req, res) {
    try {
      const settings = await SettingsRepository.getSettings();
      res.status(200).json(settings.followups || []);
    } catch (error) {
      console.error('Get followups error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Add a new followup
  async addFollowup(req, res) {
    try {
      const { name, delayDays, enabled = true } = req.body;
      
      if (!name || delayDays === undefined) {
        return res.status(400).json({ error: 'Name and delayDays are required' });
      }

      const settings = await SettingsRepository.getSettings();
      const followups = settings.followups || [];
      const maxOrder = followups.reduce((max, f) => Math.max(max, f.order || 0), 0);
      
      followups.push({
        name,
        delayDays: parseInt(delayDays),
        enabled,
        order: maxOrder + 1
      });
      
      const updated = await SettingsRepository.updateSettings({ followups });
      
      // Auto-schedule this new followup for leads that have completed their sequence
      const EmailSchedulerService = require('../services/EmailSchedulerService');
      const scheduleResult = await EmailSchedulerService.scheduleNewFollowupForCompletedLeads(name);
      
      res.status(201).json({
        message: `Followup added successfully. Scheduled for ${scheduleResult.scheduled} completed leads.`,
        followups: updated.followups,
        autoScheduled: scheduleResult
      });
    } catch (error) {
      console.error('Add followup error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Update a followup
  async updateFollowup(req, res) {
    try {
      const { id } = req.params;
      const { name, delayDays, enabled, order, templateId, condition } = req.body;

      const settings = await SettingsRepository.getSettings();
      const followups = settings.followups || [];
      
      // Find by id (string like 'followup_xxx'), name, or order
      const followupIndex = followups.findIndex(f => 
        f.id === id || 
        f.name === id || 
        f.order === parseInt(id) ||
        (f.id && f.id.toString() === id)
      );
      
      if (followupIndex === -1) {
        return res.status(404).json({ error: 'Followup not found' });
      }

      const followup = followups[followupIndex];
      if (name !== undefined) followup.name = name;
      if (delayDays !== undefined) followup.delayDays = parseInt(delayDays);
      if (enabled !== undefined) followup.enabled = enabled;
      if (order !== undefined) followup.order = parseInt(order);
      if (templateId !== undefined) followup.templateId = templateId || null;
      
      if (condition !== undefined) {
        if (!followup.condition) followup.condition = {};
        if (condition.type !== undefined) followup.condition.type = condition.type;
        if (condition.checkStep !== undefined) followup.condition.checkStep = condition.checkStep;
        if (condition.alternativeTemplateId !== undefined) {
          followup.condition.alternativeTemplateId = condition.alternativeTemplateId || null;
        }
        if (condition.skipIfNotMet !== undefined) followup.condition.skipIfNotMet = condition.skipIfNotMet;
      }
      
      followups[followupIndex] = followup;
      const updated = await SettingsRepository.updateSettings({ followups });
      
      res.status(200).json({
        message: 'Followup updated successfully',
        followups: updated.followups
      });
    } catch (error) {
      console.error('Update followup error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Delete a followup
  async deleteFollowup(req, res) {
    try {
      const { id } = req.params;

      const settings = await SettingsRepository.getSettings();
      const followups = settings.followups || [];
      
      // Find by id (string like 'followup_xxx'), name, or order
      const followupIndex = followups.findIndex(f => 
        f.id === id || 
        f.name === id || 
        f.order === parseInt(id) ||
        (f.id && f.id.toString() === id)
      );
      
      if (followupIndex === -1) {
        return res.status(404).json({ error: 'Followup not found' });
      }

      if (followups[followupIndex].order === 0) {
        return res.status(400).json({ error: 'Cannot delete the initial email' });
      }

      followups.splice(followupIndex, 1);
      const updated = await SettingsRepository.updateSettings({ followups });
      
      res.status(200).json({
        message: 'Followup deleted successfully',
        followups: updated.followups
      });
    } catch (error) {
      console.error('Delete followup error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Reorder followups
  async reorderFollowups(req, res) {
    try {
      const { followupIds } = req.body;

      const settings = await SettingsRepository.getSettings();
      const followups = settings.followups || [];
      
      followupIds.forEach((id, index) => {
        const followup = followups.find(f => f.id === parseInt(id) || f.order === parseInt(id));
        if (followup) {
          followup.order = index;
        }
      });
      
      followups.sort((a, b) => a.order - b.order);
      const updated = await SettingsRepository.updateSettings({ followups });
      
      res.status(200).json({
        message: 'Followups reordered successfully',
        followups: updated.followups
      });
    } catch (error) {
      console.error('Reorder followups error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Clear all logs from Brevo
  async clearBrevoLogs(req, res) {
    console.log('🧹 Clearing all Brevo logs...');
    try {
      const settings = await SettingsRepository.getSettings();
      const apiKey = settings.brevo?.apiKey || process.env.BREVO_API_KEY;
      
      if (!apiKey) {
        return res.status(500).json({ error: 'Brevo API key not configured' });
      }

      let deletedCount = 0;
      let hasMore = true;
      let loopCount = 0;

      while (hasMore && loopCount < 50) { 
        loopCount++;
        
        const response = await axios.get('https://api.brevo.com/v3/smtp/statistics/emails', {
          params: { limit: 100, startDate: '2020-01-01', endDate: new Date().toISOString().split('T')[0] },
          headers: { 'api-key': apiKey }
        });

        const logs = response.data.emails;
        if (!logs || logs.length === 0) {
          hasMore = false;
          break;
        }

        console.log(`Found ${logs.length} logs to delete (Batch ${loopCount})`);

        const deletePromises = logs.map(log => 
          axios.delete(`https://api.brevo.com/v3/smtp/log/${log.messageId}`, {
            headers: { 'api-key': apiKey }
          }).then(() => ({ id: log.messageId, status: 'deleted' }))
            .catch(err => ({ id: log.messageId, status: 'failed', error: err.message }))
        );

        const results = await Promise.all(deletePromises);
        const batchDeleted = results.filter(r => r.status === 'deleted').length;
        deletedCount += batchDeleted;
        
        console.log(`Batch ${loopCount}: Deleted ${batchDeleted}/${logs.length}`);

        if (logs.length < 100) hasMore = false;
        await new Promise(resolve => setTimeout(resolve, 500));
      }

      res.status(200).json({ 
        message: 'Logs clearing completed', 
        deletedCount,
        batchesProcessed: loopCount 
      });
    } catch (error) {
      console.error('Clear logs error:', error.response?.data || error.message);
      res.status(500).json({ error: error.message });
    }
  }

  // Get paused dates
  async getPausedDates(req, res) {
    try {
      const settings = await SettingsRepository.getSettings();
      res.status(200).json({ 
        pausedDates: settings.pausedDates || [],
        weekendDays: settings.businessHours?.weekendDays || [0, 6]
      });
    } catch (error) {
      console.error('Get paused dates error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Pause a specific date
  async pauseDate(req, res) {
    try {
      const { date } = req.body;
      
      if (!date) {
        return res.status(400).json({ error: 'Date is required' });
      }

      const pauseDate = new Date(date);
      pauseDate.setHours(0, 0, 0, 0);
      
      const settings = await SettingsRepository.getSettings();
      const pausedDates = settings.pausedDates || [];
      
      const alreadyPaused = pausedDates.some(pd => 
        new Date(pd).toDateString() === pauseDate.toDateString()
      );
      
      if (alreadyPaused) {
        return res.status(400).json({ error: 'Date is already paused' });
      }
      
      pausedDates.push(pauseDate);
      const updated = await SettingsRepository.updateSettings({ pausedDates });
      
      // CRITICAL: Move all jobs scheduled for this paused date to next working day
      const { prisma } = require('../lib/prisma');
      const moment = require('moment');
      const EmailSchedulerService = require('../services/EmailSchedulerService');
      
      // Find jobs scheduled for the paused date
      const startOfDay = moment(pauseDate).startOf('day').toDate();
      const endOfDay = moment(pauseDate).endOf('day').toDate();
      
      console.log(`[PauseDate] Looking for jobs between ${startOfDay} and ${endOfDay}`);
      
      const jobsToReschedule = await prisma.emailJob.findMany({
        where: {
          scheduledFor: { gte: startOfDay, lte: endOfDay },
          status: { in: RulebookService.getActiveStatuses() }
        },
        include: { lead: true }
      });
      
      console.log(`[PauseDate] Found ${jobsToReschedule.length} jobs to move`);
      
      let rescheduledCount = 0;
      let failedCount = 0;
      
      for (const job of jobsToReschedule) {
        try {
          // Use the moveJobToNextWorkingDay method which handles everything
          const newJob = await EmailSchedulerService.moveJobToNextWorkingDay(
            job.id, 
            `Paused date: ${moment(pauseDate).format('YYYY-MM-DD')}`
          );
          
          if (newJob) {
            rescheduledCount++;
            console.log(`[PauseDate] ✓ Moved job ${job.id} -> ${newJob.id} (${job.email})`);
          } else {
            console.log(`[PauseDate] Job ${job.id} was not moved (status: ${job.status})`);
          }
        } catch (err) {
          failedCount++;
          console.error(`[PauseDate] ✗ Failed to move job ${job.id}:`, err.message);
          
          // If move fails, just cancel the old job so it doesn't send on paused date
          try {
            await prisma.emailJob.update({
              where: { id: job.id },
              data: {
                status: 'cancelled',
                lastError: `Paused date - reschedule failed: ${err.message}`
              }
            });
          } catch (e) {
            console.error(`[PauseDate] Could not cancel job ${job.id}:`, e.message);
          }
        }
      }
      
      console.log(`[PauseDate] Paused ${pauseDate.toDateString()}: moved ${rescheduledCount}, failed ${failedCount}, total ${jobsToReschedule.length}`);
      
      res.status(200).json({
        message: `Date ${pauseDate.toDateString()} paused successfully`,
        pausedDates: updated.pausedDates,
        rescheduledJobs: rescheduledCount,
        failedJobs: failedCount,
        totalJobsOnDate: jobsToReschedule.length
      });
    } catch (error) {
      console.error('Pause date error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Unpause a specific date
  async unpauseDate(req, res) {
    try {
      const { date } = req.body;
      
      if (!date) {
        return res.status(400).json({ error: 'Date is required' });
      }

      const unpauseDate = new Date(date);
      unpauseDate.setHours(0, 0, 0, 0);
      
      const settings = await SettingsRepository.getSettings();
      const pausedDates = settings.pausedDates || [];
      
      const initialLength = pausedDates.length;
      const filteredDates = pausedDates.filter(pd => 
        new Date(pd).toDateString() !== unpauseDate.toDateString()
      );
      
      if (filteredDates.length === initialLength) {
        return res.status(404).json({ error: 'Date was not paused' });
      }
      
      const updated = await SettingsRepository.updateSettings({ pausedDates: filteredDates });
      
      res.status(200).json({
        message: `Date ${unpauseDate.toDateString()} unpaused successfully`,
        pausedDates: updated.pausedDates
      });
    } catch (error) {
      console.error('Unpause date error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Update weekend days
  async updateWeekendDays(req, res) {
    try {
      const { weekendDays } = req.body;
      
      if (!Array.isArray(weekendDays)) {
        return res.status(400).json({ error: 'weekendDays must be an array' });
      }
      
      if (weekendDays.length >= 7) {
        return res.status(400).json({ error: 'Cannot mark all days as weekends' });
      }
      
      if (!weekendDays.every(d => d >= 0 && d <= 6)) {
        return res.status(400).json({ error: 'Weekend days must be between 0 and 6' });
      }
      
      const settings = await SettingsRepository.getSettings();
      const businessHours = settings.businessHours || {};
      businessHours.weekendDays = weekendDays;
      
      const updated = await SettingsRepository.updateSettings({ businessHours });
      
      res.status(200).json({
        message: 'Weekend days updated successfully',
        weekendDays: updated.businessHours?.weekendDays
      });
    } catch (error) {
      console.error('Update weekend days error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Reschedule all emails on a paused date
  async reschedulePausedEmails(req, res) {
    try {
      const { date } = req.body;
      
      if (!date) {
        return res.status(400).json({ error: 'Date is required' });
      }

      const targetDate = new Date(date);
      const startOfDay = new Date(targetDate);
      startOfDay.setHours(0, 0, 0, 0);
      const endOfDay = new Date(targetDate);
      endOfDay.setHours(23, 59, 59, 999);
      
      const EmailSchedulerService = require('../services/EmailSchedulerService');
      
      // Find all pending jobs scheduled for this date using Prisma
      const jobsToReschedule = await prisma.emailJob.findMany({
        where: {
          scheduledFor: { gte: startOfDay, lte: endOfDay },
          status: { in: RulebookService.getPendingOnlyStatuses() }
        }
      });
      
      console.log(`Found ${jobsToReschedule.length} jobs to reschedule from ${date}`);
      
      let rescheduled = 0;
      let failed = 0;
      
      for (const job of jobsToReschedule) {
        try {
          await EmailSchedulerService.moveJobToNextWorkingDay(job.id, `Date ${date} paused`);
          rescheduled++;
        } catch (error) {
          console.error(`Failed to move job ${job.id}:`, error);
          failed++;
        }
      }
      
      res.status(200).json({
        message: `Rescheduled ${rescheduled} emails from ${date}`,
        rescheduled,
        failed,
        total: jobsToReschedule.length
      });
    } catch (error) {
      console.error('Reschedule paused emails error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Test Brevo API connection
  async testBrevoConnection(req, res) {
    try {
      const { apiKey } = req.body;
      
      let keyToTest = apiKey;
      if (!keyToTest) {
        const settings = await SettingsRepository.getSettings();
        keyToTest = settings.brevo?.apiKey || process.env.BREVO_API_KEY;
      }
      
      if (!keyToTest) {
        return res.status(400).json({ 
          success: false, 
          error: 'No API key provided or configured' 
        });
      }
      
      const response = await axios.get(
        'https://api.brevo.com/v3/account',
        {
          headers: {
            'api-key': keyToTest,
            'Content-Type': 'application/json'
          }
        }
      );
      
      res.status(200).json({
        success: true,
        account: {
          email: response.data.email,
          firstName: response.data.firstName,
          lastName: response.data.lastName,
          plan: response.data.plan?.[0]?.type || 'Unknown'
        }
      });
    } catch (error) {
      console.error('Brevo connection test failed:', error.response?.data || error.message);
      res.status(400).json({
        success: false,
        error: error.response?.data?.message || error.message
      });
    }
  }

  // ========================================
  // RULEBOOK MANAGEMENT
  // ========================================

  // Get current rulebook configuration
  async getRulebook(req, res) {
    try {
      const rulebook = await SettingsRepository.getRulebook();
      res.status(200).json(rulebook);
    } catch (error) {
      console.error('Get rulebook error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Update rulebook configuration
  async updateRulebook(req, res) {
    try {
      const updates = req.body;
      
      if (!updates || Object.keys(updates).length === 0) {
        return res.status(400).json({ error: 'No updates provided' });
      }

      const rulebook = await SettingsRepository.updateRulebook(updates);
      res.status(200).json({
        message: 'Rulebook updated successfully',
        rulebook
      });
    } catch (error) {
      console.error('Update rulebook error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Reset rulebook to defaults
  async resetRulebook(req, res) {
    try {
      const rulebook = await SettingsRepository.resetRulebook();
      res.status(200).json({
        message: 'Rulebook reset to defaults',
        rulebook
      });
    } catch (error) {
      console.error('Reset rulebook error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Get default rulebook (without modifying current)
  async getDefaultRulebook(req, res) {
    try {
      const rulebook = SettingsRepository.getDefaultRulebook();
      res.status(200).json(rulebook);
    } catch (error) {
      console.error('Get default rulebook error:', error);
      res.status(500).json({ error: error.message });
    }
  }

  // Get mail type permissions for frontend
  // Returns simplified permissions object for action validation
  async getMailTypePermissions(req, res) {
    try {
      const permissions = RulebookService.getMailTypePermissions();
      const statuses = RulebookService.getStatusDefinitions();
      
      res.status(200).json({
        mailTypes: permissions,
        statuses,
        statusGroups: {
          active: RulebookService.getActiveStatuses(),
          retriable: RulebookService.getRetriableStatuses(),
          negative: RulebookService.getNegativeStatuses()
        }
      });
    } catch (error) {
      console.error('Get mail type permissions error:', error);
      res.status(500).json({ error: error.message });
    }
  }
}

module.exports = new SettingsController();
