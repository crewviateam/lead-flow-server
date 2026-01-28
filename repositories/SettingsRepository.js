// repositories/SettingsRepository.js
// Data access layer for Settings (singleton pattern) with caching

const { prisma } = require('../lib/prisma');
const { cache } = require('../lib/cache');

// Default followups configuration
const DEFAULT_FOLLOWUPS = [
  { id: 'followup_initial', name: 'Initial Email', delayDays: 0, enabled: true, order: 0 },
  { id: 'followup_first', name: 'First Followup', delayDays: 3, enabled: true, order: 1 },
  { id: 'followup_second', name: 'Second Followup', delayDays: 7, enabled: true, order: 2 }
];

const DEFAULT_SMART_SEND_TIME = {
  enabled: true,
  morningWindow: { startHour: 9, endHour: 11 },
  afternoonWindow: { startHour: 14, endHour: 16 },
  priority: 'morning'
};

class SettingsRepository {

  /**
   * Get global settings (with caching)
   */
  async getSettings() {
    // Try cache first
    const cached = await cache.getSettings();
    if (cached) return cached;

    // Fetch from DB or create default
    let settings = await prisma.settings.findUnique({
      where: { id: 'global' }
    });

    if (!settings) {
      settings = await this.createDefault();
    }

    // Transform to legacy format for backward compatibility
    const transformed = this._transformToLegacy(settings);
    
    // Cache it
    await cache.setSettings(transformed);
    
    return transformed;
  }

  /**
   * Create default settings
   */
  async createDefault() {
    return prisma.settings.create({
      data: {
        id: 'global',
        followups: JSON.stringify(DEFAULT_FOLLOWUPS),
        smartSendTime: JSON.stringify(DEFAULT_SMART_SEND_TIME)
      }
    });
  }

  /**
   * Update settings
   */
  async updateSettings(updates) {
    const data = {};
    
    if (updates.rateLimit) {
      if (updates.rateLimit.emailsPerWindow !== undefined) {
        data.rateLimitEmailsPerWindow = updates.rateLimit.emailsPerWindow;
      }
      if (updates.rateLimit.windowMinutes !== undefined) {
        data.rateLimitWindowMinutes = updates.rateLimit.windowMinutes;
      }
    }

    if (updates.businessHours) {
      if (updates.businessHours.startHour !== undefined) {
        data.businessHoursStart = updates.businessHours.startHour;
      }
      if (updates.businessHours.endHour !== undefined) {
        data.businessHoursEnd = updates.businessHours.endHour;
      }
      if (updates.businessHours.weekendDays !== undefined) {
        data.weekendDays = updates.businessHours.weekendDays;
      }
    }

    if (updates.retry) {
      if (updates.retry.maxAttempts !== undefined) {
        data.retryMaxAttempts = updates.retry.maxAttempts;
      }
      if (updates.retry.softBounceDelayHours !== undefined) {
        data.retrySoftBounceDelayHrs = updates.retry.softBounceDelayHours;
      }
    }

    if (updates.brevo) {
      if (updates.brevo.apiKey !== undefined) data.brevoApiKey = updates.brevo.apiKey;
      if (updates.brevo.fromEmail !== undefined) data.brevoFromEmail = updates.brevo.fromEmail;
      if (updates.brevo.fromName !== undefined) data.brevoFromName = updates.brevo.fromName;
    }

    if (updates.smartSendTime) {
      data.smartSendTime = JSON.stringify(updates.smartSendTime);
    }

    if (updates.reporting) {
      if (updates.reporting.enabled !== undefined) data.reportingEnabled = updates.reporting.enabled;
      if (updates.reporting.recipients !== undefined) data.reportingRecipients = updates.reporting.recipients;
      if (updates.reporting.dayOfWeek !== undefined) data.reportingDayOfWeek = updates.reporting.dayOfWeek;
      if (updates.reporting.time !== undefined) data.reportingTime = updates.reporting.time;
    }

    // Handle followups updates (this was missing!)
    if (updates.followups) {
      data.followups = JSON.stringify(updates.followups);
    }

    // Handle pausedDates updates - CRITICAL: This was missing and causing paused dates to never be saved!
    if (updates.pausedDates !== undefined) {
      data.pausedDates = updates.pausedDates;
    }

    const settings = await prisma.settings.update({
      where: { id: 'global' },
      data
    });

    // Invalidate cache
    await cache.invalidateSettings();

    return this._transformToLegacy(settings);
  }

  /**
   * Get followups
   */
  async getFollowups() {
    const settings = await this.getSettings();
    return settings.followups || DEFAULT_FOLLOWUPS;
  }

  /**
   * Add a followup
   */
  async addFollowup(followupData) {
    const settings = await this.getSettings();
    const followups = settings.followups || [];
    
    const maxOrder = followups.reduce((max, f) => Math.max(max, f.order || 0), 0);
    followups.push({
      id: `followup_${Date.now()}`,
      name: followupData.name,
      delayDays: parseInt(followupData.delayDays),
      enabled: followupData.enabled !== false,
      order: maxOrder + 1,
      templateId: followupData.templateId || null,
      condition: followupData.condition || { type: 'always' }
    });

    await prisma.settings.update({
      where: { id: 'global' },
      data: { followups: JSON.stringify(followups) }
    });

    await cache.invalidateSettings();
    return followups;
  }

  /**
   * Update a followup
   */
  async updateFollowup(followupId, updates) {
    const settings = await this.getSettings();
    const followups = settings.followups || [];
    
    const idx = followups.findIndex(f => f.id === followupId || f.name === followupId);
    if (idx === -1) throw new Error('Followup not found');

    Object.assign(followups[idx], updates);

    await prisma.settings.update({
      where: { id: 'global' },
      data: { followups: JSON.stringify(followups) }
    });

    await cache.invalidateSettings();
    return followups;
  }

  /**
   * Delete a followup
   */
  async deleteFollowup(followupId) {
    const settings = await this.getSettings();
    const followups = (settings.followups || []).filter(f => f.id !== followupId && f.name !== followupId);

    await prisma.settings.update({
      where: { id: 'global' },
      data: { followups: JSON.stringify(followups) }
    });

    await cache.invalidateSettings();
    return followups;
  }

  /**
   * Reorder followups
   */
  async reorderFollowups(followupIds) {
    const settings = await this.getSettings();
    const followups = settings.followups || [];
    
    followupIds.forEach((id, index) => {
      const followup = followups.find(f => f.id === id || f.name === id);
      if (followup) followup.order = index;
    });

    followups.sort((a, b) => a.order - b.order);

    await prisma.settings.update({
      where: { id: 'global' },
      data: { followups: JSON.stringify(followups) }
    });

    await cache.invalidateSettings();
    return followups;
  }

  /**
   * Paused dates management
   */
  async getPausedDates() {
    const settings = await this.getSettings();
    return {
      pausedDates: settings.pausedDates || [],
      weekendDays: settings.businessHours?.weekendDays || [0, 6]
    };
  }

  async pauseDate(date) {
    const pauseDate = new Date(date);
    pauseDate.setHours(0, 0, 0, 0);

    const settings = await prisma.settings.findUnique({ where: { id: 'global' } });
    const pausedDates = settings.pausedDates || [];
    
    if (!pausedDates.some(pd => new Date(pd).toDateString() === pauseDate.toDateString())) {
      pausedDates.push(pauseDate);
      await prisma.settings.update({
        where: { id: 'global' },
        data: { pausedDates }
      });
      await cache.invalidateSettings();
    }

    return pausedDates;
  }

  async unpauseDate(date) {
    const unpauseDate = new Date(date);
    unpauseDate.setHours(0, 0, 0, 0);

    const settings = await prisma.settings.findUnique({ where: { id: 'global' } });
    const pausedDates = (settings.pausedDates || []).filter(pd => 
      new Date(pd).toDateString() !== unpauseDate.toDateString()
    );

    await prisma.settings.update({
      where: { id: 'global' },
      data: { pausedDates }
    });

    await cache.invalidateSettings();
    return pausedDates;
  }

  /**
   * Transform DB model to legacy format for backward compatibility
   */
  _transformToLegacy(settings) {
    return {
      id: settings.id,
      rateLimit: {
        emailsPerWindow: settings.rateLimitEmailsPerWindow,
        windowMinutes: settings.rateLimitWindowMinutes
      },
      businessHours: {
        startHour: settings.businessHoursStart,
        endHour: settings.businessHoursEnd,
        weekendDays: settings.weekendDays
      },
      followups: (() => {
        let followups = typeof settings.followups === 'string' 
          ? JSON.parse(settings.followups) 
          : (settings.followups || DEFAULT_FOLLOWUPS);
        // Ensure all followups have IDs (migration)
        return followups.map((f, index) => ({
          ...f,
          id: f.id || `followup_${f.name?.replace(/\s+/g, '_').toLowerCase() || index}`
        }));
      })(),
      smartSendTime: typeof settings.smartSendTime === 'string'
        ? JSON.parse(settings.smartSendTime)
        : (settings.smartSendTime || DEFAULT_SMART_SEND_TIME),
      pausedDates: settings.pausedDates || [],
      retry: {
        maxAttempts: settings.retryMaxAttempts,
        softBounceDelayHours: settings.retrySoftBounceDelayHrs
      },
      brevo: {
        apiKey: settings.brevoApiKey,
        fromEmail: settings.brevoFromEmail,
        fromName: settings.brevoFromName
      },
      reporting: {
        enabled: settings.reportingEnabled,
        recipients: settings.reportingRecipients,
        dayOfWeek: settings.reportingDayOfWeek,
        time: settings.reportingTime
      },
      rulebook: settings.rulebook || null,
      updatedAt: settings.updatedAt,
      updatedBy: settings.updatedBy
    };
  }

  // ========================================
  // RULEBOOK MANAGEMENT
  // ========================================
  
  /**
   * Get current rulebook configuration
   */
  async getRulebook() {
    const RulebookService = require('../services/RulebookService');
    return RulebookService.getRulebook();
  }

  /**
   * Update rulebook configuration
   */
  async updateRulebook(updates) {
    const RulebookService = require('../services/RulebookService');
    return RulebookService.updateRulebook(updates);
  }

  /**
   * Reset rulebook to defaults
   */
  async resetRulebook() {
    const RulebookService = require('../services/RulebookService');
    return RulebookService.resetRulebook();
  }

  /**
   * Get default rulebook
   */
  getDefaultRulebook() {
    const RulebookService = require('../services/RulebookService');
    return RulebookService.getDefaultRulebook();
  }
}

module.exports = new SettingsRepository();

