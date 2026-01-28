// services/AnalyticsPollingService.js
// Analytics polling service using Prisma

const axios = require('axios');
const moment = require('moment-timezone');
const { prisma } = require('../lib/prisma');
const { SettingsRepository } = require('../repositories');
require('dotenv').config();

class AnalyticsPollingService {
  constructor() {
    this.baseUrl = 'https://api.brevo.com/v3';
    this.processedEvents = new Set();
    this._cachedApiKey = null;
    this._cacheTime = null;
    this._cacheDurationMs = 60000; // 1 minute
  }
  
  /**
   * Get Brevo API key from Settings DB with fallback to .env
   */
  async getApiKey() {
    const now = Date.now();
    if (this._cachedApiKey && this._cacheTime && (now - this._cacheTime) < this._cacheDurationMs) {
      return this._cachedApiKey;
    }
    
    try {
      const settings = await SettingsRepository.getSettings();
      this._cachedApiKey = settings.brevo?.apiKey || process.env.BREVO_API_KEY || '';
      this._cacheTime = now;
      return this._cachedApiKey;
    } catch (error) {
      console.error('Error loading API key from DB:', error.message);
      return process.env.BREVO_API_KEY || '';
    }
  }


  /**
   * Poll Brevo for ALL email events and update analytics directly
   */
  async pollBrevoEvents() {
    console.log('📊 Analytics Poll: Starting comprehensive event sync...');
    
    try {
      const events = await this.fetchAllRecentEvents();
      
      console.log(`📊 Analytics Poll: Fetched ${events.length} events from Brevo`);
      
      let processed = 0;
      let updated = 0;

      for (const event of events) {
        try {
          const wasUpdated = await this.processBrevoEvent(event);
          processed++;
          if (wasUpdated) updated++;
        } catch (error) {
          console.error(`Analytics Poll: Error processing event:`, error.message);
        }
      }

      console.log(`📊 Analytics Poll: Processed ${processed} events, Updated ${updated} records`);
      return { fetched: events.length, processed, updated };
    } catch (error) {
      console.error('Analytics Poll: Error:', error.message);
      throw error;
    }
  }

  /**
   * Fetch all events from Brevo for the last 24 hours
   */
  async fetchAllRecentEvents() {
    const allEvents = [];
    const endDate = new Date();
    const startDate = new Date(Date.now() - 24 * 60 * 60 * 1000);

    const eventTypes = ['delivered', 'opened', 'uniqueOpened', 'clicks', 'hardBounces', 'softBounces', 'deferred', 'blocked', 'spam', 'requests'];
    
    for (const eventType of eventTypes) {
      try {
        const events = await this.fetchEventsByType(eventType, startDate, endDate);
        allEvents.push(...events);
      } catch (error) {
        console.log(`Analytics Poll: No ${eventType} events or error:`, error.message);
      }
    }

    return allEvents;
  }

  /**
   * Fetch events of a specific type from Brevo
   */
  async fetchEventsByType(eventType, startDate, endDate) {
    try {
      const apiKey = await this.getApiKey();
      
      const response = await axios.get(
        `${this.baseUrl}/smtp/statistics/events`,
        {
          params: {
            event: eventType,
            startDate: startDate.toISOString().split('T')[0],
            endDate: endDate.toISOString().split('T')[0],
            limit: 500,
            offset: 0
          },
          headers: {
            'api-key': apiKey,
            'Content-Type': 'application/json'
          }
        }
      );

      const events = response.data.events || [];
      return events.map(e => ({ ...e, fetchedEventType: eventType }));
    } catch (error) {
      if (error.response?.status === 404 || error.response?.status === 400) {
        return [];
      }
      throw error;
    }
  }


  /**
   * Process a single Brevo event and update analytics
   */
  async processBrevoEvent(event) {
    const AnalyticsService = require('./AnalyticsService');
    return await AnalyticsService.handleEvent(event, 'poll');
  }


  /**
   * Rebuilds the daily analytics summary from EmailJobs using Prisma
   */
  async rebuildAnalyticsFromJobs(targetDate = new Date()) {
    const startOfDay = moment(targetDate).startOf('day').toDate();
    const endOfDay = moment(targetDate).endOf('day').toDate();
    
    console.log(`Analytics Poll: Rebuilding stats for ${moment(startOfDay).format('YYYY-MM-DD')}...`);

    // Use Prisma raw SQL for aggregation
    const result = await prisma.$queryRaw`
      SELECT 
        COUNT(*) as sent,
        COUNT(*) FILTER (WHERE delivered_at IS NOT NULL) as delivered,
        COUNT(*) FILTER (WHERE opened_at IS NOT NULL) as opened,
        COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) as clicked,
        COUNT(*) FILTER (WHERE status = 'soft_bounce') as soft_bounce,
        COUNT(*) FILTER (WHERE status = 'hard_bounce') as hard_bounce,
        COUNT(*) FILTER (WHERE status = 'deferred') as deferred,
        COUNT(*) FILTER (WHERE status = 'blocked') as blocked,
        COUNT(*) FILTER (WHERE status = 'spam') as spam,
        COUNT(*) FILTER (WHERE status = 'failed') as failed,
        COUNT(*) FILTER (WHERE status = 'bounced') as bounced
      FROM email_jobs
      WHERE sent_at >= ${startOfDay} 
        AND sent_at <= ${endOfDay}
        AND status != 'rescheduled'
    `;

    const stats = result[0] || {};
    
    const update = {
      emailsSent: Number(stats.sent) || 0,
      emailsDelivered: Number(stats.delivered) || 0,
      emailsOpened: Number(stats.opened) || 0,
      emailsClicked: Number(stats.clicked) || 0,
      emailsSoftBounced: Number(stats.soft_bounce) || 0,
      emailsHardBounced: Number(stats.hard_bounce) || 0,
      emailsDeferred: Number(stats.deferred) || 0,
      emailsBlocked: Number(stats.blocked) || 0,
      emailsSpam: Number(stats.spam) || 0,
      emailsFailed: (Number(stats.failed) || 0) + (Number(stats.hard_bounce) || 0) + (Number(stats.blocked) || 0) + (Number(stats.spam) || 0),
      emailsBounced: (Number(stats.bounced) || 0) + (Number(stats.soft_bounce) || 0) + (Number(stats.hard_bounce) || 0),
      deliveryRate: Number(stats.sent) > 0 ? ((Number(stats.delivered) / Number(stats.sent)) * 100).toFixed(1) : 0
    };

    console.log('Analytics Poll: Rebuild complete and persisted.', update);
    return update;
  }
}

module.exports = new AnalyticsPollingService();
