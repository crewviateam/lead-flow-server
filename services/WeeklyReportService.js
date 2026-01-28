// services/WeeklyReportService.js
// Weekly report service using Prisma

const moment = require('moment');
const { prisma } = require('../lib/prisma');
const { SettingsRepository } = require('../repositories');
const BrevoEmailService = require('./BrevoEmailService');

class WeeklyReportService {
  
  /**
   * Check if the report should be run now based on settings
   */
  async checkAndSendReport() {
    try {
      const settings = await SettingsRepository.getSettings();
      const config = settings.reporting;
      
      if (!config || !config.enabled) {
        return;
      }

      const now = moment();
      const todayDay = now.day(); // 0-6 (Sun-Sat)
      
      console.log(`[WeeklyReport] Check: today=${todayDay}, configured=${config.dayOfWeek}, time=${config.time}`);
      
      if (todayDay !== config.dayOfWeek) return;
      
      // Parse configured time safely using parseInt
      const timeParts = (config.time || '09:00').split(':');
      const configuredHour = parseInt(timeParts[0], 10);
      const nowHour = now.hour();
      
      console.log(`[WeeklyReport] Hour check: now=${nowHour}, configured=${configuredHour}`);
      
      if (configuredHour !== nowHour) return;
      
      // Check if we already sent today (prevent duplicate sends)
      const todayKey = now.format('YYYY-MM-DD');
      if (config.lastSentDate === todayKey) {
        console.log('[WeeklyReport] Already sent today, skipping');
        return;
      }
      
      console.log('📊 Generating weekly report...');
      await this.generateAndSendReport(config.recipients);
      
      // Update lastSentDate to prevent duplicate sends
      await SettingsRepository.updateSettings({
        reporting: { ...config, lastSentDate: todayKey }
      });
      
      console.log('📊 Weekly report sent and lastSentDate updated');
      
    } catch (error) {
      console.error('WeeklyReportService Error:', error);
    }
  }

  async generateAndSendReport(recipients) {
    if (!recipients || recipients.length === 0) {
      console.log('No recipients configured for weekly report');
      return;
    }

    const endDate = moment().endOf('day');
    const startDate = moment().subtract(7, 'days').startOf('day');
    
    const stats = await this.getStats(startDate.toDate(), endDate.toDate());
    const htmlContent = this.generateEmailHtml(stats, startDate, endDate);
    
    for (const recipient of recipients) {
      try {
        await BrevoEmailService.sendEmail({
          to: recipient,
          name: 'Admin',
          type: 'report',
          subject: `Weekly LeadFlow Report (${startDate.format('MMM D')} - ${endDate.format('MMM D')})`,
          htmlContent,
          idempotencyKey: `report-${moment().format('YYYY-MM-DD')}-${recipient}`
        });
        console.log(`✅ Weekly report sent to ${recipient}`);
      } catch (err) {
        console.error(`Failed to send report to ${recipient}:`, err.message);
      }
    }
  }

  async getStats(start, end) {
    // Use Prisma for all counts - ALWAYS exclude 'rescheduled' status for accurate counts
    const excludeRescheduled = { NOT: { status: 'rescheduled' } };
    
    const [
      totalLeads,
      emailsSent,
      opened,
      clicked,
      bounced,
      activeLeads,
      convertedLeads
    ] = await Promise.all([
      prisma.lead.count({
        where: { createdAt: { gte: start, lte: end } }
      }),
      // Count unique sent jobs (excluding rescheduled)
      prisma.emailJob.count({
        where: { 
          sentAt: { gte: start, lte: end },
          ...excludeRescheduled
        }
      }),
      prisma.emailJob.count({
        where: {
          status: { in: ['opened', 'clicked'] },
          updatedAt: { gte: start, lte: end },
          ...excludeRescheduled
        }
      }),
      prisma.emailJob.count({
        where: {
          status: 'clicked',
          updatedAt: { gte: start, lte: end },
          ...excludeRescheduled
        }
      }),
      prisma.emailJob.count({
        where: {
          status: { in: ['hard_bounce', 'soft_bounce'] },
          updatedAt: { gte: start, lte: end },
          ...excludeRescheduled
        }
      }),
      prisma.lead.count({
        where: {
          status: { contains: 'scheduled' }
        }
      }),
      prisma.lead.count({
        where: {
          status: 'converted',
          updatedAt: { gte: start, lte: end }
        }
      })
    ]);

    // Also count pending leads
    const pendingLeads = await prisma.lead.count({
      where: { status: { contains: 'pending' } }
    });

    return {
      totalLeads,
      emailsSent,
      opened,
      clicked,
      bounced,
      activeLeads: activeLeads + pendingLeads,
      convertedLeads,
      openRate: emailsSent > 0 ? Math.round((opened / emailsSent) * 100) : 0,
      clickRate: emailsSent > 0 ? Math.round((clicked / emailsSent) * 100) : 0
    };
  }

  generateEmailHtml(stats, start, end) {
    return `
      <!DOCTYPE html>
      <html>
      <head>
        <style>
          body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; }
          .container { max-width: 600px; margin: 0 auto; padding: 20px; background: #f9fafb; }
          .header { background: #7c3aed; color: white; padding: 20px; border-radius: 8px 8px 0 0; text-align: center; }
          .card { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
          .stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }
          .stat-item { background: #f3f4f6; padding: 15px; border-radius: 8px; text-align: center; }
          .stat-value { font-size: 24px; font-weight: bold; color: #7c3aed; }
          .stat-label { font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.5px; }
          .footer { text-align: center; font-size: 12px; color: #9ca3af; margin-top: 20px; }
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h1 style="margin:0; font-size: 24px;">Weekly Performance</h1>
            <p style="margin:5px 0 0 0; opacity: 0.9;">${start.format('MMMM D')} - ${end.format('MMMM D, YYYY')}</p>
          </div>
          
          <div class="card">
            <h2 style="margin-top:0; font-size: 18px; border-bottom: 1px solid #e5e7eb; padding-bottom: 10px;">Engagement Overview</h2>
            <div class="stat-grid">
              <div class="stat-item">
                <div class="stat-value">${stats.openRate}%</div>
                <div class="stat-label">Open Rate</div>
              </div>
              <div class="stat-item">
                <div class="stat-value">${stats.clickRate}%</div>
                <div class="stat-label">Click Rate</div>
              </div>
              <div class="stat-item">
                <div class="stat-value">${stats.emailsSent}</div>
                <div class="stat-label">Emails Sent</div>
              </div>
              <div class="stat-item">
                <div class="stat-value">${stats.convertedLeads}</div>
                <div class="stat-label">Conversions</div>
              </div>
            </div>
          </div>

          <div class="card">
            <h2 style="margin-top:0; font-size: 18px; border-bottom: 1px solid #e5e7eb; padding-bottom: 10px;">System Health</h2>
            <table style="width:100%; border-collapse: collapse;">
              <tr>
                <td style="padding: 8px 0; border-bottom: 1px solid #f3f4f6;">New Leads</td>
                <td style="text-align: right; font-weight: bold;">${stats.totalLeads}</td>
              </tr>
              <tr>
                <td style="padding: 8px 0; border-bottom: 1px solid #f3f4f6;">Active in Sequences</td>
                <td style="text-align: right; font-weight: bold;">${stats.activeLeads}</td>
              </tr>
              <tr>
                <td style="padding: 8px 0; border-bottom: 1px solid #f3f4f6;">Bounces</td>
                <td style="text-align: right; font-weight: bold; color: #ef4444;">${stats.bounced}</td>
              </tr>
            </table>
          </div>
          
          <div class="footer">
            <p>Sent by LeadFlow Automation System</p>
          </div>
        </div>
      </body>
      </html>
    `;
  }
}

module.exports = new WeeklyReportService();
