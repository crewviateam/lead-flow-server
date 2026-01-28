// services/CronService.js
// Cron service using Prisma

const cron = require('node-cron');
const { prisma } = require('../lib/prisma');
const { LeadRepository, EmailJobRepository } = require('../repositories');
const { emailSendQueue } = require('../queues/emailQueues');
const AnalyticsPollingService = require('./AnalyticsPollingService');

class CronService {
  constructor() {
    this.jobs = [];
    this.isProcessingEmails = false;
    this.isPollingAnalytics = false;
  }

  init() {
    console.log('⏳ Cron Service initialized');
    
    // Run every minute - Check for pending jobs that are DUE
    this.scheduleJob('* * * * *', async () => {
      await this.processPendingEmails();
    });

    // Run every 30 minutes - Poll Brevo for analytics updates
    this.scheduleJob('*/30 * * * *', async () => {
      await this.pollAnalytics();
    });

    // Run every 5 minutes - Check for leads to unfreeze
    this.scheduleJob('*/5 * * * *', async () => {
      await this.processFrozenLeads();
    });

    // Run every hour at minute 0 - Check for weekly reports
    this.scheduleJob('0 * * * *', async () => {
      const WeeklyReportService = require('./WeeklyReportService');
      await WeeklyReportService.checkAndSendReport();
    });

    // Run every 5 minutes - Move jobs scheduled on paused dates
    this.scheduleJob('*/5 * * * *', async () => {
      await this.processPausedDates();
    });
  }

  async processFrozenLeads() {
    try {
      const now = new Date();
      const EmailSchedulerService = require('./EmailSchedulerService');

      // Find leads that are frozen and their time has expired
      const leadsToUnfreeze = await prisma.lead.findMany({
        where: {
          status: 'frozen',
          frozenUntil: { lte: now, not: null }
        }
      });

      if (leadsToUnfreeze.length > 0) {
        console.log(`Cron: Found ${leadsToUnfreeze.length} leads to unfreeze`);
        for (const lead of leadsToUnfreeze) {
          try {
            console.log(`Cron: Automatically unfreezing lead ${lead.email}`);
            await EmailSchedulerService.unfreezeLead(lead.id);
          } catch (err) {
            console.error(`Cron: Failed to unfreeze lead ${lead.email}:`, err);
          }
        }
      }
    } catch (error) {
      console.error('Cron: Error in processFrozenLeads:', error);
    }
  }

  async processPausedDates() {
    try {
      const EmailSchedulerService = require('./EmailSchedulerService');
      const result = await EmailSchedulerService.moveJobsOnPausedDates();
      if (result.moved > 0) {
        console.log(`⏸️ Cron: Moved ${result.moved} jobs from paused dates`);
      }
    } catch (error) {
      console.error('Cron: Error processing paused dates:', error);
    }
  }

  scheduleJob(expression, task) {
    const job = cron.schedule(expression, task);
    this.jobs.push(job);
    return job;
  }

  async processPendingEmails() {
    if (this.isProcessingEmails) {
      console.log('Skipping email cron cycle - previous cycle still running');
      return;
    }

    this.isProcessingEmails = true;
    try {
      const now = new Date();
      
      // Find jobs where scheduledFor <= now (TIME IS DUE)
      const pendingJobs = await prisma.emailJob.findMany({
        where: {
          status: 'pending',
          scheduledFor: { lte: now }
        },
        take: 500
      });

      if (pendingJobs.length > 0) {
        console.log(`Cron: Found ${pendingJobs.length} due email jobs at ${now.toISOString()}`);
        
        for (const job of pendingJobs) {
          // Use atomic update to prevent race conditions - only one process can claim the job
          const claimed = await prisma.emailJob.updateMany({
            where: { 
              id: job.id, 
              status: 'pending'  // Only update if still pending
            },
            data: { status: 'queued' }
          });
          
          // If no rows updated, another process already claimed this job
          if (claimed.count === 0) {
            console.log(`Cron: Job ${job.id} already claimed by another process`);
            continue;
          }
          
          // Fetch fresh job data after claiming
          const freshJob = await prisma.emailJob.findUnique({ where: { id: job.id } });
          if (!freshJob) continue;

          // Fetch lead name for personalization
          const lead = await LeadRepository.findById(freshJob.leadId);
          const leadName = lead ? lead.name : 'Valued Customer';

          // Add to BullMQ
          const queueJobId = freshJob.idempotencyKey || freshJob.id.toString();
          const queueJob = await emailSendQueue.add(
            'send-email',
            {
              emailJobId: freshJob.id.toString(),
              leadId: freshJob.leadId.toString(),
              leadEmail: freshJob.email,
              leadName: leadName,
              emailType: freshJob.type,
              city: lead ? lead.city : '',
              country: lead ? lead.country : ''
            },
            {
              jobId: queueJobId
            }
          );

          // Save the actual BullMQ job ID back to the EmailJob
          await prisma.emailJob.update({
            where: { id: freshJob.id },
            data: {
              metadata: {
                ...freshJob.metadata,
                queueJobId: queueJob.id
              }
            }
          });
          
          console.log(`Cron: Queued job ${freshJob.id} (BullMQ ID: ${queueJob.id}, scheduled for ${freshJob.scheduledFor.toISOString()})`);
        }
      }
    } catch (error) {
      console.error('Cron: Error processing pending emails:', error);
    } finally {
      this.isProcessingEmails = false;
    }
  }

  async pollAnalytics() {
    if (this.isPollingAnalytics) {
      console.log('Skipping analytics poll - previous poll still running');
      return;
    }

    this.isPollingAnalytics = true;
    try {
      console.log('📊 Cron: Polling Brevo for analytics updates...');
      await AnalyticsPollingService.pollBrevoEvents();
      console.log('📊 Cron: Analytics polling completed');
    } catch (error) {
      console.error('Cron: Error polling analytics:', error);
    } finally {
      this.isPollingAnalytics = false;
    }
  }
}

module.exports = new CronService();
