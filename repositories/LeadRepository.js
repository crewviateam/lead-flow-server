// repositories/LeadRepository.js
// Data access layer for Lead operations with Prisma

const { prisma } = require('../lib/prisma');

class LeadRepository {
  
  /**
   * Find lead by ID with optional includes
   */
  async findById(id, options = {}) {
    const parsedId = parseInt(id);
    if (isNaN(parsedId)) {
      return null;
    }
    
    const include = options.include || {};
    return prisma.lead.findUnique({
      where: { id: parsedId },
      include: {
        emailSchedule: include.emailSchedule === true,
        emailJobs: include.emailJobs === true,
        eventHistory: include.eventHistory === true,
        manualMails: include.manualMails === true
      }
    });
  }

  /**
   * Find lead by email
   */
  async findByEmail(email) {
    return prisma.lead.findUnique({
      where: { email: email.toLowerCase().trim() }
    });
  }

  /**
   * Find leads with pagination and filtering
   */
  async findMany({ 
    page = 1, 
    limit = 50, 
    status, 
    tags, 
    sortBy = 'createdAt', 
    sortOrder = 'desc' 
  }) {
    const where = {};
    
    if (status) {
      const statusList = status.includes(',') ? status.split(',').map(s => s.trim()) : [status.trim()];
      
      // Check if filtering for failure statuses - also check email jobs
      const failureStatuses = ['blocked', 'failed', 'hard_bounce', 'soft_bounce', 'spam', 'deferred', 'bounced'];
      const isFailureFilter = statusList.some(s => failureStatuses.includes(s.toLowerCase()));
      
      if (isFailureFilter) {
        // For failure filters, check BOTH lead.status AND email job status
        where.OR = [
          // Lead status contains the failure type (e.g., "Initial Email:blocked")
          ...statusList.map(s => ({
            status: { contains: s, mode: 'insensitive' }
          })),
          // OR Lead has an email job with this failure status
          {
            emailJobs: {
              some: {
                status: { in: statusList.map(s => s.toLowerCase()) }
              }
            }
          }
        ];
      } else {
        // For non-failure statuses, use contains matching as before
        where.OR = statusList.map(s => ({
          status: { contains: s, mode: 'insensitive' }
        }));
      }
    }
    
    if (tags) {
      const tagList = tags.split(',').map(t => t.toLowerCase().trim()).filter(Boolean);
      if (tagList.length > 0) {
        where.tags = { hasEvery: tagList };
      }
    }

    const orderBy = {};
    if (sortBy === 'score') {
      orderBy.score = sortOrder;
    } else if (sortBy === 'name') {
      orderBy.name = sortOrder;
    } else {
      orderBy.createdAt = sortOrder;
    }

    const [leads, total] = await Promise.all([
      prisma.lead.findMany({
        where,
        orderBy,
        skip: (page - 1) * limit,
        take: limit,
        include: {
          emailSchedule: true
        }
      }),
      prisma.lead.count({ where })
    ]);

    return {
      leads,
      pagination: {
        page,
        limit,
        total,
        pages: Math.ceil(total / limit)
      }
    };
  }

  /**
   * Find leads with cursor-based pagination (more efficient for large datasets)
   * Use this for infinite scroll or when dealing with 10K+ leads
   * @param {number|null} cursor - The ID to start from (null for first page)
   * @param {number} limit - Number of items to fetch
   * @param {string} direction - 'forward' or 'backward'
   * @returns {leads: Lead[], nextCursor: number|null, hasMore: boolean}
   */
  async findManyCursor({ 
    cursor = null, 
    limit = 50, 
    status,
    tags,
    sortOrder = 'desc' 
  }) {
    const where = {};
    
    if (status) {
      where.status = { contains: status, mode: 'insensitive' };
    }
    
    if (tags) {
      const tagList = tags.split(',').map(t => t.toLowerCase().trim()).filter(Boolean);
      if (tagList.length > 0) {
        where.tags = { hasEvery: tagList };
      }
    }

    const leads = await prisma.lead.findMany({
      where,
      take: limit + 1, // Fetch one extra to check if there's more
      ...(cursor && {
        cursor: { id: cursor },
        skip: 1 // Skip the cursor item itself
      }),
      orderBy: { id: sortOrder },
      include: {
        emailSchedule: true
      }
    });

    // Check if there are more results
    const hasMore = leads.length > limit;
    
    // Remove the extra item we fetched
    if (hasMore) {
      leads.pop();
    }

    // Get the next cursor (last item's ID)
    const nextCursor = leads.length > 0 ? leads[leads.length - 1].id : null;

    return {
      leads,
      nextCursor,
      hasMore
    };
  }

  /**
   * Create a new lead
   */
  async create(data) {
    return prisma.lead.create({
      data: {
        email: data.email.toLowerCase().trim(),
        name: data.name,
        country: data.country,
        city: data.city,
        timezone: data.timezone,
        status: data.status || 'pending',
        queueStatus: data.queueStatus || 'pending',
        tags: data.tags || [],
        emailSchedule: {
          create: {
            initialStatus: 'pending',
            followups: JSON.stringify([])
          }
        }
      },
      include: {
        emailSchedule: true
      }
    });
  }

  /**
   * Update a lead
   */
  async update(id, data) {
    return prisma.lead.update({
      where: { id: parseInt(id) },
      data: {
        ...data,
        updatedAt: new Date()
      }
    });
  }

  /**
   * Delete a lead and all related data
   */
  async delete(id) {
    return prisma.lead.delete({
      where: { id: parseInt(id) }
    });
  }

  /**
   * Add event to lead's history with deduplication
   * Only adds event if it's new or higher in the event hierarchy
   */
  async addEvent(leadId, event, details = {}, emailType = null, emailJobId = null) {
    // Event hierarchy - higher number means more "progressed" state
    const eventHierarchy = {
      'scheduled': 1,
      'queued': 2,
      'sent': 3,
      'delivered': 4,
      'opened': 5,
      'unique_opened': 5,
      'clicked': 6,
      'soft_bounce': 7,
      'hard_bounce': 8,
      'failed': 8,
      'blocked': 8,
      'spam': 8
    };
    
    // If we have an emailJobId, check for existing events to prevent duplicates
    if (emailJobId) {
      const existingEvents = await prisma.eventHistory.findMany({
        where: {
          leadId: parseInt(leadId),
          emailJobId: parseInt(emailJobId)
        },
        orderBy: { timestamp: 'desc' }
      });
      
      // Check if this exact event already exists (deduplication)
      const duplicateEvent = existingEvents.find(e => e.event === event);
      if (duplicateEvent) {
        console.log(`[LeadRepository] Skipping duplicate event: ${event} for job ${emailJobId}`);
        return duplicateEvent; // Return existing event, don't create new
      }
      
      // Check if a higher-ranked event already exists (event progression)
      const newEventRank = eventHierarchy[event] || 0;
      const higherEventExists = existingEvents.some(e => {
        const existingRank = eventHierarchy[e.event] || 0;
        // Don't add lower events if higher ones exist (e.g., don't add 'sent' if 'delivered' exists)
        return existingRank >= newEventRank && existingRank > 0 && newEventRank > 0;
      });
      
      if (higherEventExists && newEventRank > 0) {
        console.log(`[LeadRepository] Skipping event ${event} - higher event already exists for job ${emailJobId}`);
        return existingEvents[0]; // Return latest event
      }
    }
    
    // Create the new event
    return prisma.eventHistory.create({
      data: {
        leadId: parseInt(leadId),
        event,
        details: details,
        emailType,
        emailJobId: emailJobId ? parseInt(emailJobId) : null,
        timestamp: details.timestamp || new Date()
      }
    });
  }

  /**
   * Update lead status with atomic operation
   */
  async updateStatus(id, status, queueStatus = null) {
    const data = { status, updatedAt: new Date() };
    if (queueStatus) data.queueStatus = queueStatus;
    
    return prisma.lead.update({
      where: { id: parseInt(id) },
      data
    });
  }

  /**
   * Increment counters atomically
   */
  async incrementCounter(id, counter, amount = 1) {
    const validCounters = ['emailsSent', 'emailsOpened', 'emailsClicked', 'emailsBounced'];
    if (!validCounters.includes(counter)) {
      throw new Error(`Invalid counter: ${counter}`);
    }
    
    return prisma.lead.update({
      where: { id: parseInt(id) },
      data: {
        [counter]: { increment: amount }
      }
    });
  }

  /**
   * Update score atomically
   */
  async updateScore(id, delta) {
    return prisma.lead.update({
      where: { id: parseInt(id) },
      data: {
        score: { increment: delta }
      }
    });
  }

  /**
   * Manage tags
   */
  async addTags(id, tags) {
    const lead = await this.findById(id);
    const existingTags = new Set(lead.tags || []);
    tags.forEach(t => existingTags.add(t.toLowerCase().trim()));
    
    return prisma.lead.update({
      where: { id: parseInt(id) },
      data: { tags: Array.from(existingTags) }
    });
  }

  async removeTags(id, tags) {
    const lead = await this.findById(id);
    const tagsToRemove = new Set(tags.map(t => t.toLowerCase().trim()));
    const newTags = (lead.tags || []).filter(t => !tagsToRemove.has(t));
    
    return prisma.lead.update({
      where: { id: parseInt(id) },
      data: { tags: newTags }
    });
  }

  /**
   * Get all unique tags
   */
  async getAllTags() {
    const leads = await prisma.lead.findMany({
      select: { tags: true }
    });
    
    const tagCounts = {};
    leads.forEach(lead => {
      (lead.tags || []).forEach(tag => {
        tagCounts[tag] = (tagCounts[tag] || 0) + 1;
      });
    });
    
    return {
      tags: Object.keys(tagCounts).sort(),
      tagCounts
    };
  }

  /**
   * Find leads by failure status (for Failed Outreach page)
   */
  async findByFailureStatus(page = 1, limit = 50) {
    const failureStatuses = ['hard_bounce', 'failed', 'blocked', 'spam', 'deferred'];
    
    const where = {
      OR: [
        { status: { in: failureStatuses } },
        { status: { contains: 'blocked' } },
        { status: { contains: 'failed' } },
        { status: { contains: 'hard_bounce' } },
        { status: { contains: 'spam' } }
      ]
    };

    const [leads, total] = await Promise.all([
      prisma.lead.findMany({
        where,
        orderBy: { updatedAt: 'desc' },
        skip: (page - 1) * limit,
        take: limit,
        include: {
          emailSchedule: true,
          emailJobs: {
            where: { status: { in: failureStatuses } },
            orderBy: { updatedAt: 'desc' },
            take: 1
          }
        }
      }),
      prisma.lead.count({ where })
    ]);

    return { leads, pagination: { page, limit, total, pages: Math.ceil(total / limit) } };
  }

  /**
   * Update email schedule
   */
  async updateEmailSchedule(leadId, scheduleData) {
    return prisma.emailSchedule.upsert({
      where: { leadId: parseInt(leadId) },
      update: scheduleData,
      create: {
        leadId: parseInt(leadId),
        ...scheduleData
      }
    });
  }

  /**
   * Add manual mail to lead
   */
  async addManualMail(leadId, mailData) {
    return prisma.manualMail.create({
      data: {
        leadId: parseInt(leadId),
        title: mailData.title,
        scheduledFor: mailData.scheduledFor,
        templateId: mailData.templateId ? parseInt(mailData.templateId) : null,
        status: mailData.status || 'pending',
        emailJobId: mailData.emailJobId ? parseInt(mailData.emailJobId) : null
      }
    });
  }

  /**
   * Get stats for dashboard
   */
  async getStats() {
    const stats = await prisma.lead.groupBy({
      by: ['status'],
      _count: { status: true }
    });

    return stats.reduce((acc, stat) => {
      acc[stat.status] = stat._count.status;
      return acc;
    }, {});
  }
}

module.exports = new LeadRepository();
