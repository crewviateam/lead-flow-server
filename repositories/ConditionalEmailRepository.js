// repositories/ConditionalEmailRepository.js
// Repository for managing conditional emails and their jobs

const { prisma } = require('../lib/prisma');
const RulebookService = require('../services/RulebookService');

class ConditionalEmailRepository {
  
  // ============================
  // CONDITIONAL EMAIL CRUD
  // ============================
  
  /**
   * Get all conditional emails
   */
  async findAll() {
    return prisma.conditionalEmail.findMany({
      include: {
        template: {
          select: { id: true, name: true, subject: true }
        },
        _count: {
          select: { jobs: true }
        }
      },
      orderBy: { priority: 'desc' }
    });
  }
  
  /**
   * Get enabled conditional emails
   */
  async findEnabled() {
    return prisma.conditionalEmail.findMany({
      where: { enabled: true },
      include: {
        template: true
      },
      orderBy: { priority: 'desc' }
    });
  }
  
  /**
   * Find conditional emails by trigger event
   */
  async findByTriggerEvent(triggerEvent, triggerStep = null) {
    const where = { 
      enabled: true,
      triggerEvent 
    };
    
    if (triggerStep) {
      where.triggerStep = triggerStep;
    }
    
    return prisma.conditionalEmail.findMany({
      where,
      include: { template: true },
      orderBy: { priority: 'desc' }
    });
  }
  
  /**
   * Get a conditional email by ID
   */
  async findById(id) {
    return prisma.conditionalEmail.findUnique({
      where: { id: parseInt(id) },
      include: {
        template: true,
        jobs: {
          take: 10,
          orderBy: { createdAt: 'desc' },
          include: {
            lead: {
              select: { id: true, email: true, name: true }
            }
          }
        }
      }
    });
  }
  
  /**
   * Create a new conditional email
   */
  async create(data) {
    // Properly handle templateId - empty string, null, or invalid should be null
    let templateId = null;
    if (data.templateId && data.templateId !== '' && !isNaN(parseInt(data.templateId))) {
      templateId = parseInt(data.templateId);
    }
    
    return prisma.conditionalEmail.create({
      data: {
        name: data.name,
        description: data.description || null,
        triggerEvent: data.triggerEvent,
        triggerStep: data.triggerStep,
        delayHours: data.delayHours || 0,
        templateId: templateId,
        cancelPending: data.cancelPending !== false, // Default true
        priority: data.priority || 10,
        enabled: data.enabled !== false // Default true
      },
      include: { template: true }
    });
  }
  
  /**
   * Update a conditional email
   */
  async update(id, data) {
    const updateData = {};
    
    if (data.name !== undefined) updateData.name = data.name;
    if (data.description !== undefined) updateData.description = data.description;
    if (data.triggerEvent !== undefined) updateData.triggerEvent = data.triggerEvent;
    if (data.triggerStep !== undefined) updateData.triggerStep = data.triggerStep;
    if (data.delayHours !== undefined) updateData.delayHours = data.delayHours;
    if (data.templateId !== undefined) {
      // Handle empty string, null, or invalid values
      if (data.templateId && data.templateId !== '' && !isNaN(parseInt(data.templateId))) {
        updateData.templateId = parseInt(data.templateId);
      } else {
        updateData.templateId = null;
      }
    }
    if (data.cancelPending !== undefined) updateData.cancelPending = data.cancelPending;
    if (data.priority !== undefined) updateData.priority = data.priority;
    if (data.enabled !== undefined) updateData.enabled = data.enabled;
    
    return prisma.conditionalEmail.update({
      where: { id: parseInt(id) },
      data: updateData,
      include: { template: true }
    });
  }
  
  /**
   * Delete a conditional email
   */
  async delete(id) {
    return prisma.conditionalEmail.delete({
      where: { id: parseInt(id) }
    });
  }
  
  /**
   * Toggle enabled status
   */
  async toggleEnabled(id) {
    const current = await prisma.conditionalEmail.findUnique({
      where: { id: parseInt(id) }
    });
    
    if (!current) throw new Error('Conditional email not found');
    
    return prisma.conditionalEmail.update({
      where: { id: parseInt(id) },
      data: { enabled: !current.enabled }
    });
  }
  
  // ============================
  // CONDITIONAL EMAIL JOB CRUD
  // ============================
  
  /**
   * Find a job by conditional email ID and lead ID
   */
  async findJob(conditionalEmailId, leadId) {
    return prisma.conditionalEmailJob.findUnique({
      where: {
        conditionalEmailId_leadId: {
          conditionalEmailId: parseInt(conditionalEmailId),
          leadId: parseInt(leadId)
        }
      },
      include: { conditionalEmail: true }
    });
  }
  
  /**
   * Create a conditional email job
   */
  async createJob(data) {
    return prisma.conditionalEmailJob.create({
      data: {
        conditionalEmailId: parseInt(data.conditionalEmailId),
        leadId: parseInt(data.leadId),
        status: data.status || 'pending',
        scheduledFor: data.scheduledFor || null,
        triggeredByEvent: data.triggeredByEvent || null,
        triggeredByJobId: data.triggeredByJobId ? parseInt(data.triggeredByJobId) : null,
        cancelledFollowups: data.cancelledFollowups || null,
        emailJobId: data.emailJobId ? parseInt(data.emailJobId) : null
      },
      include: { conditionalEmail: true }
    });
  }
  
  /**
   * Update a conditional email job
   */
  async updateJob(id, data) {
    return prisma.conditionalEmailJob.update({
      where: { id: parseInt(id) },
      data: {
        status: data.status,
        scheduledFor: data.scheduledFor,
        sentAt: data.sentAt,
        brevoMessageId: data.brevoMessageId,
        emailJobId: data.emailJobId,
        lastError: data.lastError,
        cancelledFollowups: data.cancelledFollowups
      }
    });
  }
  
  /**
   * Check if a job already exists for this conditional email + lead
   */
  async jobExists(conditionalEmailId, leadId) {
    const job = await prisma.conditionalEmailJob.findUnique({
      where: {
        conditionalEmailId_leadId: {
          conditionalEmailId: parseInt(conditionalEmailId),
          leadId: parseInt(leadId)
        }
      }
    });
    return !!job;
  }
  
  /**
   * Get pending jobs that are ready to be sent
   */
  async getPendingJobs(limit = 50) {
    return prisma.conditionalEmailJob.findMany({
      where: {
        status: { in: RulebookService.getPendingOnlyStatuses() },
        scheduledFor: { lte: new Date() }
      },
      include: {
        conditionalEmail: {
          include: { template: true }
        },
        lead: true
      },
      take: limit,
      orderBy: [
        { conditionalEmail: { priority: 'desc' } },
        { scheduledFor: 'asc' }
      ]
    });
  }
  
  /**
   * Get jobs for a specific lead
   */
  async getJobsForLead(leadId) {
    return prisma.conditionalEmailJob.findMany({
      where: { leadId: parseInt(leadId) },
      include: {
        conditionalEmail: {
          include: { template: true }
        }
      },
      orderBy: { createdAt: 'desc' }
    });
  }
}

module.exports = new ConditionalEmailRepository();
