// controllers/TerminalStatesController.js
// API for Terminal States page - Dead, Unsubscribed, Complaint leads

const { prisma } = require('../lib/prisma');
const RulebookService = require('../services/RulebookService');

class TerminalStatesController {
  
  /**
   * Get leads by terminal state (dead, unsubscribed, complaint)
   * GET /api/terminal-states?state=dead&page=1&limit=20
   */
  async getLeadsByState(req, res) {
    try {
      const { state, page = 1, limit = 20, search } = req.query;
      const pageNum = parseInt(page);
      const limitNum = parseInt(limit);
      
      // Validate state
      const validStates = RulebookService.getTerminalStates();
      if (!validStates.includes(state)) {
        return res.status(400).json({ 
          error: `Invalid state. Must be one of: ${validStates.join(', ')}` 
        });
      }
      
      const whereClause = { terminalState: state };
      
      // Optional search filter
      if (search) {
        whereClause.OR = [
          { email: { contains: search, mode: 'insensitive' } },
          { name: { contains: search, mode: 'insensitive' } }
        ];
      }
      
      const [leads, total] = await Promise.all([
        prisma.lead.findMany({
          where: whereClause,
          orderBy: { terminalStateAt: 'desc' },
          skip: (pageNum - 1) * limitNum,
          take: limitNum,
          include: {
            emailJobs: {
              orderBy: { createdAt: 'desc' },
              take: 1,
              select: {
                id: true,
                type: true,
                status: true,
                lastError: true,
                retryCount: true,
                scheduledFor: true,
                sentAt: true
              }
            }
          }
        }),
        prisma.lead.count({ where: whereClause })
      ]);
      
      return res.json({ 
        leads, 
        total, 
        page: pageNum, 
        pages: Math.ceil(total / limitNum),
        state
      });
    } catch (error) {
      console.error('[TerminalStatesController] getLeadsByState error:', error);
      return res.status(500).json({ error: 'Failed to fetch terminal state leads' });
    }
  }
  
  /**
   * Get stats for all terminal states
   * GET /api/terminal-states/stats
   */
  async getStats(req, res) {
    try {
      const [dead, unsubscribed, complaint] = await Promise.all([
        prisma.lead.count({ where: { terminalState: 'dead' } }),
        prisma.lead.count({ where: { terminalState: 'unsubscribed' } }),
        prisma.lead.count({ where: { terminalState: 'complaint' } })
      ]);
      
      // Also get recent terminal state changes
      const recentChanges = await prisma.lead.findMany({
        where: {
          terminalState: { not: null }
        },
        orderBy: { terminalStateAt: 'desc' },
        take: 10,
        select: {
          id: true,
          email: true,
          name: true,
          terminalState: true,
          terminalStateAt: true,
          terminalReason: true
        }
      });
      
      return res.json({
        counts: { dead, unsubscribed, complaint },
        total: dead + unsubscribed + complaint,
        recentChanges
      });
    } catch (error) {
      console.error('[TerminalStatesController] getStats error:', error);
      return res.status(500).json({ error: 'Failed to fetch terminal state stats' });
    }
  }
  
  /**
   * Resurrect a dead lead (allow retrying after fixing issue)
   * POST /api/terminal-states/:id/resurrect
   */
  async resurrect(req, res) {
    try {
      const { id } = req.params;
      const { reason } = req.body;
      
      const lead = await prisma.lead.findUnique({
        where: { id: parseInt(id) }
      });
      
      if (!lead) {
        return res.status(404).json({ error: 'Lead not found' });
      }
      
      if (lead.terminalState !== 'dead') {
        return res.status(400).json({ 
          error: `Cannot resurrect lead with terminal state: ${lead.terminalState}. Only 'dead' leads can be resurrected.` 
        });
      }
      
      // Clear terminal state
      await prisma.lead.update({
        where: { id: parseInt(id) },
        data: {
          terminalState: null,
          terminalStateAt: null,
          terminalReason: null,
          status: 'idle',
          totalRetries: 0  // Reset retry count
        }
      });
      
      // Add event to history
      await prisma.eventHistory.create({
        data: {
          leadId: parseInt(id),
          event: 'resurrected',
          timestamp: new Date(),
          details: {
            previousState: 'dead',
            reason: reason || 'Manually resurrected by user',
            source: 'TerminalStatesController'
          }
        }
      });
      
      console.log(`[TerminalStatesController] Lead ${id} resurrected from dead state`);
      
      return res.json({ 
        success: true, 
        message: 'Lead resurrected successfully',
        leadId: parseInt(id)
      });
    } catch (error) {
      console.error('[TerminalStatesController] resurrect error:', error);
      return res.status(500).json({ error: 'Failed to resurrect lead' });
    }
  }
  
  /**
   * Get details for a specific terminal state lead
   * GET /api/terminal-states/:id
   */
  async getLeadDetails(req, res) {
    try {
      const { id } = req.params;
      
      const lead = await prisma.lead.findUnique({
        where: { id: parseInt(id) },
        include: {
          emailJobs: {
            orderBy: { createdAt: 'desc' },
            take: 20
          },
          eventHistory: {
            orderBy: { timestamp: 'desc' },
            take: 20
          }
        }
      });
      
      if (!lead) {
        return res.status(404).json({ error: 'Lead not found' });
      }
      
      return res.json(lead);
    } catch (error) {
      console.error('[TerminalStatesController] getLeadDetails error:', error);
      return res.status(500).json({ error: 'Failed to fetch lead details' });
    }
  }
}

module.exports = new TerminalStatesController();
