// repositories/TemplateRepository.js
// Data access layer for EmailTemplate operations
// OPTIMIZED: Uses Redis caching with 30 min TTL

const { prisma } = require('../lib/prisma');
const { cache } = require('../lib/cache');

// Cache TTL: 30 minutes for templates (rarely change)
const TEMPLATE_CACHE_TTL = 1800;

class TemplateRepository {

  /**
   * Find template by ID
   * Note: Individual templates not cached (less frequent reads)
   */
  async findById(id) {
    return prisma.emailTemplate.findUnique({
      where: { id: parseInt(id) }
    });
  }

  /**
   * Get all templates (CACHED - 30 min TTL)
   */
  async findAll() {
    // Try cache first
    const cacheKey = 'all';
    const cached = await cache.get('templates', cacheKey);
    if (cached) {
      console.log('[TemplateRepository] Cache HIT for templates');
      return cached;
    }
    
    console.log('[TemplateRepository] Cache MISS - querying database');
    const templates = await prisma.emailTemplate.findMany({
      orderBy: { createdAt: 'desc' }
    });
    
    // Cache the result
    await cache.set('templates', cacheKey, templates, TEMPLATE_CACHE_TTL);
    return templates;
  }

  /**
   * Invalidate template cache
   */
  async invalidateCache() {
    await cache.del('templates', 'all');
    console.log('[TemplateRepository] Template cache invalidated');
  }

  /**
   * Create a new template
   */
  async create(data) {
    const template = await prisma.emailTemplate.create({
      data: {
        name: data.name,
        subject: data.subject,
        body: data.body,
        variables: data.variables || [],
        isDefault: data.isDefault || false
      }
    });
    
    // Invalidate cache after mutation
    await this.invalidateCache();
    return template;
  }

  /**
   * Update a template
   */
  async update(id, data) {
    const template = await prisma.emailTemplate.update({
      where: { id: parseInt(id) },
      data: {
        ...data,
        updatedAt: new Date()
      }
    });
    
    // Invalidate cache after mutation
    await this.invalidateCache();
    return template;
  }

  /**
   * Delete a template
   */
  async delete(id) {
    const result = await prisma.emailTemplate.delete({
      where: { id: parseInt(id) }
    });
    
    // Invalidate cache after mutation
    await this.invalidateCache();
    return result;
  }

  /**
   * Get default template
   */
  async getDefault() {
    return prisma.emailTemplate.findFirst({
      where: { isDefault: true }
    });
  }

  /**
   * Set template as default
   */
  async setDefault(id) {
    // Remove default from all others
    await prisma.emailTemplate.updateMany({
      where: { isDefault: true },
      data: { isDefault: false }
    });

    // Set this one as default
    const template = await prisma.emailTemplate.update({
      where: { id: parseInt(id) },
      data: { isDefault: true }
    });
    
    // Invalidate cache after mutation
    await this.invalidateCache();
    return template;
  }
}

module.exports = new TemplateRepository();

