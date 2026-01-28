const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();

async function main() {
  console.log('Backfilling categories...');
  
  // 1. Initial
  const initial = await prisma.emailJob.updateMany({
    where: {
      type: { contains: 'initial', mode: 'insensitive' }
    },
    data: { category: 'initial' }
  });
  console.log(`Updated ${initial.count} initial emails`);

  // 2. Conditional
  const conditional = await prisma.emailJob.updateMany({
    where: {
      type: { contains: 'conditional', mode: 'insensitive' }
    },
    data: { category: 'conditional' }
  });
  console.log(`Updated ${conditional.count} conditional emails`);

  // 3. Manual
  // Logic: Type implies manual, or metadata has manual flag
  const manual = await prisma.emailJob.updateMany({
    where: {
      OR: [
        { type: { equals: 'manual', mode: 'insensitive' } },
        { metadata: { path: ['manual'], equals: true } },
        { metadata: { path: ['manual'], equals: 'true' } }
      ]
    },
    data: { category: 'manual' }
  });
  console.log(`Updated ${manual.count} manual emails`);
  
  // 4. Followup (Default)
  // Everything else remains 'followup', which is correct for "First Followup", etc.
  
  console.log('Done.');
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
