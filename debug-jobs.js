const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();

async function main() {
  const jobs = await prisma.emailJob.findMany({
    where: { leadId: 105 },
    orderBy: { id: 'asc' },
    select: { id: true, type: true, status: true, scheduledFor: true }
  });
  
  console.log('Total jobs for lead 105:', jobs.length);
  console.log('');
  console.log('ID\tTYPE\t\t\tSTATUS\t\tSCHEDULED');
  console.log('---------------------------------------------------');
  
  for (const j of jobs) {
    const sched = j.scheduledFor ? j.scheduledFor.toISOString().slice(0, 16) : 'N/A';
    console.log(`${j.id}\t${j.type.padEnd(16)}\t${j.status.padEnd(12)}\t${sched}`);
  }
}

main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
