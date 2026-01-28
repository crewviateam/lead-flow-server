module.exports = {
  apps: [{
    name: 'leadflow-backend',
    script: './app.js',
    instances: 1,
    exec_mode: 'fork', // Monolith mode (CronService not designed for cluster mode)
    watch: false,
    max_memory_restart: '1G',
    env: {
      NODE_ENV: 'production',
      PORT: 3000
    },
    log_date_format: 'YYYY-MM-DD HH:mm Z',
    merge_logs: true
  }]
};
