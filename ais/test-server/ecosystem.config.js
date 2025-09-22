// ecosystem.config.js
module.exports = {
  apps : [{
    name   : "nmea-server", // The name for your process
    script : "server.js",   // The script to run
    
    // Optional: Advanced settings you might want later
    watch  : false,         // Disable watching for file changes
    autorestart: true,      // Automatically restart if it crashes
  }]
}
