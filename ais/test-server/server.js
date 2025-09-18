// server.js
const net = require('net');
const fs = require('fs');
const readline = require('AIS-nmea-sample.txt');

// --- Configuration ---
const HOST = '0.0.0.0';
const PORT = 10110;
const FILENAME = 'nmea.txt';
const DELAY_MS = 1000;

// 📍--- NEW: Your simulated location and vessel data ---
const myVessel = {
    // Starting position (Kyiv, for example)
    lat: 50.4501,
    lon: 30.5234,
    speed: 10.5, // Speed in knots
    course: 45.0, // Course in degrees true
};

// ⚙️ --- NEW: Function to calculate the NMEA checksum ---
function calculateChecksum(sentence) {
    // The checksum is a simple XOR of all characters between '$' and '*'
    let checksum = 0;
    for (let i = 1; i < sentence.length; i++) {
        checksum ^= sentence.charCodeAt(i);
    }
    // Convert to a 2-digit hexadecimal string
    return checksum.toString(16).toUpperCase().padStart(2, '0');
}

// ⚙️ --- NEW: Function to generate a $GPRMC sentence ---
function generateGPRMC(vesselData) {
    const now = new Date();

    // Format time: HHMMSS.ss
    const time = now.getUTCHours().toString().padStart(2, '0') +
                 now.getUTCMinutes().toString().padStart(2, '0') +
                 now.getUTCSeconds().toString().padStart(2, '0') + '.' +
                 now.getUTCMilliseconds().toString().padStart(3, '0').substring(0, 2);

    // Format latitude: DDMM.MMMM,N/S
    const latDeg = Math.floor(Math.abs(vesselData.lat));
    const latMin = (Math.abs(vesselData.lat) - latDeg) * 60;
    const latitude = latDeg.toString().padStart(2, '0') +
                     latMin.toFixed(4).padStart(7, '0') +
                     ',' + (vesselData.lat >= 0 ? 'N' : 'S');

    // Format longitude: DDDMM.MMMM,E/W
    const lonDeg = Math.floor(Math.abs(vesselData.lon));
    const lonMin = (Math.abs(vesselData.lon) - lonDeg) * 60;
    const longitude = lonDeg.toString().padStart(3, '0') +
                      lonMin.toFixed(4).padStart(7, '0') +
                      ',' + (vesselData.lon >= 0 ? 'E' : 'W');
    
    // Format speed (knots) and course (degrees)
    const speed = vesselData.speed.toFixed(2);
    const course = vesselData.course.toFixed(2);

    // Format date: DDMMYY
    const date = now.getUTCDate().toString().padStart(2, '0') +
                 (now.getUTCMonth() + 1).toString().padStart(2, '0') +
                 now.getUTCFullYear().toString().substring(2);

    // Assemble the sentence without the checksum
    const sentenceWithoutChecksum = `$GPRMC,${time},A,${latitude},${longitude},${speed},${course},${date},,,A`;

    // Calculate and append the checksum
    const checksum = calculateChecksum(sentenceWithoutChecksum);
    return sentenceWithoutChecksum + '*' + checksum;
}


// A helper function for creating a delay
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const server = net.createServer(async (socket) => {
    const clientAddress = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`📡 Client connected: ${clientAddress}`);

    let aisLineCounter = 0;

    try {
        while (socket.writable) {
            const fileStream = fs.createReadStream(FILENAME);
            const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

            for await (const line of rl) {
                // Send one line of AIS data from the file
                socket.write(line + '\r\n');
                console.log(`Sent AIS to ${clientAddress}: ${line}`);
                await sleep(DELAY_MS);
                
                aisLineCounter++;

                // --- NEW: Inject "My Location" every 5 AIS messages ---
                if (aisLineCounter % 5 === 0) {
                    // To make it move, slightly update the longitude each time
                    myVessel.lon += 0.0005; 
                    
                    const myLocationSentence = generateGPRMC(myVessel);
                    socket.write(myLocationSentence + '\r\n');
                    console.log(`📍 Sent My Location: ${myLocationSentence}`);
                    await sleep(DELAY_MS); // Add an extra delay for our own position
                }
            }
            console.log(`Finished sending file to ${clientAddress}. Restarting loop.`);
        }
    } catch (error) {
        console.error(`Error with client ${clientAddress}:`, error.message);
    }

    socket.on('close', () => console.log(`Client disconnected: ${clientAddress}`));
    socket.on('error', (err) => console.log(`Socket error with ${clientAddress}: ${err.message}`));
});

server.listen(PORT, HOST, () => {
    console.log(`✅ NMEA server started with pm2, listening on ${HOST}:${PORT}`);
});
