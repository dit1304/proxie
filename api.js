const tls = require('tls');
const cluster = require('cluster');
const os = require('os');
const fs = require('fs');
const net = require('net');
if (cluster.isPrimary) {
    let completedWorkers = 0;
    let numCPUs;
    (async () => {
        try {
            const myip = (await (await fetch('https://myip.bexcode.us.to')).json()).myip;
            const prx = fs.readFileSync('proxy.txt', 'utf-8').split('\n').map(line => line.trim()).filter(Boolean);
            const allIPs = [...new Set(prx)];
            numCPUs = Math.min(os.cpus().length, allIPs.length);
            const proxyPerThreads = Math.ceil(allIPs.length / numCPUs);
            console.log(`Total IPs: ${allIPs.length}`);
            for (let i = 0; i < numCPUs; i++) {
                const startIndex = i * proxyPerThreads;
                if (startIndex >= allIPs.length) break;
                const workerProxy = allIPs.slice(startIndex, startIndex + proxyPerThreads);
                const worker = cluster.fork();
                worker.send({ ips: workerProxy, numCPUs, myip });
            }
        } catch (error) {
            console.error('Error fetching or processing proxy data:', error.message);
            process.exit(1);
        }
    })();

    cluster.on('exit', (worker, code, signal) => {
        completedWorkers++;
        if (completedWorkers === numCPUs) {
            console.log('selesai');
            process.exit(0);
        }
    });
} else {
    let assignedIPs = [];
    let openPorts = []
    let totalCore;
    let myips;
    process.on('message', (msg) => {
        if (msg.ips) {
            assignedIPs = msg.ips;
            totalCore = msg.numCPUs;
            myips = msg.myip;
            scanProxies('tcp').then(() => {
            scanProxies().then(() => {
                process.exit(0);
            }).catch(error => {
                console.error('Scan error:', error);
                process.exit(1);
            });
            }).catch(error => {
                console.error('Scan error:', error);
                process.exit(1);
            });
        }
    });

    async function sendRequest(host, port, targetHost, path, retryCount = 0) {
        return new Promise((resolve, reject) => {
            if (!host && !port) reject(new Error('Mana Host Dan Portnya'));
            const socket = tls.connect({
                host: host,
                port: parseInt(port),
                servername: targetHost,
            }, () => {
                const request = `GET ${path} HTTP/1.1\r\n` +
                    `Host: ${targetHost}\r\n` +
                    `User-Agent: Mozilla/5.0\r\n` +
                    `Connection: close\r\n\r\n`;
                socket.write(request);
            });

            let responseBody = '';
            socket.on('data', (data) => {
                responseBody += data.toString();
            });

            socket.on('end', () => {
                try {
                    const body = responseBody.split('\r\n\r\n')[1] || '';
                    resolve(body);
                } catch (error) {
                    reject(new Error(error));
                }
            });

            socket.on('error', (error) => {
                reject(new Error(error));
            });

            socket.setTimeout(5000, () => {
                socket.destroy();
                reject(new Error('Request timeout after'));
            });
        });
    }

    async function checkIP(proxy) {
        const [host, port] = proxy.split(/[^a-zA-Z0-9.\n]+/);
        
        return new Promise(async (resolve) => {
            try {
                const ipinfo = await sendRequest(host, port, 'myip.bexcode.us.to', '/');
                if (!ipinfo) return resolve({ proxyip: false });
                
                const ipingfo = JSON.parse(ipinfo.trim());
                if (ipingfo.myip && ipingfo.myip !== myips) {
                    const { myip, ...ipinfoh } = ipingfo;
                    resolve({
                        proxy: host,
                        port: port,
                        proxyip: true,
                        ip: myip,
                        ...ipinfoh
                    });
                } else {
                    resolve({ 
                        proxy: host,
                        port: port,
                        proxyip: false 
                    });
                }
            } catch (error) {
                resolve({
                    msg: error.message,
                    proxy: host,
                    port: port,
                    proxyip: false
                });
            }
        });
    }
function checkPort(ip) {
  return new Promise((resolve) => {
    const [host, port] = ip.split(/[^a-zA-Z0-9.\n]+/);
    const socket = new net.Socket(); 
    socket.setTimeout(500);
    socket.on('connect', () => {
        socket.destroy();
        resolve(ip);
    });
    
    socket.on('timeout', () => {
        socket.destroy();
        resolve(null);
    });
    
    socket.on('error', (r) => {
        resolve(null);
    });
    
    socket.connect(port, host);
  });
}
async function scanProxies(ask) {
    const promises = [];
    const batchSize = ask === 'tcp' ? 500 : 50;
    if (ask === 'tcp') {
        for (const ip of assignedIPs) {
                promises.push(
                    checkPort(`${ip}`).then(data => {
                        if (data) {
                            openPorts.push(data);
                        }
                    })
                );

                if (promises.length >= batchSize) {
                    await Promise.all(promises);
                    promises.length = 0;
                }
        }
    } else {
        for (const proxy of openPorts) {
            promises.push(
                checkIP(`${proxy}`).then(data => {
                    if (data.proxyip) {
                    console.log(data);
                        fs.appendFileSync('output.txt', `${data.proxy},${data.port},${data.countryCode},${data.org?.replace(/[.,]/g, "")}\n`);
                    }
                })
            );

            if (promises.length >= batchSize) {
                await Promise.all(promises);
                promises.length = 0;
            }
        }
    }
    if (promises.length) {
        await Promise.all(promises);
    }
}
}
