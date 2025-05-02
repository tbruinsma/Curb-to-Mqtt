const axios = require('axios');
const fs = require('fs');
const io = require('socket.io-client');
const mqtt = require('mqtt');
const yaml = require('js-yaml');

// Load configuration from config.yaml file
const config = yaml.load(fs.readFileSync('config.yaml', 'utf8'));

// Extracting values from the config object
const { 
    TOKEN_URL, CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD, AUDIENCE, 
    MQTT_BROKER_URL, MQTT_TOPIC, MQTT_USERNAME, MQTT_PASSWORD, DEBUG, 
    HACONFIG, PERSIST_TOKEN, PERSIST_LAST_TOKEN, PERSIST_LAST_REFRESH
} = config;

// Debug logging function
function debugLog(...args) {
    if (DEBUG) {
        console.log('[DEBUG]', ...args);
    }
}

function dateAdd(interval, units, date=null) {
    //if no date is past, seed it as current date/time
    if(!date)
        date = new Date();

    const newDate = new Date(date);

    switch (interval.toLowerCase()) {
        case 'day':
            newDate.setDate(date.getDate() + units);
            break;
        case 'week':
        newDate.setDate(date.getDate() + 7 * units);
            break;
        case 'month':
            newDate.setMonth(date.getMonth() + units);
            break;
        case 'year':
            newDate.setFullYear(date.getFullYear() + units);
            break;
        case 'hour':
            newDate.setHours(date.getHours() + units);
            break;
        case 'minute':
            newDate.setMinutes(date.getMinutes() + units);
            break;
        case 'second':
            newDate.setSeconds(date.getSeconds() + units);
            break;
        default:
        throw new Error('Invalid interval: ' + interval);
    }
    return newDate;
}

// Function to fetch a new access token
async function fetchUserAccessToken(force=false) {
    try {

        if(!force && PERSIST_TOKEN){
            let lastRefresh = new Date();

            if(!PERSIST_LAST_REFRESH)
                lastRefresh = new Date(PERSIST_LAST_REFRESH);

            let lastToken = PERSIST_LAST_TOKEN;

            if(lastToken.length==0 || lastRefresh < dateAdd('hour', -5)){
                //force a refresh
                force=true;
            }
            else{
                return lastToken;
            }
        }else{
            force=true;
        }

        if(force){
            const response = await axios.post(TOKEN_URL, {
                grant_type: 'password',
                audience: AUDIENCE,
                username: USERNAME,
                password: PASSWORD,
                client_id: CLIENT_ID,
                client_secret: CLIENT_SECRET
            }, {
                headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' }
            });

            if(PERSIST_TOKEN){
                let doc = yaml.safeLoad(fs.readFileSync('./config.yaml', 'utf8'));
                doc.PERSIST_LAST_TOKEN = response.data.access_token;
                doc.PERSIST_LAST_REFRESH = (new Date()).now;
                fs.writeFile('./config.yaml', yaml.safeDump(doc), (err) => {
                    if (err) {
                        console.log(err);
                    }
                });
            }
        }
        

        debugLog('Access token fetched successfully.');
        return response.data.access_token;
    } catch (error) {
        console.error('Error fetching access token:', error.message);
        throw error;
    }
}



// Function to fetch the latest readings
async function fetchLatest(locationId, accessToken) {
    try {
         debugLog('fetch latest readings:', locationId);
		const response = await axios.get(`https://app.energycurb.com/api/latest/${locationId}`, {
            headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' }
        });

        if (response.data && response.data.circuits.length > 0) {
            debugLog('Latest readings circuit count:', response.data.circuits.length);
            return response.data;
        } else {
            console.error('No readings found.');
            throw new Error('No readings found');
        }
    } catch (error) {
        console.error('Error fetching Location Config:', error.message);
        throw error;
    }
}

// Function to fetch the location ID
async function fetchLocationId(accessToken) {
    try {
        const response = await axios.get('https://app.energycurb.com/api/v3/locations', {
            headers: { 'Authorization': `Bearer ${accessToken}` }
        });

        if (response.data && response.data.length > 0) {
            debugLog('Location ID fetched:', response.data[0].id);
            return response.data[0].id;
        } else {
            console.error('No locations found.');
            throw new Error('No locations found');
        }
    } catch (error) {
        console.error('Error fetching location ID:', error.message);
        throw error;
    }
}


// Function to connect to Curb WebSocket and MQTT
async function connectToLiveData() {
    try {
        let USER_ACCESS_TOKEN = await fetchUserAccessToken();
        let LOCATION_ID = await fetchLocationId(USER_ACCESS_TOKEN);

        debugLog('Connecting to WebSocket with location ID:', LOCATION_ID);

        const socket = io('https://app.energycurb.com/api/circuit-data', {
            transports: ['websocket'],
            query: { token: USER_ACCESS_TOKEN }
        });

        const mqttOptions = {};
        if (MQTT_USERNAME) mqttOptions.username = MQTT_USERNAME;
        if (MQTT_PASSWORD) mqttOptions.password = MQTT_PASSWORD;

        const mqttClient = mqtt.connect(MQTT_BROKER_URL, mqttOptions);

        mqttClient.on('connect', () => debugLog('Connected to MQTT broker.'));
        mqttClient.on('error', (err) => console.error('MQTT Error:', err));

        socket.on('connect', () => {
            debugLog('Connected to Curb WebSocket.');
            socket.emit('authenticate', { token: USER_ACCESS_TOKEN });
        });
		
		//Publish Home Assistant auto discovery
		if(HACONFIG){
			debugLog('Publish HA Config to MQTT.');
			const latest = await fetchLatest(LOCATION_ID, USER_ACCESS_TOKEN);
			
            latest.circuits.forEach(circuit => {
					const payload = {
						device: {
                            ids: [
                                'curb_energy'
                            ], 
                            mdl: 'Curb Energy',
                            mf: 'Curb Energy',
                            name: 'Curb Energy'
                        } ,
						device_class: 'power',
						name: circuit.label,
						state_class: 'measurement',
						state_topic: `${MQTT_TOPIC}/${circuit.id}/state`,
						uniq_id: `${circuit.circuit_type}_${circuit.id}`,
						unit_of_measurement: 'W'
					};
					const topic = `homeassistant/sensor/${circuit.id}/config`;
					mqttClient.publish(topic, JSON.stringify(payload));
					//debugLog('[MQTT]', topic, payload);
				});
		}


        socket.on('authorized', () => {
            debugLog('WebSocket authentication successful.');
            socket.emit('subscribe', LOCATION_ID);
        });

        socket.on('unauthorized', (err) => console.error('Authentication failed:', err));

        socket.on('disconnect', () => {
            debugLog('Disconnected from Curb WebSocket. Reconnecting in 5 seconds...');
            setTimeout(connectToLiveData, 5000);
        });

        socket.on('error', (error) => console.error('Socket error:', error));

        socket.on('data', (data) => {
            debugLog('Received data from WebSocket.');
            data.circuits.forEach(circuit => {
                // const payload = {
                //     id: circuit.id,
                //     label: circuit.label,
                //     power: circuit.w,
                //     type: circuit.circuit_type
                // };
                const topic = `${MQTT_TOPIC}/${circuit.id}/state`;
                //mqttClient.publish(topic, JSON.stringify(payload));
                mqttClient.publish(topic, circuit.w.toString(), (err) => {
                    if (err) {
                      console.error('Error publishing message:', err);
                    } else {
                      console.log('Message published successfully');
                    }});
                //debugLog('Published to MQTT:', topic, payload);
            });
        });
        
        // Periodically refresh token every 12 hours
        setInterval(async () => {
            try {
                const newToken = await fetchUserAccessToken(true);
                if (newToken !== USER_ACCESS_TOKEN) {
                    USER_ACCESS_TOKEN = newToken;
                    socket.emit('authenticate', { token: USER_ACCESS_TOKEN });
                    debugLog('Access token refreshed and re-authenticated.');
                }
            } catch (error) {
                console.error('Error refreshing token:', error.message);
            }
        }, 43200000); // 12 hours
        
    } catch (error) {
        console.error('Error during setup:', error.message);
    }
}


connectToLiveData();
