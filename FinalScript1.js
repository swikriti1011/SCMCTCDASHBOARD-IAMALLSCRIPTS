const mqtt = require('mqtt')
const fs = require("fs");
const http = require('http');
const request = require('request');

const timeout = ms => new Promise(res => setTimeout(res, ms))

var SENSOR_ALTERNATE_ID = '3562757';
var CAPABILITY_ALTERNATE_ID = '734264';
var DEVICE_ALTERNATE_ID = '3245754';
var HOST_ADDRESS = '58a1ef3d-f554-4c32-83ab-6753d7bdf108.eu10.cp.iot.sap'

var rn = require('random-number');
var DP_Oil_Seperator_1; 
// rn.generator({
//       min:  0
//     , max:  100
//     , integer: true
// })

// Change this value to adjust the water consumption
//dpoilseperator_val = 21;

//var DP_Oil_Seperator_1 = rn.generator({
  //    min:  0.20
 //   , max:  0.80
 //   , float: true
//})


var urlget = " https://europe-global-account-sapinnovation-accenture-com-dev-d7cc28886.cfapps.eu10.hana.ondemand.com/catalog/sensor_data?$filter=sensor_id%20eq%20%27" + SENSOR_ALTERNATE_ID + "%27";
var urlcreate= " https://europe-global-account-sapinnovation-accenture-com-dev-d7cc28886.cfapps.eu10.hana.ondemand.com/catalog/sensor_data";
var urlupdate="https://europe-global-account-sapinnovation-accenture-com-dev-d7cc28886.cfapps.eu10.hana.ondemand.com/catalog/sensor_data/" + SENSOR_ALTERNATE_ID + "";
var min=60;
var max=70;
var threshold=74;


const CERTIFICATE_FILE = './Machine1-device_certificate.pem';
const PASSPHRASE_FILE = './Machine1-device_key.txt';
var mqttClient;

//Connect to MQTT
function ConnectMQTTChannel(_SENSOR_ALTERNATE_ID,_CAPABILITY_ALTERNATE_ID,_DEVICE_ALTERNATE_ID){

    SENSOR_ALTERNATE_ID     = _SENSOR_ALTERNATE_ID;
    CAPABILITY_ALTERNATE_ID = _CAPABILITY_ALTERNATE_ID;
    DEVICE_ALTERNATE_ID     = _DEVICE_ALTERNATE_ID;

   mqttClient = connectToMQTT()

}

//csvfunction
async function sendDataViaMQTT() {
	//var today = new Date().toISOString().slice(0, 10);
	//	var today = new Date();
	// today=((today.getMonth() + 1) + '/' + today.getDate() + '/' + today.getFullYear());
	var today = new Date().toISOString().slice(0, 10);
	while(true){
		//await timeout(3600000)
		await timeout(5000)
 request({

            url: urlget,
            
            headers: {
                "Authorization": "Basic",
                "Content-Type": "application/json"

            }

        }, function (error, response, body) {
            if (!error&& response.statusCode == 200) {
				console.log("fetched get data");

                var sdata = JSON.parse(body)
                
               var len = sdata.value.length;
			   console.log("Sensor record"+len);
			   if(len==0){
			 var today = new Date().toISOString().slice(0, 10);
			console.log("Insert Sensor data");   
			 var entity={
				"sensor_id": SENSOR_ALTERNATE_ID,
            "min_value": 22,
            "max_value": 25,
            "date":today ,
            "thflag": ""
			};
			 request({
                                    url: urlcreate,
                                    method: 'POST',
                                    headers: {
                                          "Authorization": "Basic",
                                        "Content-Type": "application/json"
                                        
                                    },

                                    json: entity
                                }, function (error, response, body) {

                                    // handle response
                                    if (!error && response.statusCode == 201) {

			                               console.log("created");
			                             //  process.exit();
									}

                                        
								});
								
								
					  var payload = {
        sensorAlternateId: SENSOR_ALTERNATE_ID,
        capabilityAlternateId: CAPABILITY_ALTERNATE_ID,
        measures: [

            {"DP_Oil_Seperator_1":min}

           ]
        }
       console.log(payload);
		//console.log(dpoilseperator_val);
        var topicName = 'measures/' + DEVICE_ALTERNATE_ID;
       mqttClient.publish(topicName, JSON.stringify(payload), [], error => {
         if(!error) {
             console.log("Data successfully sent!");
          } else {
              console.log("An unecpected error occurred:", error);
          }
			

        });		
			                               process.exit();		
								

			   }
			   else{
				   

				   
				   
				   
				   
				   
				 console.log("Else block with already created data");
				 var date=sdata.value[0].date;
				 var minval=sdata.value[0].min_value;
				 var maxval=sdata.value[0].max_value;
				 var diffDays = parseInt((today.getTime() - date.getTime()) / (1000 * 60 * 60 * 24)); //gives day difference
                  console.log("Date difference:" +diffDays);
				       if(diffDays<=2 && maxval=="65" )
					   {
						   var DP_Oil_Seperator_1 = rn.generator({
                                 min:  min
                                , max:  65
                                 , integer: true
                                 });
								 
							var payload = {
        sensorAlternateId: SENSOR_ALTERNATE_ID,
        capabilityAlternateId: CAPABILITY_ALTERNATE_ID,
        measures: [

            {"DP_Oil_Seperator_1":DP_Oil_Seperator_1()}

           ]
        }
       console.log(payload);
		//console.log(dpoilseperator_val);
        var topicName = 'measures/' + DEVICE_ALTERNATE_ID;
       mqttClient.publish(topicName, JSON.stringify(payload), [], error => {
         if(!error) {
             console.log("Data successfully sent!");
          } else {
              console.log("An unecpected error occurred:", error);
          }
			

        });				 
								 
			   
					   }
					   else if(diffDays==3&& maxval=="65" )
					   {
						   
						    var DP_Oil_Seperator_1 = rn.generator({
                                 min:  min
                                , max:  68
                                 , integer: true
                                 });
								 
						            var entity={
			//"sensor_id": SENSOR_ALTERNATE_ID,
             "min_value": min,
            "max_value": "68",
            "date":today ,
            "thflag": ""

			};
			          request({
                                    url:urlupdate,
                                    method: 'PUT',
                                    headers: {
                                          "Authorization": "Basic",
                                        "Content-Type": "application/json"
                                       // "X-Requested-With": "XMLHttpRequest",
                                    
                                        
                                    },

                                    json: entity
                                }, function (error, response, body) {
									
                                    if (!error) {

			              console.log("updated");
			                    //process.exit();
									}

                               //}        
								});
		 
								var payload = {
        sensorAlternateId: SENSOR_ALTERNATE_ID,
        capabilityAlternateId: CAPABILITY_ALTERNATE_ID,
        measures: [

            {"DP_Oil_Seperator_1":DP_Oil_Seperator_1()}

           ]
        }
       console.log(payload);
		//console.log(dpoilseperator_val);
        var topicName = 'measures/' + DEVICE_ALTERNATE_ID;
       mqttClient.publish(topicName, JSON.stringify(payload), [], error => {
         if(!error) {
             console.log("Data successfully sent!");
          } else {
              console.log("An unecpected error occurred:", error);
          }
			

        });				 
								  
						 
								 
					   }
				 
				 else if(diffDays<=2 && maxval=="68" )
				 {
					 	    var DP_Oil_Seperator_1 = rn.generator({
                                 min:  min
                                , max:  68
                                 , integer: true
                                 });
								 
					var payload = {
        sensorAlternateId: SENSOR_ALTERNATE_ID,
        capabilityAlternateId: CAPABILITY_ALTERNATE_ID,
        measures: [

            {"DP_Oil_Seperator_1":DP_Oil_Seperator_1()}

           ]
        }
       console.log(payload);
		//console.log(dpoilseperator_val);
        var topicName = 'measures/' + DEVICE_ALTERNATE_ID;
       mqttClient.publish(topicName, JSON.stringify(payload), [], error => {
         if(!error) {
             console.log("Data successfully sent!");
          } else {
              console.log("An unecpected error occurred:", error);
          }
			

        });				 
								 			 
				 
								 
				 }
				 
				 else if(diffDays==3&& maxval=="68" )
				 {
					 	    var DP_Oil_Seperator_1 = rn.generator({
                                 min:  min
                                , max:  70
                                 , integer: true
                                 });
								 
								    var entity={
			//"sensor_id": SENSOR_ALTERNATE_ID,
             "min_value": min,
            "max_value": "70",
            "date":today ,
            "thflag": ""

			};
			          request({
                                    url:urlupdate,
                                    method: 'PUT',
                                    headers: {
                                          "Authorization": "Basic",
                                        "Content-Type": "application/json"
                                       // "X-Requested-With": "XMLHttpRequest",
                                    
                                        
                                    },

                                    json: entity
                                }, function (error, response, body) {
									
                                    if (!error) {

			              console.log("updated");
			                    //process.exit();
									}

                               //}        
								});
								
								
								
								var payload = {
        sensorAlternateId: SENSOR_ALTERNATE_ID,
        capabilityAlternateId: CAPABILITY_ALTERNATE_ID,
        measures: [

            {"DP_Oil_Seperator_1":DP_Oil_Seperator_1()}

           ]
        }
       console.log(payload);
		//console.log(dpoilseperator_val);
        var topicName = 'measures/' + DEVICE_ALTERNATE_ID;
       mqttClient.publish(topicName, JSON.stringify(payload), [], error => {
         if(!error) {
             console.log("Data successfully sent!");
          } else {
              console.log("An unecpected error occurred:", error);
          }
			

        });				 
								 
		 		 
								 
				 }
				 
				  else if(diffDays<=2 && maxval=="70" )
				 {
					 	    var DP_Oil_Seperator_1 = rn.generator({
                                 min:  min
                                , max:  70
                                 , integer: true
                                 });
								 
						var payload = {
        sensorAlternateId: SENSOR_ALTERNATE_ID,
        capabilityAlternateId:[]
=		CAPABILITY_ALTERNATE_ID,
        measures: [

            {"DP_Oil_Seperator_1":DP_Oil_Seperator_1()}

           ]
        }
       console.log(payload);
		//console.log(dpoilseperator_val);
        var topicName = 'measures/' + DEVICE_ALTERNATE_ID;
       mqttClient.publish(topicName, JSON.stringify(payload), [], error => {
         if(!error) {
             console.log("Data successfully sent!");
          } else {
              console.log("An unecpected error occurred:", error);
          }
			

        });				 
								 		 

								 
				 }
				 
				 
							  else if(diffDays==3 && maxval=="70" )
				 {
					 	    var DP_Oil_Seperator_1 = rn.generator({
                                 min:  71
                                , max:  74
                                 , integer: true
                                 });
								 
								    var entity={
			//"sensor_id": SENSOR_ALTERNATE_ID,
             "min_value": "71",
            "max_value": "74",
            "date":today ,
            "thflag": "X"

			};
			          request({
                                    url:urlupdate,
                                    method: 'PUT',
                                    headers: {
                                          "Authorization": "Basic",
                                        "Content-Type": "application/json"
                                       // "X-Requested-With": "XMLHttpRequest",
                                    
                                        
                                    },

                                    json: entity
                                }, function (error, response, body) {
									
                                    if (!error) {

			              console.log("updated");
			                    //process.exit();
									}

                               //}        
								});
									 
						 
						var payload = {
        sensorAlternateId: SENSOR_ALTERNATE_ID,
        capabilityAlternateId: CAPABILITY_ALTERNATE_ID,
        measures: [

            {"DP_Oil_Seperator_1":DP_Oil_Seperator_1()}

           ]
        }
       console.log(payload);
		//console.log(dpoilseperator_val);
        var topicName = 'measures/' + DEVICE_ALTERNATE_ID;
       mqttClient.publish(topicName, JSON.stringify(payload), [], error => {
         if(!error) {
             console.log("Data successfully sent!");
          } else {
              console.log("An unecpected error occurred:", error);
          }
			

        });				 
								 		 

								 
				 }


			  else if(diffDays==3 && maxval=="74" )
				 {
					 	    var DP_Oil_Seperator_1 = rn.generator({
                                 min:  71
                                , max:  74
                                 , integer: true
                                 });
								 

									 
						 
						var payload = {
        sensorAlternateId: SENSOR_ALTERNATE_ID,
        capabilityAlternateId: CAPABILITY_ALTERNATE_ID,
        measures: [

            {"DP_Oil_Seperator_1":DP_Oil_Seperator_1()}

           ]
        }
       console.log(payload);
		//console.log(dpoilseperator_val);
        var topicName = 'measures/' + DEVICE_ALTERNATE_ID;
       mqttClient.publish(topicName, JSON.stringify(payload), [], error => {
         if(!error) {
             console.log("Data successfully sent!");
          } else {
              console.log("An unecpected error occurred:", error);
          }
			

        });				 
								 		 
	}



							  else if(diffDays==1&& maxval=="74" )
				 {	 
                          console.log("Running Maintenance Activity");
								 
				 }
				 
				 else if(diffDays==2 && maxval=="74"){
					 
				 var DP_Oil_Seperator_1 = rn.generator({
                                 min:  60
                                , max:  65
                                 , integer: true
                                 });
								 
								    var entity={
			//"sensor_id": SENSOR_ALTERNATE_ID,
             "min_value": "60",
            "max_value": "65",
            "date":today ,
            "thflag": ""

			};
			          request({
                                    url:urlupdate,
                                    method: 'PUT',
                                    headers: {
                                          "Authorization": "Basic",
                                        "Content-Type": "application/json"
                                       // "X-Requested-With": "XMLHttpRequest",
                                    
                                        
                                    },

                                    json: entity
                                }, function (error, response, body) {
									
                                    if (!error) {

			              console.log("updated");
			                    //process.exit();
									}

                               //}        
								});
									 
						 
						var payload = {
        sensorAlternateId: SENSOR_ALTERNATE_ID,
        capabilityAlternateId: CAPABILITY_ALTERNATE_ID,
        measures: [

            {"DP_Oil_Seperator_1":DP_Oil_Seperator_1()}

           ]
        }
       console.log(payload);
		//console.log(dpoilseperator_val);
        var topicName = 'measures/' + DEVICE_ALTERNATE_ID;
       mqttClient.publish(topicName, JSON.stringify(payload), [], error => {
         if(!error) {
             console.log("Data successfully sent!");
          } else {
              console.log("An unecpected error occurred:", error);
          }
			

        });				 
								 		 
	 
					 
					 
					 
					 
					 
					 
				 }
				 




				 
			   }// end of length else block
			   
				   
			  


            }


            
		

        }); // end of read request
	
	
	
	
	}// while closing 
	

	
	
	
	
	
	///////////////////////////New code end//////////////////////////////////////////////////////////////////////////////////
	

   
}

function connectToMQTT() {
    var options = {
        keepalive: 10,
        clientId: DEVICE_ALTERNATE_ID,
        clean: true,
       reconnectPeriod: 2000,
        connectTimeout: 2000,
        cert: fs.readFileSync('./Machine1-device_certificate.pem'),
        key: fs.readFileSync('./Machine1-device_certificate.pem'),
        passphrase: fs.readFileSync('./Machine1-device_key.txt').toString(),  
        rejectUnauthorized: false
    };

    var mqttClient = mqtt.connect(`mqtts://${HOST_ADDRESS}:8883`, options);

    mqttClient.subscribe('ack/' + DEVICE_ALTERNATE_ID);
    mqttClient.on('connect', () => {console.log("Connection established!")
       sendDataViaMQTT();
	 
	  
    });
    mqttClient.on("error", err => console.log("Unexpected error occurred:", err));
   mqttClient.on('reconnect', () => console.log("Reconnected!"));
   mqttClient.on('close', () => console.log("Disconnected!"));
   mqttClient.on('message', (topic, msg) => console.log("Received acknowledgement for message:", msg.toString()));
	//mqttClient.end();
		//process.exit();

    return mqttClient
	
	
}

ConnectMQTTChannel(SENSOR_ALTERNATE_ID,CAPABILITY_ALTERNATE_ID,DEVICE_ALTERNATE_ID);

module.exports = {
    connectMQTTChannel : ConnectMQTTChannel
}