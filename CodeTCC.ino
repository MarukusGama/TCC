#include <FlexCAN_T4.h>
#include <SPI.h>
#include <SD.h>
#include <Watchdog_t4.h>

// ================= CONFIG =================
#define PIN_VOLT A0
#define PIN_WAKE 2
#define SD_CS 10

#define APN "zap.vivo.com.br"
#define APN_USER "vivo"
#define APN_PASS "vivo"

// ================= MQTT =================
#define MQTT_BROKER "15a3886e95d1462682ca6afa2fe843a2.s1.eu.hivemq.cloud"
#define MQTT_PORT 8883
#define MQTT_TOPIC "veiculos/carro01/telemetria"

#define MQTT_CLIENT_ID "carro1"
#define MQTT_USER "Vinicius"
#define MQTT_PASS "Mvog.1223"

// ================= CAN =================
FlexCAN_T4<CAN1, RX_SIZE_256, TX_SIZE_16> Can0;
CAN_message_t msg;

// ================= WATCHDOG =================
WDT_T4<WDT1> wdt;

// ================= LTE =================
#define LTE Serial2

// ================= TEMPO =================
unsigned long lastSaveTime = 0;
unsigned long lastSendTime = 0;
unsigned long lastOBDRequest = 0;
unsigned long lastGPSRequest = 0;
unsigned long lastMQTTCheck = 0;

const unsigned long saveInterval = 60000;
const unsigned long sendInterval = 15000;
const unsigned long obdInterval = 200;
const unsigned long gpsInterval = 5000;
const unsigned long mqttCheckInterval = 10000;

// ================= POWER =================
bool powerFail = false;
unsigned long powerFailTime = 0;
const unsigned long shutdownDelay = 5000;

// ================= GPS =================
String latitude = "0";
String longitude = "0";
String gpsDateTime = "";
bool gpsFix = false;

// ================= VEÍCULO =================
struct VehicleData {
  int rpm = 0;
  int speed = 0;
  int coolantTemp = 0;
  int engineLoad = 0;
  int fuelLevel = 0;
};

VehicleData car;

// ================= OBD =================
uint8_t obdPids[] = {0x0C, 0x0D, 0x05, 0x04, 0x2F};
uint8_t pidIndex = 0;
const uint8_t totalPids = sizeof(obdPids) / sizeof(obdPids[0]);

// ================= STATUS =================
bool mqttConnected = false;

// ================= FUNÇÕES =================

// -------- POWER --------
void checkPower() {
  float voltage = analogRead(PIN_VOLT) * (3.3 / 1023.0) * 11;
  if (voltage < 11.5) {
    if (!powerFail) {
      powerFail = true;
      powerFailTime = millis();
    }
  } else {
    powerFail = false;
  }
}

// -------- SLEEP --------
void enterSleep() {
  while (digitalRead(PIN_WAKE) == LOW) delay(1000);
}

// -------- OBD --------
void requestPID(uint8_t pid) {
  CAN_message_t tx;
  tx.id = 0x7DF;
  tx.len = 8;
  tx.buf[0] = 0x02;
  tx.buf[1] = 0x01;
  tx.buf[2] = pid;
  for (int i = 3; i < 8; i++) tx.buf[i] = 0;
  Can0.write(tx);
}

void decodeOBD(CAN_message_t &msg) {
  if (msg.id != 0x7E8) return;

  switch (msg.buf[2]) {
    case 0x0C: car.rpm = ((msg.buf[3] << 8) | msg.buf[4]) / 4; break;
    case 0x0D: car.speed = msg.buf[3]; break;
    case 0x05: car.coolantTemp = msg.buf[3] - 40; break;
    case 0x04: car.engineLoad = (msg.buf[3] * 100) / 255; break;
    case 0x2F: car.fuelLevel = (msg.buf[3] * 100) / 255; break;
  }
}

// -------- GPS --------
void requestGPS() {
  LTE.println("AT+CGNSINF");
}

void readGPS() {
  static String buffer = "";

  while (LTE.available()) {
    char c = LTE.read();

    if (c == '\n') {
      buffer.trim();

      if (buffer.startsWith("+CGNSINF:")) {
        String data = buffer.substring(String("+CGNSINF:").length());
        data.trim();

        String parts[20];
        int f = 0;
        int last = 0;

        for (int i = 0; i < data.length() && f < 20; i++) {
          if (data[i] == ',') {
            parts[f++] = data.substring(last, i);
            last = i + 1;
          }
        }

        if (f < 20) {
          parts[f++] = data.substring(last);
        }

        gpsFix = (parts[1] == "1");

        if (gpsFix) {
          gpsDateTime = parts[2];
          latitude = parts[3];
          longitude = parts[4];
        }
      }

      buffer = "";
    } else {
      buffer += c;
    }
  }
}

// -------- TIMESTAMP --------
String getTimestamp() {
  return (gpsFix && gpsDateTime.length() >= 14) ? gpsDateTime : String(millis());
}

// -------- DATA --------
String buildDataString() {
  String s = "";
  s += getTimestamp();
  s += ",";
  s += String(car.rpm);
  s += ",";
  s += String(car.speed);
  s += ",";
  s += String(car.coolantTemp);
  s += ",";
  s += String(car.engineLoad);
  s += ",";
  s += String(car.fuelLevel);
  s += ",";
  s += latitude;
  s += ",";
  s += longitude;
  return s;
}

// -------- SD --------
void saveToSD() {
  if (powerFail) return;

  File f = SD.open("queue.txt", FILE_WRITE);
  if (f) {
    f.println(buildDataString());
    f.close();
  }
}

// -------- MQTT --------
void mqttConnect() {

  LTE.println("AT+MQTTDISC"); delay(200);
  LTE.println("AT+MQTTREL"); delay(200);

  LTE.println("AT+MQTTCONN=\"client1\",\"" MQTT_BROKER "\"," + String(MQTT_PORT));
  delay(2000);

  mqttConnected = true; // simplificado
}

bool sendBatch(String payload) {

  if (!mqttConnected) return false;

  LTE.println("AT+MQTTPUB=\"" MQTT_TOPIC "\",0,0,0");
  delay(200);

  LTE.print(payload);
  delay(500);

  return true;
}

// -------- PROCESS QUEUE --------
void processQueueBatch() {

  File file = SD.open("queue.txt", FILE_READ);
  if (!file) return;

  File temp = SD.open("temp.txt", FILE_WRITE);
  if (!temp) {
    file.close();
    return;
  }

  String payload = "[";
  int count = 0;

  while (file.available() && count < 10) {
    String line = file.readStringUntil('\n');
    line.trim();
    if (!line.length()) continue;

    if (count++) payload += ",";
    payload += "\"" + line + "\"";
  }

  payload += "]";

  if (count > 0 && sendBatch(payload)) {

    while (file.available()) {
      String line = file.readStringUntil('\n');
      if (line.length()) temp.println(line);
    }

  } else {

    file.seek(0);
    while (file.available()) {
      String line = file.readStringUntil('\n');
      if (line.length()) temp.println(line);
    }
  }

  file.close();
  temp.close();

  SD.remove("queue.txt");
  SD.rename("temp.txt", "queue.txt");
}

// -------- NETWORK WATCHDOG --------
void checkMQTT() {

  if (millis() - lastMQTTCheck < mqttCheckInterval) return;

  if (!mqttConnected) {
    mqttConnect();
  }

  lastMQTTCheck = millis();
}

// ================= SETUP =================
void setup() {

  pinMode(PIN_WAKE, INPUT);

  Serial.begin(115200);
  LTE.begin(115200);


  Can0.begin();
  Can0.setBaudRate(500000);

  if (!SD.begin(SD_CS)) {
    Serial.println("Falha ao inicializar microSD");
  } else {
    Serial.println("microSD inicializado");
  }

  wdt.begin(10);

  LTE.println("AT+CGNSPWR=1");
  delay(1000);

  mqttConnect();
}

// ================= LOOP =================
void loop() {

  wdt.feed();

  checkPower();

  if (powerFail && millis() - powerFailTime > shutdownDelay) {
    processQueueBatch();
    enterSleep();
  }

  if (Can0.read(msg)) decodeOBD(msg);

  if (millis() - lastOBDRequest > obdInterval) {
    requestPID(obdPids[pidIndex]);
    pidIndex = (pidIndex + 1) % totalPids;
    lastOBDRequest = millis();
  }

  if (millis() - lastGPSRequest > gpsInterval) {
    requestGPS();
    lastGPSRequest = millis();
  }

  readGPS();

  if (millis() - lastSaveTime > saveInterval) {
    saveToSD();
    lastSaveTime = millis();
  }

  if (millis() - lastSendTime > sendInterval) {
    processQueueBatch();
    lastSendTime = millis();
  }

  checkMQTT();
}