const SftpClient = require("ssh2-sftp-client");
const { Sequelize, DataTypes } = require("sequelize");
const fs = require('fs');

async function processData() {
  try {
    const sftpUsername = "azureuser";
    const host = "20.203.195.102";
    const port = 22; // Replace with the actual port number
    const privateKeyPath = "C:/Users/divan/Downloads/silent-cloud-AUR__key.pem";
    const dbName = "postgres";
    const dbPassword = "solariym@123";
    const dbHost = "solariym.postgres.database.azure.com";
    const dbPort = 5432;
    const dbUsername = 'postgresAzure@solariym';

    const sftpConfig = {
      host: host,
      port: parseInt(port),
      username: sftpUsername,
      privateKey: fs.readFileSync(privateKeyPath, "utf-8"),
    };

    const remoteFolderPath = "/home/azureuser/silent_cloud"; // The folder containing the files
    const filePattern = "US"; // The pattern to match the file names

    // Connect to SFTP
    const sftp = new SftpClient();
    await sftp.connect(sftpConfig);

    console.log("Start of the process.");
    console.log("Connected to SFTP.");

    const fileList = await sftp.list(remoteFolderPath);
    const filteredFiles = fileList.filter((file) => file.name.includes(filePattern));

    if (filteredFiles.length > 0) {
      filteredFiles.sort((a, b) => b.modifyTime - a.modifyTime);
      const latestFile = filteredFiles[0];
      const remoteFilePath = `${remoteFolderPath}/${latestFile.name}`;

      const fileContentsBuffer = await sftp.get(remoteFilePath);
      const fileContents = fileContentsBuffer.toString("utf-8");
      
      let userHeaders =
        'Patient ID;country;Date of Data Sheet Creation;Data Sheet Version;Campaign ID;Date of registration;subscribtion status;subscription daysToExpiration;subscription last check date;subscription productId;isautorenewing;expirationintentios;Last time online (Date-Time);Shop/ Clinic - Date-Time;Shop ID;ENT Office - Date-Time;ENT Office ID;ENT Form -Date-Time;ha;cbt;Sound Therapy;TSCHQ 1 - Date-Time;TSCHQ 1 - Q1;TSCHQ 1 - Q2;TSCHQ 1 - Q3;TSCHQ 1 - Q4;TSCHQ 1 - Q5;TSCHQ 1 - Q6;TSCHQ 1 - Q7;TSCHQ 1 - Q8;TSCHQ 1 - Q9;TSCHQ 1 - Q10;TSCHQ 1 - Q11;TSCHQ 1 - Q12;TSCHQ 1 - Q13;Medication - Date-Time;Medication name;Medication - Answer Index;Hearing Assessment (latest) -Date-Time;Hearing Assessment - Becky;Hearing Assessment (latest) - Tone Test;Hearing Assessment (latest) - Contradictory assessment;Hearing Assessment (latest) -Overall;Sound Finder - Date-Time;Sound Finder - outcome;THI - Base - Date-Time;THI - Base - score;THI - Current - Date-Time;THI - Current - score;TFI - Base - Date-Time;TFI - Base - score;TFI - Current - Date-Time;TFI - Current - score;HQ - Base -Date-Time;HQ - Base - score;HQ- Current -Date-Time;HQ- Current - score;PHQ9 - Base - Date-Time;PHQ9 - Base - score;PHQ9 - Current - Date-Time;PHQ9 - Current score;GAD7 - Base - Date-Time;GAD7 - Base - score;GAD7- Current - Date-Time;GAD7- Current - score;VAS-base - Date-Time;VAS-base - Loudness;VAS-base - Annoyance;VAS-current - Date-Time;VAS-current - Loudness;VAS-current - Annoyance;Basic Sound Assesssment - Date-Time;Masking - Date-Time;Masking - band;Pitch Matching - Date-Time;Pitch Matching - frequency;Start date of AAT Day 1;AAT - EC 02;AAT - EC 03;AAT - EC 04;AAT - EC 05;AAT - EC 06;AAT - EC 07;AAT - EC 08;AAT - EC 09;AAT - EC 10;AAT - EC 11;AAT - EC 12;AAT - EC 13;AAT - EC 14;AAT - EC 15;AAT - EC 16;AAT - EC 17;CBT 01;CBT 02;CBT 03;CBT 04;CBT 05;CBT 06;CBT 07;CBT 08;CBT 09;CBT 10;CBT 11;CBT 12;CBT 13;CBT 14;CBT 15;CBT 16;CBT 17;CBT 18;CBT 19;CBT 20;CBT 21;CBT 22;CBT 23;CBT 24;CBT 25;CBT 26;CBT 27;CBT 28;CBT 29;CBT 30;CBT 31;CBT 32;CBT 33;CBT 34;CBT 35;CBT 36;CBT 37;CBT 38;CBT 39;CBT 40;"Sound Therapy Assignment Date Basic Sound";"Sound Therapy Assignment Date Tailored Sound";"Sound Therapy Assignment Date Tailored Tonal";"Sound Therapy usage - Last used date Basic Sound";"Sound Therapy usage - Last used date Tailored Sound";"Sound Therapy usage-last used date Tailored Tonal";Survey 1 - Date;Customer Survey;Survey 1-Q1;Survey 1-Q2;Survey 1 -Q3;Survey 2 - Date;Survey 2-Q1';

      userHeaders +=
        ";Survey 2-Q2;Survey 2-Q3;platform;AppsFlyer - Media Source;AppsFlyer - Campaign Name;AppsFlyer - Campaign ID;AppsFlyer - Adset Name;AppsFlyer - Adset ID";
        const dataArrays = [];
      const rows = fileContents.split("\n");
      const headers = userHeaders.split(";");

      for (let i = 0; i < rows.length; i++) {
        const rowData = rows[i].split(";");
        const dataObject = {};
        for (let j = 0; j < headers.length; j++) {
          const header = headers[j];
          let value = rowData[j];
          if (value=='99999'){
            value=null;
          }else if(value && value.includes(",")||value && value.includes(".")){
            // value = parseInt(value.replace(/,/g, ""), 10);
            value=String(value);
          }
          dataObject[header] = value;
        }
        dataArrays.push(dataObject);
      }

      const sequelize = new Sequelize(dbName, dbUsername, dbPassword, {
        host: dbHost,
        port: dbPort,
        dialect: "postgres",
        dialectOptions: {
          ssl: {
            require: true, // Enable SSL connection
            rejectUnauthorized: false, // Disable SSL certificate verification (use only in development/testing)
          },
          connectTimeout: 500000,
        },
      });

      await sequelize.authenticate();
      console.log("Connection to the database has been established successfully!");

      const AUR_US = sequelize.define("AUR_US", generateAttributes(headers), {
        schema: 'ha_layer',
        tableName: "AUR_US",
        timestamps: false,
      });

      function generateAttributes(headers) {
        const attributes = {};
        headers.forEach((header) => {
          attributes[header] = {
            type: DataTypes.STRING,
            allowNull: true,
          };
        });
        return attributes;
      }

      await sequelize.sync();
      console.log("Model synced with the database");

      await AUR_US.bulkCreate(dataArrays);
      console.log("Data inserted successfully!");

      sequelize.close(); // Close the database connection
      console.log("Latest file retrieved:", latestFile.name);
    } else {
      console.log("No files found matching the pattern:", filePattern);
    }

    sftp.end();
    console.log("SFTP connection closed.");

    console.log("End of the process.");
  } catch (error) {
    console.error("Error:", error);
    throw error;
  }
}

processData().catch((error) => {
  console.error("Unhandled error:", error);
  process.exit(1);
});