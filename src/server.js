const express = require("express");
const mysql = require("mysql2");
const cors = require("cors");

const app = express();
app.use(cors());
app.use(express.json());

// MySQL Database Connection
const db = mysql.createConnection({
  host: "localhost", // Change to your DB host
  user: "root",      // Your MySQL username
  password: "rohit", // Your MySQL password
  database: "sys",   // Change to your database name
});

db.connect((err) => {
  if (err) {
    console.error("Error connecting to the database:", err);
  } else {
    console.log("Connected to the database");
  }
});

// Search API Endpoint
app.post("/api/search", (req, res) => {
  const { LEI, legalName, pincode } = req.body;

  let query = "SELECT * FROM CompanyData WHERE 1=1";
  const params = [];

  if (LEI) {
    query += " AND RegistrationNo = ?";
    params.push(LEI);
  }
  if (legalName) {
    query += " AND CompanyName LIKE ?";
    params.push(`%${legalName}%`);
  }
  if (pincode) {
    query += " AND PostCode = ?";
    params.push(pincode);
  }

  db.query(query, params, (err, results) => {
    if (err) {
      console.error("Error executing query:", err);
      res.status(500).send("Server error");
    } else {
      res.json(results);
    }
  });
});

// Update Address API Endpoint
app.post("/api/update-address", (req, res) => {
    const {
      RegistrationNo,
      CompanyName,
      BuildingNumber,
      StreetName,
      TownName,
      CountrySubDivision,
      PostCode,
      Country,
    } = req.body;
  
    const query = `
      UPDATE CompanyData 
      SET CompanyName = ?, BuildingNumber = ?, StreetName = ?, TownName = ?, 
          CountrySubDivision = ?, PostCode = ?, Country = ? 
      WHERE RegistrationNo = ?`;
  
    const params = [
      CompanyName,
      BuildingNumber,
      StreetName,
      TownName,
      CountrySubDivision,
      PostCode,
      Country,
      RegistrationNo
    ];
  
    db.query(query, params, (err, results) => {
      if (err) {
        console.error("Error executing query:", err);
        res.status(500).send("Error updating address in the database");
      } else {
        res.status(200).send("Address updated successfully");
      }
    });
  });
  

// Add Address API Endpoint
app.post("/api/add-address", (req, res) => {
    const {
      RegistrationNo,
      CompanyName,
      BuildingNumber,
      StreetName,
      TownName,
      CountrySubDivision,
      PostCode,
      Country,
    } = req.body;
  
    const query = `
      INSERT INTO CompanyData 
      (RegistrationNo, CompanyName, BuildingNumber, StreetName, TownName, CountrySubDivision, PostCode, Country) 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
  
    const params = [
      RegistrationNo,
      CompanyName,
      BuildingNumber,
      StreetName,
      TownName,
      CountrySubDivision,
      PostCode,
      Country,
    ];
  
    db.query(query, params, (err, results) => {
      if (err) {
        console.error("Error executing query:", err);
        res.status(500).send("Error adding address to the database");
      } else {
        res.status(200).send("Address added successfully");
      }
    });
  });
  


// Start the server
const PORT = 5000;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
