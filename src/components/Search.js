import React, { useState } from "react";
import axios from "axios";
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  TextField,
  Button,
  IconButton,
} from "@mui/material";
import EditIcon from "@mui/icons-material/Edit";

const Search = () => {
  const [LEI, setLEI] = useState("");
  const [legalName, setLegalName] = useState("");
  const [pincode, setPincode] = useState("");
  const [results, setResults] = useState([]);
  const [editableRowIndex, setEditableRowIndex] = useState(null);
  const [formData, setFormData] = useState({
    RegistrationNo: "",
    CompanyName: "",
    BuildingNumber: "",
    StreetName: "",
    TownName: "",
    CountrySubDivision: "",
    PostCode: "",
    Country: "",
  });

  const [newAddressForm, setNewAddressForm] = useState({
    RegistrationNo: "",
    CompanyName: "",
    BuildingNumber: "",
    StreetName: "",
    TownName: "",
    CountrySubDivision: "",
    PostCode: "",
    Country: "",
  });

  const [showAddForm, setShowAddForm] = useState(false);

  const handleSearch = async () => {
    if (LEI || legalName || pincode) {
      try {
        const response = await axios.post("http://localhost:5000/api/search", {
          LEI,
          legalName,
          pincode,
        });
        setResults(response.data);
      } catch (error) {
        console.error("Error fetching data:", error);
        alert("Error fetching data. Please try again.");
      }
    } else {
      alert("Please fill at least one field to search");
    }
  };

  const handleDownloadCSV = () => {
    if (results.length === 0) {
      alert("No data available to download.");
      return;
    }

    const headers = Object.keys(formData).join(","); // CSV headers
    const rows = results
      .map((result) => Object.values(result).join(","))
      .join("\n"); // CSV rows

    const csvContent = `data:text/csv;charset=utf-8,${headers}\n${rows}`;
    const encodedUri = encodeURI(csvContent);
    const link = document.createElement("a");
    link.setAttribute("href", encodedUri);
    link.setAttribute("download", "report.csv");
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleEdit = (index) => {
    setEditableRowIndex(index);
    setFormData({ ...results[index] });
  };

  const handleSave = async () => {
    try {
      const response = await axios.post(
        "http://localhost:5000/api/update-address",
        formData
      );
      alert(response.data);
      setEditableRowIndex(null);
    } catch (error) {
      console.error("Error updating address:", error);
      alert("Error updating address. Please try again.");
    }
  };

  const handleAddAddress = async () => {
    try {
      const response = await axios.post(
        "http://localhost:5000/api/add-address",
        newAddressForm
      );
      alert(response.data);
      setNewAddressForm({
        RegistrationNo: "",
        CompanyName: "",
        BuildingNumber: "",
        StreetName: "",
        TownName: "",
        CountrySubDivision: "",
        PostCode: "",
        Country: "",
      });
      handleSearch();
    } catch (error) {
      console.error("Error adding address:", error);
      alert("Error adding address. Please try again.");
    }
  };

  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData((prevData) => ({ ...prevData, [name]: value }));
  };

  const handleNewAddressInputChange = (e) => {
    const { name, value } = e.target;
    setNewAddressForm((prevData) => ({ ...prevData, [name]: value }));
  };

  return (
    <div style={{ padding: "20px" }}>
      <div style={{ display: "flex", gap: "10px", marginBottom: "20px" }}>
        <TextField
          label="LEI"
          variant="outlined"
          value={LEI}
          onChange={(e) => setLEI(e.target.value)}
        />
        <TextField
          label="Legal Name"
          variant="outlined"
          value={legalName}
          onChange={(e) => setLegalName(e.target.value)}
        />
        <TextField
          label="Pincode"
          variant="outlined"
          value={pincode}
          onChange={(e) => setPincode(e.target.value)}
        />
        <Button variant="contained" color="primary" onClick={handleSearch}>
          Search
        </Button>
      </div>

      <Button
        variant="contained"
        color="secondary"
        onClick={() => setShowAddForm(!showAddForm)}
      >
        {showAddForm ? "Hide Address Form" : "Add Address"}
      </Button>

      {showAddForm && (
        <div
          style={{
            marginTop: "20px",
            display: "flex",
            flexDirection: "column",
            gap: "10px",
          }}
        >
          {Object.keys(newAddressForm).map((key) => (
            <TextField
              key={key}
              label={key.replace(/([A-Z])/g, " $1")}
              variant="outlined"
              name={key}
              value={newAddressForm[key]}
              onChange={handleNewAddressInputChange}
            />
          ))}
          <Button variant="contained" color="primary" onClick={handleAddAddress}>
            Add Address
          </Button>
        </div>
      )}

      {results.length > 0 && (
        <>
          <Button
            variant="contained"
            color="success"
            onClick={handleDownloadCSV}
            style={{ marginTop: "10px", marginBottom: "10px",marginLeft:"30px" }}
          >
            Download Report
          </Button>
          <TableContainer component={Paper} style={{ marginTop: "10px" }}>
            <Table>
              <TableHead>
                <TableRow>
                  {Object.keys(formData).map((key) => (
                    <TableCell key={key}>
                      <strong>{key.replace(/([A-Z])/g, " $1")}</strong>
                    </TableCell>
                  ))}
                  <TableCell>
                    <strong>Edit</strong>
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {results.map((result, index) => (
                  <TableRow key={index}>
                    {Object.keys(formData).map((key) => (
                      <TableCell key={key}>
                        {editableRowIndex === index ? (
                          <TextField
                            name={key}
                            value={formData[key]}
                            onChange={handleInputChange}
                          />
                        ) : (
                          result[key]
                        )}
                      </TableCell>
                    ))}
                    <TableCell>
                      <IconButton onClick={() => handleEdit(index)}>
                        <EditIcon />
                      </IconButton>
                      {editableRowIndex === index && (
                        <Button onClick={handleSave}>Save</Button>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </>
      )}
    </div>
  );
};

export default Search;
