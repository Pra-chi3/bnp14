import React from "react";
import logo from "../assets/flag.png"; 
import search from "../assets/search.png"; 

const Header = ({ toggleSearch }) => {
  return (
    <nav
      style={{
        padding: "10px 0px",
        background: "#008859",
        color: "#fff",
        width: "100vw",
        height: "10vh",
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "10px", // Adjust spacing between image and text
          marginLeft: "30px",
        }}
      >
        {/* Square Logo */}
        <img
          src={logo}
          alt="Logo"
          style={{
            width: "40px", // Set the width of the square
            height: "40px", // Match height to width for square shape
            objectFit: "cover", // Ensures the image fits within the square
          }}
        />
        <h1 style={{ margin: 0 }}>BNP Paribas</h1>
      </div>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "10px", // Adjust spacing between "GOA" text and the search icon
          marginRight: "30px",
        }}
      >
        {/* GOA Text */}
        <span
          style={{
            fontSize: "24px",
            color: "#fff",
            fontWeight: "bold",
          }}
        >
          GOA
        </span>

        {/* Search Icon */}
        <button
          style={{
            background: "none",
            border: "none",
            cursor: "pointer",
            padding: "5px",
          }}
          onClick={toggleSearch} // Trigger toggleSearch to show/hide search section
        >
          <img
            src={search} // Ensure the path is correct
            alt="Search Icon"
            style={{
              width: "30px", // Adjust the size as needed
              height: "30px",
            }}
          />
        </button>
      </div>
    </nav>
  );
};

export default Header;
