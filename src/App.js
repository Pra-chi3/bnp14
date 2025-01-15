import React, { useState } from "react";
import Header from "./components/Header";
import Search from "./components/Search";
import backgroundImage from './assets/bg.jpeg'; // Import the background image

function App() {
  const [showSearch, setShowSearch] = useState(false); // Manage search visibility

  const toggleSearch = () => {
    setShowSearch((prev) => !prev); // Toggle visibility of search
  };

  return (
    <div className="App">
      <Header toggleSearch={toggleSearch} />
      {showSearch && <Search />} {/* Conditionally render Search */}
      <div 
        style={{
          width: '100%', // Make the background full width
          height: 'calc(87vh)', // Set height to full screen height minus header height
          backgroundImage: `url(${backgroundImage})`, // Set background image
          backgroundSize: 'cover', // Ensure the background image covers the area
          backgroundPosition: 'center', // Center the background image
          backgroundRepeat: 'no-repeat' // Prevent the image from repeating
        }}
      ></div>
    </div>
  );
}

export default App;
